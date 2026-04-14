"""
Base classes for health check framework.
Every check module uses these dataclasses and the BaseCheckRunner.
"""
from __future__ import annotations
import logging
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger("health_check")


class Status(str, Enum):
    PASS = "pass"
    PARTIAL = "partial"
    FAIL = "fail"
    NOT_EVALUATED = "not_evaluated"
    INFO = "info"


class Priority(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class Recommendation:
    action: str
    impact: str
    priority: str = "medium"
    estimated_savings: Optional[str] = None
    docs_url: Optional[str] = None
    sql_command: Optional[str] = None


@dataclass
class CheckResult:
    check_id: str
    name: str
    subsection: str
    score: int  # 0, 50, or 100
    status: str  # pass / partial / fail / not_evaluated / info
    current_value: str
    target_value: str
    details: dict = field(default_factory=dict)
    recommendation: Optional[Recommendation] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


@dataclass
class SectionResult:
    section_id: str
    section_name: str
    section_type: str  # core / conditional / advisory
    active: bool
    score: Optional[float]
    subsections: list[str]
    checks: list[CheckResult]
    icon: str = ""

    @property
    def scored_checks(self) -> list[CheckResult]:
        return [c for c in self.checks if c.status not in ("not_evaluated", "info")]

    @property
    def failed_checks(self) -> list[CheckResult]:
        return [c for c in self.checks if c.status in ("fail", "partial")]

    def to_dict(self) -> dict:
        return {
            "section_id": self.section_id,
            "section_name": self.section_name,
            "section_type": self.section_type,
            "active": self.active,
            "score": self.score,
            "icon": self.icon,
            "subsections": self.subsections,
            "checks": [c.to_dict() for c in self.checks],
            "issues_count": len(self.failed_checks),
            "total_checks": len(self.checks),
            "scored_checks": len(self.scored_checks),
        }


class QueryExecutor:
    """Executes SQL queries via the Statement Execution REST API.
    
    Uses /api/2.0/sql/statements which works reliably with OAuth tokens
    (on-behalf-of user auth) unlike the Thrift-based SQL connector.
    """

    def __init__(self, host: str, token: str, warehouse_id: str):
        self.host = host.rstrip("/")
        if not self.host.startswith("https://"):
            self.host = f"https://{self.host}"
        self.token = token
        self.warehouse_id = warehouse_id
        self._cache: dict[str, list[dict]] = {}
        self._lock = threading.Lock()
        # Track query stats for diagnostics
        self.stats = {"success": 0, "fail": 0, "cache_hit": 0, "errors": [], "token_present": bool(token), "token_prefix": (token or "")[:20]}

    def execute(self, query: str, use_cache: bool = True, timeout: int = 120) -> list[dict]:
        cache_key = query.strip()
        with self._lock:
            if use_cache and cache_key in self._cache:
                logger.debug("Cache hit for query")
                self.stats["cache_hit"] += 1
                return self._cache[cache_key]

        import requests

        try:
            resp = requests.post(
                f"{self.host}/api/2.0/sql/statements",
                headers={"Authorization": f"Bearer {self.token}"},
                json={
                    "warehouse_id": self.warehouse_id,
                    "statement": query.strip(),
                    "wait_timeout": f"{min(timeout, 50)}s",
                    "disposition": "INLINE",
                    "format": "JSON_ARRAY",
                },
                timeout=timeout + 30,
            )

            if resp.status_code != 200:
                error_msg = resp.text[:500]
                try:
                    err_data = resp.json()
                    error_msg = err_data.get("message", error_msg)
                except Exception:
                    pass
                raise RuntimeError(f"SQL API error ({resp.status_code}): {error_msg}")

            data = resp.json()
            status = data.get("status", {})
            state = status.get("state", "UNKNOWN")

            if state == "FAILED":
                err = status.get("error", {}).get("message", "Unknown SQL error")
                raise RuntimeError(f"SQL query failed: {err}")

            if state == "PENDING" or state == "RUNNING":
                # Need to poll for result
                stmt_id = data.get("statement_id")
                result_data = self._poll_statement(stmt_id, timeout)
            elif state == "SUCCEEDED":
                result_data = data
            else:
                raise RuntimeError(f"Unexpected statement state: {state}")

            # Parse result with type conversion
            manifest = result_data.get("manifest", {})
            schema_cols = manifest.get("schema", {}).get("columns", [])
            columns = [col["name"] for col in schema_cols]
            col_types = [col.get("type_name", "STRING") for col in schema_cols]
            data_array = result_data.get("result", {}).get("data_array", [])
            
            def _convert(val, type_name):
                if val is None:
                    return None
                tn = type_name.upper()
                try:
                    if tn in ("INT", "INTEGER", "SMALLINT", "TINYINT", "BIGINT", "LONG"):
                        return int(val)
                    elif tn in ("FLOAT", "DOUBLE", "DECIMAL", "DEC", "NUMERIC"):
                        return float(val)
                    elif tn == "BOOLEAN":
                        return str(val).lower() in ("true", "1")
                    elif "DECIMAL" in tn:
                        return float(val)
                except (ValueError, TypeError):
                    pass
                return val
            
            result = []
            for row in data_array:
                converted = {columns[i]: _convert(row[i], col_types[i]) if i < len(col_types) else row[i]
                            for i in range(len(columns))}
                result.append(converted)

        except Exception as e:
            with self._lock:
                self.stats["fail"] += 1
                err_summary = str(e)[:200]
                if len(self.stats["errors"]) < 5:
                    self.stats["errors"].append(err_summary)
            logger.error(f"Query failed: {e}")
            raise

        with self._lock:
            self.stats["success"] += 1
            if use_cache:
                self._cache[cache_key] = result
        return result

    def _poll_statement(self, statement_id: str, timeout: int) -> dict:
        """Poll for async statement completion."""
        import requests
        import time as _time

        deadline = _time.time() + timeout
        while _time.time() < deadline:
            _time.sleep(2)
            resp = requests.get(
                f"{self.host}/api/2.0/sql/statements/{statement_id}",
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=30,
            )
            if resp.status_code != 200:
                raise RuntimeError(f"Poll error ({resp.status_code}): {resp.text[:300]}")
            data = resp.json()
            state = data.get("status", {}).get("state", "UNKNOWN")
            if state == "SUCCEEDED":
                return data
            elif state == "FAILED":
                err = data.get("status", {}).get("error", {}).get("message", "Unknown")
                raise RuntimeError(f"SQL query failed: {err}")
            elif state in ("CANCELED", "CLOSED"):
                raise RuntimeError(f"Statement {state}")
        raise RuntimeError(f"Statement timed out after {timeout}s")

    def clear_cache(self):
        self._cache.clear()


class APIClient:
    """Wraps the Databricks SDK for REST API calls."""

    def __init__(self):
        from databricks.sdk import WorkspaceClient
        self.w = WorkspaceClient()

    def list_warehouses(self):
        return list(self.w.warehouses.list())

    def list_clusters(self):
        return list(self.w.clusters.list())

    def list_jobs(self, limit=100):
        return list(self.w.jobs.list(limit=limit))

    def list_cluster_policies(self):
        return list(self.w.cluster_policies.list())

    def get_warehouse(self, warehouse_id):
        return self.w.warehouses.get(warehouse_id)


class BaseCheckRunner:
    """Base class for section check runners."""

    section_id: str = ""
    section_name: str = ""
    section_type: str = "core"  # core / conditional / advisory
    icon: str = ""

    def __init__(self, executor: QueryExecutor, api_client: APIClient, include_table_analysis: bool = False):
        self.executor = executor
        self.api = api_client
        self.include_table_analysis = include_table_analysis

    def is_active(self) -> bool:
        """Check if this section has meaningful usage (>$100/mo or >10 resources)."""
        return True  # Override in conditional sections

    def get_subsections(self) -> list[str]:
        return []

    def run_checks(self) -> list[CheckResult]:
        """Run all checks for this section in parallel."""
        check_methods = sorted([m for m in dir(self) if m.startswith("check_")])

        def _run_one(method_name):
            try:
                result = getattr(self, method_name)()
                if isinstance(result, list):
                    return result
                elif result is not None:
                    return [result]
                return []
            except Exception as e:
                check_id = method_name.replace("check_", "").replace("_", ".")
                logger.error(f"Check {check_id} failed: {e}")
                return [CheckResult(
                    check_id=check_id,
                    name=method_name,
                    subsection="Unknown",
                    score=0,
                    status="not_evaluated",
                    current_value=f"Error: {str(e)[:200]}",
                    target_value="N/A",
                    recommendation=Recommendation(
                        action=f"Check failed with error: {str(e)[:200]}. Verify system table access and permissions.",
                        impact="Unable to evaluate this check",
                        priority="medium",
                    )
                )]

        results = []
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(_run_one, m): m for m in check_methods}
            for future in as_completed(futures):
                results.extend(future.result())
        return results

    def run(self) -> SectionResult:
        active = self.is_active()
        if not active:
            return SectionResult(
                section_id=self.section_id,
                section_name=self.section_name,
                section_type=self.section_type,
                active=False,
                score=None,
                subsections=self.get_subsections(),
                checks=[],
                icon=self.icon,
            )

        checks = self.run_checks()
        scored = [c for c in checks if c.status not in ("not_evaluated", "info")]
        score = round(sum(c.score for c in scored) / len(scored), 1) if scored else None

        return SectionResult(
            section_id=self.section_id,
            section_name=self.section_name,
            section_type=self.section_type,
            active=True,
            score=score,
            subsections=self.get_subsections(),
            checks=checks,
            icon=self.icon,
        )
