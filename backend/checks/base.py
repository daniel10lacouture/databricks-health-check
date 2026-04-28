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


# ── Persistent TTL Cache (survives across health check runs) ──────────
import hashlib

_GLOBAL_CACHE: dict[str, tuple[float, list[dict]]] = {}
_GLOBAL_CACHE_LOCK = threading.Lock()
_CACHE_TTL = 3600  # 1 hour

def _cache_key(query: str) -> str:
    return hashlib.md5(query.strip().encode()).hexdigest()

def _get_cached(query: str) -> list[dict] | None:
    key = _cache_key(query)
    with _GLOBAL_CACHE_LOCK:
        if key in _GLOBAL_CACHE:
            ts, result = _GLOBAL_CACHE[key]
            if time.time() - ts < _CACHE_TTL:
                return result
            del _GLOBAL_CACHE[key]
    return None

def _set_cached(query: str, result: list[dict]):
    key = _cache_key(query)
    with _GLOBAL_CACHE_LOCK:
        _GLOBAL_CACHE[key] = (time.time(), result)

def clear_global_cache():
    with _GLOBAL_CACHE_LOCK:
        _GLOBAL_CACHE.clear()


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
        if use_cache:
            cached = _get_cached(query)
            if cached is not None:
                with self._lock:
                    self.stats["cache_hit"] += 1
                return cached

        import requests

        try:
            resp = requests.post(
                f"{self.host}/api/2.0/sql/statements",
                headers={"Authorization": f"Bearer {self.token}"},
                json={
                    "warehouse_id": self.warehouse_id,
                    "statement": query.strip(),
                    "wait_timeout": "30s",
                    "disposition": "EXTERNAL_LINKS",
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

            # Handle both INLINE and EXTERNAL_LINKS responses
            result_obj = result_data.get("result", {})
            data_array = result_obj.get("data_array")
            if data_array is None:
                # EXTERNAL_LINKS: fetch data from external URL
                data_array = []
                ext_links = result_obj.get("external_links", [])
                for link_info in ext_links:
                    ext_url = link_info.get("external_link")
                    if ext_url:
                        chunk_resp = requests.get(ext_url, timeout=60)
                        if chunk_resp.status_code == 200:
                            chunk_data = chunk_resp.json()
                            if isinstance(chunk_data, list):
                                data_array.extend(chunk_data)
                            elif isinstance(chunk_data, dict):
                                data_array.extend(chunk_data.get("data_array", []))
                    # Handle pagination via next_chunk_internal_link
                    next_link = link_info.get("next_chunk_internal_link")
                    while next_link:
                        nr = requests.get(
                            f"{self.host}{next_link}",
                            headers={"Authorization": f"Bearer {self.token}"},
                            timeout=60)
                        if nr.status_code != 200:
                            break
                        nd = nr.json()
                        chunk = nd.get("data_array") or nd.get("result", {}).get("data_array", [])
                        data_array.extend(chunk)
                        ext = nd.get("external_links", [{}])
                        next_link = ext[0].get("next_chunk_internal_link") if ext else None
            
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
            _set_cached(query, result)
        return result

    def _poll_statement(self, statement_id: str, timeout: int) -> dict:
        """Poll for async statement completion."""
        import requests
        import time as _time

        deadline = _time.time() + timeout
        while _time.time() < deadline:
            _time.sleep(1)
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




class DataPrefetcher:
    """Runs parallel prefetch queries for heavy system tables at startup.
    
    Instead of 192 individual queries, prefetch 6 broad datasets in parallel,
    then let check runners compute metrics from the prefetched data.
    """
    
    PREFETCH_QUERIES = {
        # query_history: pre-aggregated metrics (too large for raw rows)
        "qh_metrics": """
            SELECT
                COUNT(*) AS total_queries,
                COUNT(DISTINCT executed_by) AS unique_users,
                SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_queries,
                SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS error_rate_pct,
                COUNT(DISTINCT CASE WHEN client_application = 'Databricks SQL Dashboard' THEN executed_by END) AS dashboard_users,
                SUM(CASE WHEN client_application = 'Databricks SQL Dashboard' THEN 1 ELSE 0 END) AS dashboard_queries,
                COUNT(DISTINCT CASE WHEN client_application = 'Databricks SQL Genie Space' THEN executed_by END) AS genie_users,
                SUM(CASE WHEN client_application = 'Databricks SQL Genie Space' THEN 1 ELSE 0 END) AS genie_queries,
                SUM(CASE WHEN LOWER(statement_text) LIKE '%%ai_query(%%' OR LOWER(statement_text) LIKE '%%ai_generate%%' THEN 1 ELSE 0 END) AS ai_func_queries,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_duration_ms) AS p50_duration_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_duration_ms) AS p95_duration_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY waiting_at_capacity_duration_ms) AS p95_queue_ms
            FROM system.query.history
            WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
        """,
        
        # query_history by client_application
        "qh_by_client": """
            SELECT client_application,
                   COUNT(*) AS query_count,
                   COUNT(DISTINCT executed_by) AS user_count
            FROM system.query.history
            WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
            GROUP BY 1
        """,
        
        # query_history: error patterns
        "qh_errors": """
            SELECT SUBSTRING(error_message, 1, 120) AS error_pattern,
                   COUNT(*) AS occurrences,
                   COUNT(DISTINCT executed_by) AS affected_users
            FROM system.query.history
            WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
              AND execution_status = 'FAILED'
            GROUP BY 1 ORDER BY 2 DESC LIMIT 20
        """,
        
        # query_history: per-warehouse stats
        "qh_by_warehouse": """
            SELECT compute.warehouse_id AS warehouse_id,
                   COUNT(*) AS total_queries,
                   SUM(CASE WHEN spilled_local_bytes > 0 THEN 1 ELSE 0 END) AS spill_queries,
                   SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_queries
            FROM system.query.history
            WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
              AND compute.warehouse_id IS NOT NULL
            GROUP BY 1
        """,
        
        # query_history: top expensive queries
        "qh_expensive": """
            SELECT LEFT(statement_text, 200) AS query_preview,
                   COUNT(*) AS exec_count,
                   ROUND(AVG(total_duration_ms) / 1000, 1) AS avg_sec,
                   ROUND(SUM(total_task_duration_ms) / 1000.0 / 3600.0, 2) AS total_task_hours
            FROM system.query.history
            WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
              AND total_duration_ms > 5000
            GROUP BY 1 ORDER BY total_task_hours DESC LIMIT 20
        """,
        
        # billing.usage: comprehensive cost breakdown
        "billing_summary": """
            SELECT
                sku_name,
                billing_origin_product AS product,
                usage_metadata.warehouse_id AS warehouse_id,
                usage_metadata.cluster_id AS cluster_id,
                usage_metadata.job_id AS job_id,
                CASE WHEN sku_name LIKE '%%SERVERLESS%%' THEN 'serverless' ELSE 'classic' END AS compute_mode,
                CASE WHEN sku_name LIKE '%%PHOTON%%' THEN 'photon' ELSE 'standard' END AS engine,
                CASE WHEN sku_name LIKE '%%ALL_PURPOSE%%' OR sku_name LIKE '%%ALL PURPOSE%%' THEN 'all_purpose'
                     WHEN sku_name LIKE '%%JOBS%%' THEN 'jobs'
                     WHEN sku_name LIKE '%%SQL%%' THEN 'sql'
                     WHEN sku_name LIKE '%%SERVING%%' THEN 'serving'
                     WHEN sku_name LIKE '%%DLT%%' OR sku_name LIKE '%%PIPELINES%%' THEN 'pipelines'
                     ELSE 'other' END AS workload_type,
                SUM(usage_quantity) AS dbus,
                COUNT(*) AS records
            FROM system.billing.usage
            WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
            GROUP BY 1,2,3,4,5,6,7,8
        """,
        
        # billing with prices (for cost calculations)
        "billing_costs": """
            SELECT
                u.sku_name,
                u.billing_origin_product AS product,
                u.usage_metadata.warehouse_id AS warehouse_id,
                u.usage_metadata.job_id AS job_id,
                ROUND(SUM(u.usage_quantity), 2) AS dbus,
                ROUND(SUM(u.usage_quantity * COALESCE(lp.pricing.default, 0)), 2) AS cost
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices lp
                ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
                AND u.usage_date >= lp.price_start_time
                AND (lp.price_end_time IS NULL OR u.usage_date < lp.price_end_time)
            WHERE u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
            GROUP BY 1,2,3,4
        """,
        
        # compute.clusters: full snapshot
        "clusters": """
            SELECT cluster_id, cluster_name, cluster_source, auto_termination_minutes,
                   dbr_version, data_security_mode, policy_id,
                   min_autoscale_workers, max_autoscale_workers, worker_count, tags
            FROM system.compute.clusters
            WHERE delete_time IS NULL
        """,
        
        # information_schema.tables: full snapshot
        "tables": """
            SELECT table_catalog, table_schema, table_name, table_type, table_owner,
                   data_source_format, comment, created, last_altered
            FROM system.information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'default')
        """,
        
        # job runs aggregated
        "job_runs": """
            SELECT r.job_id, j.name AS job_name,
                   COUNT(*) AS total_runs,
                   SUM(CASE WHEN r.result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
                   SUM(CASE WHEN r.result_state = 'SUCCESS' THEN 1 ELSE 0 END) AS success_runs,
                   SUM(CASE WHEN r.trigger_type = 'MANUAL' THEN 1 ELSE 0 END) AS manual_runs,
                   AVG(r.run_duration_seconds) AS avg_duration_sec
            FROM system.lakeflow.job_run_timeline r
            LEFT JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
            WHERE r.period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())
            GROUP BY 1, 2
        """,
        
        # warehouses snapshot
        "warehouses": """
            SELECT warehouse_id, warehouse_name, warehouse_type, warehouse_size,
                   min_clusters, max_clusters, auto_stop_minutes, delete_time
            FROM system.compute.warehouses
            WHERE delete_time IS NULL
        """,
    }
    
    def __init__(self, executor):
        self.executor = executor
        self.data = {}
        self._lock = threading.Lock()
    
    def prefetch_all(self):
        """Run all prefetch queries in parallel. Returns dict of results."""
        import time as _time
        logger.info(f"Starting prefetch of {len(self.PREFETCH_QUERIES)} datasets...")
        start = _time.time()
        
        def _fetch_one(key, query):
            t0 = _time.time()
            try:
                rows = self.executor.execute(query, use_cache=False, timeout=180)
                elapsed = _time.time() - t0
                logger.info(f"Prefetch '{key}': {len(rows)} rows in {elapsed:.1f}s")
                return key, rows
            except Exception as e:
                elapsed = _time.time() - t0
                logger.warning(f"Prefetch '{key}' failed in {elapsed:.1f}s: {e}")
                return key, []
        
        results = {}
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(_fetch_one, k, q): k for k, q in self.PREFETCH_QUERIES.items()}
            for future in as_completed(futures):
                key, rows = future.result()
                results[key] = rows
        
        total_elapsed = _time.time() - start
        total_rows = sum(len(v) for v in results.values())
        logger.info(f"Prefetch complete: {len(results)} datasets, {total_rows} total rows in {total_elapsed:.1f}s")
        
        self.data = results
        return results


class BaseCheckRunner:
    """Base class for section check runners."""

    section_id: str = ""
    section_name: str = ""
    section_type: str = "core"  # core / conditional / advisory
    icon: str = ""

    def __init__(self, executor: QueryExecutor, api_client: APIClient, include_table_analysis: bool = False, prefetch_data: dict = None):
        self.executor = executor
        self.api = api_client
        self.include_table_analysis = include_table_analysis
        self._prefetch = prefetch_data or {}

    def pf(self, key: str) -> list:
        """Get prefetched data by key. Returns empty list if not available."""
        return self._prefetch.get(key, [])

    def pf_sum(self, key: str, field: str, filter_fn=None) -> float:
        """Sum a field from prefetched data, optionally filtering rows."""
        rows = self.pf(key)
        if filter_fn:
            rows = [r for r in rows if filter_fn(r)]
        return sum(float(r.get(field, 0) or 0) for r in rows)

    def pf_count(self, key: str, filter_fn=None) -> int:
        """Count rows from prefetched data, optionally filtering."""
        rows = self.pf(key)
        if filter_fn:
            return sum(1 for r in rows if filter_fn(r))
        return len(rows)

    def pf_distinct(self, key: str, field: str, filter_fn=None) -> set:
        """Get distinct values of a field from prefetched data."""
        rows = self.pf(key)
        if filter_fn:
            rows = [r for r in rows if filter_fn(r)]
        return set(r.get(field) for r in rows if r.get(field) is not None)

    def query_or_pf(self, sql: str, pf_key: str = None, pf_compute=None):
        """Try to compute result from prefetch data; fall back to SQL if unavailable.
        
        Usage:
            rows = self.query_or_pf(
                sql="SELECT COUNT(*) AS cnt FROM system.information_schema.tables ...",
                pf_key="tables",
                pf_compute=lambda data: [{"cnt": len(data)}]
            )
        """
        if pf_key and pf_compute:
            data = self.pf(pf_key)
            if data:
                try:
                    return pf_compute(data)
                except Exception:
                    pass  # Fall through to SQL
        return self.executor.execute(sql)

    def get_tables(self, filter_fn=None) -> list:
        """Get tables from prefetch or fall back to SQL."""
        data = self.pf('tables')
        if data:
            return [r for r in data if filter_fn(r)] if filter_fn else data
        rows = self.executor.execute("""
            SELECT table_catalog, table_schema, table_name, table_type, table_owner,
                   data_source_format, comment, created, last_altered
            FROM system.information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'default')""")
        return [r for r in rows if filter_fn(r)] if filter_fn else rows

    def get_clusters(self, filter_fn=None) -> list:
        """Get clusters from prefetch or fall back to SQL."""
        data = self.pf('clusters')
        if data:
            return [r for r in data if filter_fn(r)] if filter_fn else data
        rows = self.executor.execute("""
            SELECT cluster_id, cluster_name, cluster_source, auto_termination_minutes,
                   dbr_version, data_security_mode, policy_id,
                   min_autoscale_workers, max_autoscale_workers, worker_count, tags
            FROM system.compute.clusters WHERE delete_time IS NULL""")
        return [r for r in rows if filter_fn(r)] if filter_fn else rows

    def get_job_runs(self, filter_fn=None) -> list:
        """Get job run aggregates from prefetch or fall back to SQL."""
        data = self.pf('job_runs')
        if data:
            return [r for r in data if filter_fn(r)] if filter_fn else data
        rows = self.executor.execute("""
            SELECT r.job_id, j.name AS job_name,
                   COUNT(*) AS total_runs,
                   SUM(CASE WHEN r.result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
                   SUM(CASE WHEN r.result_state = 'SUCCESS' THEN 1 ELSE 0 END) AS success_runs,
                   SUM(CASE WHEN r.trigger_type = 'MANUAL' THEN 1 ELSE 0 END) AS manual_runs,
                   AVG(r.run_duration_seconds) AS avg_duration_sec
            FROM system.lakeflow.job_run_timeline r
            LEFT JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
            WHERE r.period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())
            GROUP BY 1, 2""")
        return [r for r in rows if filter_fn(r)] if filter_fn else rows

    def get_billing(self, filter_fn=None) -> list:
        """Get billing summary from prefetch or fall back to SQL."""
        data = self.pf('billing_summary')
        if data:
            return [r for r in data if filter_fn(r)] if filter_fn else data
        rows = self.executor.execute("""
            SELECT sku_name, billing_origin_product AS product,
                   usage_metadata.warehouse_id AS warehouse_id,
                   usage_metadata.cluster_id AS cluster_id,
                   usage_metadata.job_id AS job_id,
                   CASE WHEN sku_name LIKE '%%SERVERLESS%%' THEN 'serverless' ELSE 'classic' END AS compute_mode,
                   CASE WHEN sku_name LIKE '%%PHOTON%%' THEN 'photon' ELSE 'standard' END AS engine,
                   SUM(usage_quantity) AS dbus, COUNT(*) AS records
            FROM system.billing.usage
            WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
            GROUP BY 1,2,3,4,5,6,7""")
        return [r for r in rows if filter_fn(r)] if filter_fn else rows

    def get_billing_costs(self, filter_fn=None) -> list:
        """Get billing with costs from prefetch or fall back to SQL."""
        data = self.pf('billing_costs')
        if data:
            return [r for r in data if filter_fn(r)] if filter_fn else data
        rows = self.executor.execute("""
            SELECT u.sku_name, u.billing_origin_product AS product,
                   u.usage_metadata.warehouse_id AS warehouse_id,
                   u.usage_metadata.job_id AS job_id,
                   ROUND(SUM(u.usage_quantity), 2) AS dbus,
                   ROUND(SUM(u.usage_quantity * COALESCE(lp.pricing.default, 0)), 2) AS cost
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices lp
                ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
                AND u.usage_date >= lp.price_start_time
                AND (lp.price_end_time IS NULL OR u.usage_date < lp.price_end_time)
            WHERE u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
            GROUP BY 1,2,3,4""")
        return [r for r in rows if filter_fn(r)] if filter_fn else rows

    def get_qh_metrics(self) -> dict:
        """Get query history aggregate metrics from prefetch."""
        data = self.pf('qh_metrics')
        if data and len(data) > 0:
            return data[0]
        return {}

    def get_warehouses(self, filter_fn=None) -> list:
        """Get warehouses from prefetch or fall back to SQL."""
        data = self.pf('warehouses')
        if data:
            return [r for r in data if filter_fn(r)] if filter_fn else data
        rows = self.executor.execute("""
            SELECT warehouse_id, warehouse_name, warehouse_type, warehouse_size,
                   min_clusters, max_clusters, auto_stop_minutes
            FROM system.compute.warehouses WHERE delete_time IS NULL""")
        return [r for r in rows if filter_fn(r)] if filter_fn else rows



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
        with ThreadPoolExecutor(max_workers=10) as pool:
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
