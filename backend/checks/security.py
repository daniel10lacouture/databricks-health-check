"""Section 5: Security & Compliance — checks for network, IAM, audit, PII detection.
All checks include drill-down details with actual objects and recommendations."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class SecurityCheckRunner(BaseCheckRunner):
    section_id = "security"
    section_name = "Security & Compliance"
    section_type = "core"
    icon = "shield"

    def get_subsections(self):
        return ["Network Security", "Identity & Access Management",
                "Audit & Monitoring", "Data Protection & PII"]

    def check_5_1_1_ip_access_lists(self) -> CheckResult:
        try:
            resp = self.api.w.ip_access_lists.list()
            lists = list(resp)
        except Exception:
            return CheckResult("5.1.1", "IP access lists configured",
                "Network Security", 0, "not_evaluated",
                "Could not query IP access lists", "At least 1 IP access list")
        if lists:
            nc = [{"label": getattr(l, "label", ""), "list_type": str(getattr(l, "list_type", "")),
                   "ip_count": len(getattr(l, "ip_addresses", []) or []),
                   "enabled": getattr(l, "enabled", False)} for l in lists[:20]]
            return CheckResult("5.1.1", "IP access lists configured",
                "Network Security", 100, "pass",
                f"{len(lists)} IP access list(s) configured", "At least 1 IP access list",
                details={"non_conforming": nc, "summary": "IP access lists are configured."})
        return CheckResult("5.1.1", "IP access lists configured",
            "Network Security", 0, "fail",
            "No IP access lists", "At least 1 IP access list",
            details={"non_conforming": [], "summary": "No IP access lists configured — workspace is open to any IP."},
            recommendation=Recommendation(
                action="Configure IP access lists to restrict workspace access to known corporate IPs.",
                impact="Without IP restrictions, the workspace is accessible from any IP address.",
                priority="high",
                docs_url="https://docs.databricks.com/en/security/network/front-end/ip-access-list.html"))

    def check_5_2_5_pat_token_lifetime(self) -> CheckResult:
        try:
            tokens = list(self.api.w.token_management.list())
        except Exception:
            return CheckResult("5.2.5", "PAT token max lifetime configured",
                "Identity & Access Management", 0, "not_evaluated",
                "Could not query token management", "<=90 days")
        if not tokens:
            return CheckResult("5.2.5", "PAT token max lifetime configured",
                "Identity & Access Management", 100, "pass",
                "No PAT tokens in use", "<=90 days",
                details={"non_conforming": [], "summary": "No personal access tokens are active."})
        no_expiry = [t for t in tokens if not getattr(t, "expiry_time", None)]
        total = len(tokens) or 1
        no_expiry_pct = len(no_expiry) / total * 100
        if no_expiry_pct == 0: score, status = 100, "pass"
        elif no_expiry_pct < 50: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"token_id": str(getattr(t, "token_id", "")),
               "created_by": getattr(t, "created_by_username", ""),
               "comment": getattr(t, "comment", "")[:50],
               "action": "Set expiry to <=90 days"} for t in no_expiry[:20]]
        rec = None
        if no_expiry:
            rec = Recommendation(
                action=f"{len(no_expiry)} PAT token(s) have no expiry. Set max lifetime to 90 days.",
                impact="Unlimited tokens never expire — compromised tokens remain valid indefinitely.",
                priority="high",
                docs_url="https://docs.databricks.com/en/admin/access-control/tokens.html")
        return CheckResult("5.2.5", "PAT token max lifetime configured",
            "Identity & Access Management", score, status,
            f"{len(no_expiry)}/{total} tokens without expiry",
            "<=90 days", details={"non_conforming": nc}, recommendation=rec)

    def check_5_2_6_oauth_vs_pat(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT
                    CASE WHEN request_params.authenticationMethod LIKE '%OAUTH%' THEN 'oauth'
                         WHEN request_params.authenticationMethod LIKE '%PAT%' THEN 'pat'
                         ELSE 'other' END AS auth_type,
                    COUNT(*) AS cnt
                FROM system.access.audit
                WHERE event_time >= DATEADD(DAY, -30, CURRENT_DATE())
                    AND action_name = 'tokenLogin'
                GROUP BY 1""")
        except Exception:
            return CheckResult("5.2.6", "OAuth vs PAT usage ratio",
                "Identity & Access Management", 0, "not_evaluated",
                "Could not query audit logs", ">80% OAuth")
        counts = {r.get("auth_type",""): int(r.get("cnt",0)) for r in rows}
        total = sum(counts.values()) or 1
        oauth_pct = counts.get("oauth", 0) / total * 100
        if oauth_pct >= 80: score, status = 100, "pass"
        elif oauth_pct >= 30: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"auth_type": k, "count": v, "pct": round(v/total*100, 1)} for k, v in counts.items()]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"OAuth usage is {oauth_pct:.0f}%. Migrate PAT-based integrations to OAuth.",
                impact="OAuth provides better security with automatic token rotation.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html")
        return CheckResult("5.2.6", "OAuth vs PAT usage ratio",
            "Identity & Access Management", score, status,
            f"OAuth: {oauth_pct:.0f}%", ">80% OAuth",
            details={"non_conforming": nc}, recommendation=rec)

    def check_5_2_8_group_grants(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT grantee,
                    CASE WHEN grantee LIKE '%@%' THEN 'USER' ELSE 'GROUP' END AS grantee_type,
                    COUNT(*) AS grant_count
                FROM system.information_schema.catalog_privileges
                GROUP BY 1, 2""")
        except Exception:
            return CheckResult("5.2.8", "No direct user grants (use groups)",
                "Identity & Access Management", 0, "not_evaluated",
                "Could not query grants", "All grants to groups")
        total = len(rows) or 1
        direct_user = [r for r in rows if r.get("grantee_type","").upper() == "USER"]
        direct_pct = len(direct_user) / total * 100
        if direct_pct == 0: score, status = 100, "pass"
        elif direct_pct <= 20: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"grantee": r.get("grantee",""), "grant_count": r.get("grant_count",0),
               "action": "Move grants to a group, then add user to the group"} for r in direct_user[:20]]
        rec = None
        if direct_user:
            rec = Recommendation(
                action=f"{len(direct_user)} direct user grants found. Migrate to group-based access.",
                impact="Group-based grants are easier to manage and audit.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html")
        return CheckResult("5.2.8", "No direct user grants (use groups)",
            "Identity & Access Management", score, status,
            f"{direct_pct:.0f}% direct user grants ({len(direct_user)}/{total})",
            "0% direct user grants", details={"non_conforming": nc}, recommendation=rec)

    def check_5_3_1_system_tables_enabled(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT MAX(event_time) AS latest FROM system.access.audit
                WHERE event_time >= DATEADD(DAY, -2, CURRENT_DATE())""")
        except Exception:
            return CheckResult("5.3.1", "System tables enabled and receiving data",
                "Audit & Monitoring", 0, "fail",
                "Could not query audit logs — system tables may not be enabled",
                "Data within last 24 hours",
                details={"non_conforming": [], "summary": "System tables are not accessible."},
                recommendation=Recommendation(
                    action="Enable system tables for your account.",
                    impact="System tables are required for audit logging, cost analysis, and observability.",
                    priority="high",
                    docs_url="https://docs.databricks.com/en/admin/system-tables/index.html"))
        latest = rows[0].get("latest") if rows else None
        if latest:
            return CheckResult("5.3.1", "System tables enabled and receiving data",
                "Audit & Monitoring", 100, "pass",
                f"Latest event: {latest}", "Data within last 24 hours",
                details={"non_conforming": [], "summary": f"Audit data is current (latest: {latest})."})
        return CheckResult("5.3.1", "System tables enabled and receiving data",
            "Audit & Monitoring", 0, "fail",
            "No recent audit data", "Data within last 24 hours",
            details={"non_conforming": []},
            recommendation=Recommendation(
                action="Enable system tables — no recent audit data found.",
                impact="Required for security monitoring and compliance.",
                priority="high",
                docs_url="https://docs.databricks.com/en/admin/system-tables/index.html"))

    def check_5_3_5_cluster_access_mode(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT cluster_id, cluster_name, data_security_mode
                FROM system.compute.clusters
                WHERE delete_time IS NULL
                    AND cluster_source IN ('UI', 'API')
                    AND data_security_mode IN ('NO_ISOLATION', 'NONE')""")
        except Exception:
            return CheckResult("5.3.5", "Cluster access mode audit",
                "Audit & Monitoring", 0, "not_evaluated",
                "Could not query clusters", "No NO_ISOLATION interactive clusters")
        nc = [{"cluster_name": r.get("cluster_name",""), "cluster_id": r.get("cluster_id",""),
               "mode": r.get("data_security_mode",""),
               "action": "Change access mode to Single User or Shared in cluster settings"} for r in rows[:20]]
        if not rows:
            return CheckResult("5.3.5", "Cluster access mode audit",
                "Audit & Monitoring", 100, "pass",
                "All interactive clusters use proper access modes",
                "No NO_ISOLATION interactive clusters",
                details={"non_conforming": [], "summary": "All clusters have proper data security modes."})
        rec = Recommendation(
            action=f"{len(rows)} cluster(s) use NO_ISOLATION mode. Switch to Single User or Shared access mode.",
            impact="NO_ISOLATION clusters bypass UC access controls and audit logging.",
            priority="high",
            docs_url="https://docs.databricks.com/en/compute/configure.html#access-modes")
        return CheckResult("5.3.5", "Cluster access mode audit",
            "Audit & Monitoring", 0, "fail",
            f"{len(rows)} cluster(s) with NO_ISOLATION mode",
            "No NO_ISOLATION interactive clusters",
            details={"non_conforming": nc}, recommendation=rec)

    # ── 5.4 Data Protection & PII (NEW - Tier 1) ────────────────────

    def check_5_4_1_pii_detection(self) -> CheckResult:
        """Tier 1: PII/sensitive data detection from data_classification.results."""
        try:
            rows = self.executor.execute("""
                SELECT c.catalog_name, c.schema_name, c.table_name, c.column_name,
                    c.class_tag, c.confidence, c.data_type
                FROM system.data_classification.results c
                WHERE c.exclusion_state IS NULL OR c.exclusion_state != 'EXCLUDED'
                ORDER BY c.confidence DESC
                LIMIT 30""")
            # Count by classification type
            stats = self.executor.execute("""
                SELECT class_tag, COUNT(DISTINCT CONCAT(catalog_name, '.', schema_name, '.', table_name)) AS tables,
                    COUNT(*) AS columns
                FROM system.data_classification.results
                WHERE exclusion_state IS NULL OR exclusion_state != 'EXCLUDED'
                GROUP BY 1 ORDER BY 2 DESC""")
        except Exception:
            return CheckResult("5.4.1", "PII/sensitive data detection",
                "Data Protection & PII", 0, "not_evaluated",
                "Could not query data classification", "All PII columns masked")

        total_classified = sum(int(s.get("columns", 0)) for s in stats)
        total_tables = sum(int(s.get("tables", 0)) for s in stats)

        nc = [{"catalog": r.get("catalog_name",""), "schema": r.get("schema_name",""),
               "table": r.get("table_name",""), "column": r.get("column_name",""),
               "classification": r.get("class_tag",""), "confidence": r.get("confidence",""),
               "action": f"Apply column mask: ALTER TABLE ... ALTER COLUMN {r.get('column_name','')} SET MASK"
              } for r in rows[:20]]

        # Check if column masks are in use
        try:
            mask_rows = self.executor.execute("""
                SELECT COUNT(*) AS cnt FROM system.information_schema.column_masks""")
            mask_count = int(mask_rows[0].get("cnt", 0)) if mask_rows else 0
        except Exception:
            mask_count = 0

        if total_classified == 0:
            return CheckResult("5.4.1", "PII/sensitive data detection",
                "Data Protection & PII", 100, "pass",
                "No PII/sensitive data classified", "All PII columns masked",
                details={"non_conforming": [], "summary": "No sensitive data classifications found."})

        # PII exists — are there masks?
        if mask_count > 0 and mask_count >= total_classified * 0.5:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        class_summary = [{"classification": s.get("class_tag",""), "tables": s.get("tables",0),
                         "columns": s.get("columns",0)} for s in stats[:10]]

        rec = Recommendation(
            action=f"{total_classified} PII columns found across {total_tables} tables with {mask_count} column masks applied. Apply column masks to protect sensitive data.",
            impact="Unmasked PII columns expose sensitive data to all users with table access. Column masks enforce data protection at query time.",
            priority="high",
            docs_url="https://docs.databricks.com/en/data-governance/unity-catalog/column-masks.html")

        return CheckResult("5.4.1", "PII/sensitive data detection",
            "Data Protection & PII", score, status,
            f"{total_classified} PII columns in {total_tables} tables, {mask_count} masks applied",
            "All PII columns masked",
            details={"non_conforming": nc, "classification_summary": class_summary},
            recommendation=rec)

    def check_5_4_2_row_level_security(self) -> CheckResult:
        """Check row-level security (row filter) adoption."""
        try:
            rows = self.executor.execute("""
                SELECT COUNT(*) AS cnt FROM system.information_schema.row_filters""")
            mask_rows = self.executor.execute("""
                SELECT COUNT(*) AS cnt FROM system.information_schema.column_masks""")
        except Exception:
            return CheckResult("5.4.2", "Row-level security & column masking",
                "Data Protection & PII", 0, "not_evaluated",
                "Could not query", "RLS and masking configured for sensitive tables")

        filters = int(rows[0].get("cnt", 0)) if rows else 0
        masks = int(mask_rows[0].get("cnt", 0)) if mask_rows else 0

        if filters > 0 and masks > 0: score, status = 100, "pass"
        elif filters > 0 or masks > 0: score, status = 50, "partial"
        else: score, status = 0, "fail"

        nc = [{"feature": "Row Filters", "count": filters,
               "action": "Apply row filters to restrict data visibility per user/group"},
              {"feature": "Column Masks", "count": masks,
               "action": "Apply column masks to redact PII columns"}]

        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"Row filters: {filters}, Column masks: {masks}. Add row-level security and column masking for sensitive tables.",
                impact="Fine-grained access control ensures users only see data appropriate to their role.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/data-governance/unity-catalog/row-filters.html")

        return CheckResult("5.4.2", "Row-level security & column masking",
            "Data Protection & PII", score, status,
            f"{filters} row filters, {masks} column masks",
            "RLS and masking configured for sensitive tables",
            details={"non_conforming": nc}, recommendation=rec)
    def check_5_4_3_outbound_network_activity(self) -> CheckResult:
        """Track outbound network destinations — informational for security review."""
        try:
            rows = self.executor.execute("""
                SELECT destination_type, 
                       COUNT(DISTINCT destination) AS unique_destinations,
                       COUNT(*) AS total_events
                FROM system.access.outbound_network
                WHERE event_time >= DATEADD(DAY, -7, CURRENT_DATE())
                GROUP BY 1 ORDER BY total_events DESC
            """)
        except Exception as e:
            return CheckResult("5.4.3", "Outbound network activity",
                "Data Protection", None, "info",
                f"Could not query: {str(e)[:60]}", "N/A")

        if not rows:
            return CheckResult("5.4.3", "Outbound network activity",
                "Data Protection", None, "info",
                "No outbound network events recorded", "N/A")

        total_events = sum(int(r.get("total_events", 0)) for r in rows)
        nc = [{"destination_type": r.get("destination_type", "unknown"),
               "unique_destinations": r.get("unique_destinations", 0),
               "total_events": int(r.get("total_events", 0))} for r in rows[:10]]

        # Get top destinations for review
        top_dests = self.executor.execute("""
            SELECT destination, destination_type, COUNT(*) AS events
            FROM system.access.outbound_network
            WHERE event_time >= DATEADD(DAY, -7, CURRENT_DATE())
            GROUP BY 1, 2 ORDER BY events DESC LIMIT 20
        """)
        dests = [{"destination": r.get("destination", "")[:60], 
                  "type": r.get("destination_type", ""),
                  "events": int(r.get("events", 0))} for r in (top_dests or [])[:15]]

        return CheckResult("5.4.3", "Outbound network activity (7d)",
            "Data Protection", None, "info",
            f"{total_events:,} outbound events to {sum(r['unique_destinations'] for r in rows)} destinations",
            "Review for unexpected egress",
            details={"by_destination_type": nc, "top_destinations": dests,
                     "summary": "Review destinations for unexpected data egress patterns"},
            recommendation=None)

    def check_5_4_4_column_lineage_coverage(self) -> CheckResult:
        """Check if column-level lineage is being tracked."""
        try:
            rows = self.executor.execute("""
                SELECT COUNT(DISTINCT CONCAT(source_table_catalog, '.', source_table_schema, '.', source_table_name)) AS source_tables,
                       COUNT(DISTINCT CONCAT(target_table_catalog, '.', target_table_schema, '.', target_table_name)) AS target_tables,
                       COUNT(*) AS lineage_edges
                FROM system.access.column_lineage
                WHERE event_time >= DATEADD(DAY, -30, CURRENT_DATE())
            """)
        except Exception as e:
            return CheckResult("5.4.4", "Column lineage coverage",
                "Data Protection", None, "info",
                f"Could not query: {str(e)[:60]}", "N/A")

        r = rows[0] if rows else {}
        sources = int(r.get("source_tables", 0) or 0)
        targets = int(r.get("target_tables", 0) or 0)
        edges = int(r.get("lineage_edges", 0) or 0)

        if edges == 0:
            return CheckResult("5.4.4", "Column lineage coverage",
                "Data Protection", None, "info",
                "No column lineage data available", "N/A",
                recommendation=Recommendation(
                    action="Enable Unity Catalog lineage for data governance visibility.",
                    impact="Column-level lineage helps track sensitive data flows and impact analysis.",
                    priority="low",
                    docs_url="https://docs.databricks.com/en/data-governance/unity-catalog/data-lineage.html"))

        return CheckResult("5.4.4", "Column lineage coverage (30d)",
            "Data Protection", None, "info",
            f"{edges:,} lineage edges across {sources} source → {targets} target tables",
            "Track data flows",
            details={"source_tables": sources, "target_tables": targets, "lineage_edges": edges,
                     "summary": "Column-level lineage is being tracked"},
            recommendation=None)
