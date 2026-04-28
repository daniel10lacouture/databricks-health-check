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
                "Audit & Monitoring", "Data Protection & PII", "Token & Credential Hygiene"]

    def check_5_1_1_ip_access_lists(self) -> CheckResult:
        try:
            resp = self.api.w.ip_access_lists.list()
            lists = list(resp)
        except Exception:
            return CheckResult("5.1.1", "IP access lists configured",
                "Network Security", 0, "not_evaluated",
                "Requires workspace admin permissions (grant to app service principal)", "At least 1 IP access list",
                details={"summary": "This check requires workspace admin permissions on the app service principal. Go to Admin Settings → Service Principals → grant Workspace Admin role to the health check app SP."})
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
                "Requires workspace admin permissions (grant to app service principal)", "<=90 days",
                details={"summary": "This check requires workspace admin permissions on the app service principal. Go to Admin Settings → Service Principals → grant Workspace Admin role to the health check app SP."})
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

        class_summary = [{"classification": s.get("class_tag",""), "tables": s.get("tables",0),
                         "columns": s.get("columns",0)} for s in stats[:10]]

        if total_classified == 0:
            return CheckResult("5.4.1", "PII/sensitive data detection",
                "Data Protection & PII", 100, "pass",
                "No PII/sensitive data classified", "Data classification enabled",
                details={"non_conforming": [], "summary": "No sensitive data classifications found."})

        # PII exists — data classification is working. Score on detection coverage, not masking.
        # Masking is handled by check 5.6.2.
        score, status = 100, "pass"

        nc_display = [{"catalog": r.get("catalog_name",""), "schema": r.get("schema_name",""),
               "table": r.get("table_name",""), "column": r.get("column_name",""),
               "classification": r.get("class_tag",""), "confidence": r.get("confidence",""),
               "status": "Detected — see PII masking check for protection status"
              } for r in rows[:20]]

        return CheckResult("5.4.1", "PII/sensitive data detection",
            "Data Protection & PII", score, status,
            f"Data classification active: {total_classified} PII columns detected across {total_tables} tables",
            "Data classification enabled",
            details={"non_conforming": nc_display, "classification_summary": class_summary},
            recommendation=Recommendation(
                action=f"Data classification is active and detecting {total_classified} sensitive columns. See 'PII columns without masking' check for protection status.",
                impact="Having data classification enabled is the critical first step to PII protection.",
                priority="low",
                docs_url="https://docs.databricks.com/en/data-governance/unity-catalog/data-classification.html"))

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

    # ── 5.5 Network Traffic Analysis ─────────────────────────────────

    def check_5_5_1_network_denied_traffic(self) -> CheckResult:
        """Analyze denied inbound network requests."""
        try:
            rows = self.executor.execute("""
                SELECT source.ip AS source_ip, policy_outcome, COUNT(*) AS denied_requests,
                       MIN(event_time) AS first_seen, MAX(event_time) AS last_seen,
                       COUNT(DISTINCT date_format(event_time, 'yyyy-MM-dd')) AS active_days
                FROM system.access.inbound_network
                WHERE event_time >= DATEADD(DAY, -30, CURRENT_DATE()) AND policy_outcome = 'DENY'
                GROUP BY source.ip, policy_outcome ORDER BY denied_requests DESC LIMIT 30
            """)
        except Exception as e:
            return CheckResult("5.5.1", "Denied inbound traffic (30d)", "Network Traffic Analysis",
                0, "not_evaluated", f"Could not query inbound_network: {str(e)[:80]}", "N/A")

        if not rows:
            return CheckResult("5.5.1", "Denied inbound traffic (30d)", "Network Traffic Analysis",
                100, "pass", "No denied inbound traffic detected in the last 30 days",
                "Monitor denied requests",
                details={"summary": "No DENY policy outcomes found in inbound network logs. This means either no unauthorized access attempts occurred or inbound network policies are not configured."})

        total_denied = sum(int(r.get("denied_requests", 0) or 0) for r in rows)
        unique_ips = len(rows)
        if total_denied < 100: score, status = 90, "pass"
        elif total_denied < 1000: score, status = 70, "partial"
        elif unique_ips > 10: score, status = 40, "fail"
        else: score, status = 50, "partial"

        nc = [{"source_ip": r.get("source_ip", ""), "denied_requests": r.get("denied_requests", 0),
               "active_days": r.get("active_days", 0), "last_seen": str(r.get("last_seen", ""))[:19]} for r in rows[:15]]

        rec = None
        if score < 90:
            rec = Recommendation(
                action=f"{total_denied:,} denied requests from {unique_ips} IPs in 30d. Review for potential attack patterns and update IP access lists.",
                impact="High denied traffic may indicate unauthorized access attempts or misconfigured clients.",
                priority="high" if unique_ips > 10 else "medium",
                docs_url="https://docs.databricks.com/en/security/network/front-end/ip-access-list.html")
        return CheckResult("5.5.1", "Denied inbound traffic (30d)", "Network Traffic Analysis",
            score, status, f"{total_denied:,} denied requests from {unique_ips} unique IPs",
            "Monitor denied requests", details={"non_conforming": nc}, recommendation=rec)

    def check_5_5_2_outbound_network(self) -> CheckResult:
        """Audit outbound network connections."""
        try:
            rows = self.executor.execute("""
                SELECT destination, destination_type, COUNT(*) AS connection_count,
                       COUNT(DISTINCT network_source_type) AS source_types,
                       COUNT(DISTINCT date_format(event_time, 'yyyy-MM-dd')) AS active_days
                FROM system.access.outbound_network
                WHERE event_time >= DATEADD(DAY, -30, CURRENT_DATE())
                GROUP BY destination, destination_type ORDER BY connection_count DESC LIMIT 30
            """)
        except Exception as e:
            return CheckResult("5.5.2", "Outbound network audit (30d)", "Network Traffic Analysis",
                0, "not_evaluated", f"Could not query outbound_network: {str(e)[:80]}", "N/A")

        if not rows:
            return CheckResult("5.5.2", "Outbound network audit (30d)", "Network Traffic Analysis",
                None, "info", "No outbound network data available", "Review external destinations")

        total_destinations = len(rows)
        total_connections = sum(int(r.get("connection_count", 0) or 0) for r in rows)
        nc = [{"destination": r.get("destination", ""), "type": r.get("destination_type", ""),
               "connections": r.get("connection_count", 0), "active_days": r.get("active_days", 0)} for r in rows[:20]]

        return CheckResult("5.5.2", "Outbound network audit (30d)", "Network Traffic Analysis",
            None, "info", f"{total_connections:,} connections to {total_destinations} destinations",
            "Review external destinations",
            details={"non_conforming": nc, "summary": "Review outbound destinations for unauthorized data exfiltration risk"},
            recommendation=Recommendation(
                action=f"Review {total_destinations} outbound destinations. Ensure all external endpoints are authorized and expected.",
                impact="Outbound network monitoring is critical for detecting data exfiltration and unauthorized API calls.",
                priority="medium", docs_url="https://docs.databricks.com/en/security/network/classic/egress.html"))

    # ── 5.6 Data Classification ──────────────────────────────────────

    def check_5_6_1_data_classification_coverage(self) -> CheckResult:
        """Check data classification coverage across tables."""
        try:
            rows = self.executor.execute("""
                WITH classified AS (
                    SELECT COUNT(DISTINCT CONCAT(catalog_name, '.', schema_name, '.', table_name)) AS classified_tables
                    FROM system.data_classification.results
                ),
                total AS (
                    SELECT COUNT(*) AS total_tables FROM system.information_schema.tables
                    WHERE table_schema != 'information_schema'
                )
                SELECT c.classified_tables, t.total_tables,
                       CASE WHEN t.total_tables > 0 THEN ROUND(c.classified_tables * 100.0 / t.total_tables, 1) ELSE 0 END AS coverage_pct
                FROM classified c, total t
            """)
        except Exception as e:
            return CheckResult("5.6.1", "Data classification coverage", "Data Classification",
                0, "not_evaluated", f"Could not query data_classification: {str(e)[:80]}", "N/A")

        if not rows:
            return CheckResult("5.6.1", "Data classification coverage", "Data Classification",
                None, "info", "No classification data available", ">=50% tables classified")

        r = rows[0]
        classified = int(r.get("classified_tables", 0) or 0)
        total = int(r.get("total_tables", 0) or 0)
        pct = float(r.get("coverage_pct", 0) or 0)

        if pct >= 70: score, status = 100, "pass"
        elif pct >= 50: score, status = 75, "partial"
        elif pct >= 20: score, status = 50, "partial"
        elif classified > 0: score, status = 30, "fail"
        else: score, status = 10, "fail"

        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"Expand data classification: {classified}/{total} tables ({pct:.0f}%) classified. Prioritize tables with customer/financial data.",
                impact="Data classification is foundational for PII protection, GDPR/CCPA compliance, and access policy enforcement.",
                priority="high" if pct < 20 else "medium",
                docs_url="https://docs.databricks.com/en/data-governance/unity-catalog/data-classification.html")
        return CheckResult("5.6.1", "Data classification coverage", "Data Classification",
            score, status, f"{pct:.0f}% ({classified:,}/{total:,} tables classified)",
            ">=50% tables classified", recommendation=rec)

    def check_5_6_2_pii_tables_without_protection(self) -> CheckResult:
        """Identify tables with PII that lack column masking."""
        try:
            rows = self.executor.execute("""
                SELECT DISTINCT dc.catalog_name, dc.schema_name, dc.table_name, dc.column_name,
                       dc.class_tag AS classification
                FROM system.data_classification.results dc
                LEFT JOIN system.information_schema.column_masks cm
                    ON dc.catalog_name = cm.table_catalog AND dc.schema_name = cm.table_schema
                    AND dc.table_name = cm.table_name AND dc.column_name = cm.column_name
                WHERE dc.class_tag IN ('PII', 'SENSITIVE', 'EMAIL', 'PHONE', 'SSN', 'ADDRESS', 'NAME')
                  AND cm.column_name IS NULL
                ORDER BY dc.catalog_name, dc.schema_name, dc.table_name LIMIT 50
            """)
        except Exception as e:
            return CheckResult("5.6.2", "PII columns without masking", "Data Classification",
                0, "not_evaluated", f"Could not query: {str(e)[:80]}", "N/A")

        if not rows:
            return CheckResult("5.6.2", "PII columns without masking", "Data Classification",
                100, "pass", "All PII columns have masking policies or no PII detected", "0 unmasked PII columns")

        unique_tables = len(set(f"{r['catalog_name']}.{r['schema_name']}.{r['table_name']}" for r in rows))
        if len(rows) <= 5: score, status = 60, "partial"
        elif len(rows) <= 20: score, status = 35, "fail"
        else: score, status = 15, "fail"

        nc = [{"table": f"{r['catalog_name']}.{r['schema_name']}.{r['table_name']}",
               "column": r.get("column_name", ""), "classification": r.get("classification", "")} for r in rows[:20]]

        rec = Recommendation(
            action=f"{len(rows)} PII columns across {unique_tables} tables lack masking. Apply column masks to protect sensitive data.",
            impact="Unmasked PII columns expose the organization to data breach risk and regulatory non-compliance (GDPR, CCPA).",
            priority="high", docs_url="https://docs.databricks.com/en/data-governance/unity-catalog/column-masking.html")
        return CheckResult("5.6.2", "PII columns without masking", "Data Classification",
            score, status, f"{len(rows)} PII columns in {unique_tables} tables without masking",
            "0 unmasked PII columns", details={"non_conforming": nc}, recommendation=rec)

    # ── 5.7 Token & Credential Hygiene ────────────────────────────────

    def check_5_7_1_token_hygiene(self) -> CheckResult:
        """Analyze PAT token creation, usage, and rotation patterns."""
        try:
            rows = self.executor.execute("""
                WITH token_events AS (
                    SELECT action_name,
                           user_identity.email AS user_email,
                           event_time,
                           request_params['tokenId'] AS token_id,
                           request_params['comment'] AS token_comment
                    FROM system.access.audit
                    WHERE event_time >= DATEADD(DAY, -90, CURRENT_DATE())
                      AND action_name IN ('generateDbToken', 'revokeDbToken', 'garbageCollectDbToken', 'tokenLogin')
                ),
                summary AS (
                    SELECT 
                        COUNT(CASE WHEN action_name = 'generateDbToken' THEN 1 END) AS tokens_created,
                        COUNT(CASE WHEN action_name = 'revokeDbToken' THEN 1 END) AS tokens_revoked,
                        COUNT(CASE WHEN action_name = 'garbageCollectDbToken' THEN 1 END) AS tokens_gc,
                        COUNT(DISTINCT CASE WHEN action_name = 'tokenLogin' THEN user_email END) AS users_with_pat_login,
                        COUNT(CASE WHEN action_name = 'tokenLogin' THEN 1 END) AS total_pat_logins
                    FROM token_events
                ),
                top_pat_users AS (
                    SELECT user_email, COUNT(*) AS pat_logins
                    FROM token_events
                    WHERE action_name = 'tokenLogin' AND user_email IS NOT NULL
                    GROUP BY user_email
                    ORDER BY pat_logins DESC
                    LIMIT 10
                )
                SELECT s.*, COLLECT_LIST(STRUCT(t.user_email, t.pat_logins)) AS top_users
                FROM summary s
                CROSS JOIN top_pat_users t
                GROUP BY s.tokens_created, s.tokens_revoked, s.tokens_gc, s.users_with_pat_login, s.total_pat_logins
            """)
        except Exception as e:
            return CheckResult("5.7.1", "Token & credential hygiene (90d)", "Token & Credential Hygiene",
                0, "not_evaluated", f"Could not query: {str(e)[:80]}", "N/A")

        if not rows:
            return CheckResult("5.7.1", "Token & credential hygiene (90d)", "Token & Credential Hygiene",
                None, "info", "No token audit events found", "Monitor PAT token lifecycle")

        r = rows[0]
        created = int(r.get("tokens_created", 0) or 0)
        revoked = int(r.get("tokens_revoked", 0) or 0)
        gc = int(r.get("tokens_gc", 0) or 0)
        pat_users = int(r.get("users_with_pat_login", 0) or 0)
        pat_logins = int(r.get("total_pat_logins", 0) or 0)

        # Rotation ratio: higher revoke/create ratio = better hygiene
        rotation_ratio = revoked / max(created, 1) * 100
        # Score based on whether tokens are being actively managed
        score = 90 if rotation_ratio > 50 else 70 if rotation_ratio > 20 else 50 if created > 0 else 80
        status = "pass" if score >= 80 else "partial" if score >= 50 else "fail"

        nc = [{"metric": "PAT tokens created (90d)", "value": f"{created:,}"},
              {"metric": "PAT tokens revoked (90d)", "value": f"{revoked:,}"},
              {"metric": "Tokens garbage collected", "value": f"{gc:,}"},
              {"metric": "Rotation ratio (revoked/created)", "value": f"{rotation_ratio:.1f}%"},
              {"metric": "Users authenticating via PAT", "value": f"{pat_users:,}"},
              {"metric": "Total PAT login events (90d)", "value": f"{pat_logins:,}"}]

        rec = Recommendation(
            action=f"{created} tokens created vs {revoked} revoked (rotation: {rotation_ratio:.0f}%). "
                   f"{pat_users} users still authenticating via PATs ({pat_logins:,} logins). "
                   f"Migrate to OAuth or service principal authentication and enforce token expiry policies.",
            impact="Unrotated PATs are a top security risk — they provide persistent access without MFA. OAuth tokens expire automatically.",
            priority="high" if pat_users > 50 and rotation_ratio < 20 else "medium",
            docs_url="https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html")

        return CheckResult("5.7.1", "Token & credential hygiene (90d)", "Token & Credential Hygiene",
            score, status,
            f"{created} tokens created, {revoked} revoked ({rotation_ratio:.0f}% rotation), {pat_users} PAT users",
            "> 50% token rotation ratio, migrate to OAuth",
            details={"non_conforming": nc},
            recommendation=rec)

