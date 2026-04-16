"""Section 6: Governance (Unity Catalog) — checks for UC adoption, access control, lineage, tagging, volumes.
All checks include drill-down details with actual objects and recommendations."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class GovernanceCheckRunner(BaseCheckRunner):
    section_id = "governance"
    section_name = "Governance (Unity Catalog)"
    section_type = "core"
    icon = "lock"

    def get_subsections(self):
        return ["UC Adoption", "Access Control Patterns", "Lineage & Classification",
                "Volume & Storage Governance", "Migration & Workspace Governance"]

    def check_6_1_1_uc_adoption(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT table_catalog,
                    SUM(CASE WHEN table_catalog != 'hive_metastore' THEN 1 ELSE 0 END) AS uc_tables,
                    COUNT(*) AS total
                FROM system.information_schema.tables
                WHERE table_schema != 'information_schema'
                GROUP BY 1 ORDER BY 3 DESC""")
        except Exception:
            return CheckResult("6.1.1", "UC-managed tables %",
                "UC Adoption", 0, "not_evaluated", "Could not query", ">95% UC")
        uc = sum(int(r.get("uc_tables", 0)) for r in rows)
        total = sum(int(r.get("total", 0)) for r in rows) or 1
        pct = uc / total * 100
        if pct > 95: score, status = 100, "pass"
        elif pct >= 50: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"catalog": r.get("table_catalog",""), "tables": r.get("total",0)} for r in rows[:20]]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"{100-pct:.0f}% of tables not in UC. Migrate remaining tables from hive_metastore.",
                impact="UC provides centralized governance, lineage, and fine-grained access control.",
                priority="high" if pct < 50 else "medium",
                docs_url="https://docs.databricks.com/en/data-governance/unity-catalog/migrate.html")
        return CheckResult("6.1.1", "UC-managed tables %",
            "UC Adoption", score, status,
            f"{pct:.0f}% in UC ({uc}/{total})", ">95% UC",
            details={"non_conforming": nc}, recommendation=rec)

    def check_6_2_4_catalog_ownership(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT catalog_name, catalog_owner
                FROM system.information_schema.catalogs
                WHERE catalog_name != 'hive_metastore'""")
        except Exception:
            return CheckResult("6.2.4", "Catalog/schema ownership by groups",
                "Access Control Patterns", 0, "not_evaluated", "Could not query", "All owned by groups")
        total = len(rows) or 1
        individual = [r for r in rows if r.get("catalog_owner","") and "@" in str(r.get("catalog_owner",""))]
        pct = len(individual) / total * 100
        if pct == 0: score, status = 100, "pass"
        elif pct <= 20: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"catalog": r.get("catalog_name",""), "owner": r.get("catalog_owner",""),
               "action": "ALTER CATALOG ... SET OWNER TO <group>"} for r in individual[:20]]
        # Also show passing catalogs
        if not individual:
            nc = [{"catalog": r.get("catalog_name",""), "owner": r.get("catalog_owner",""),
                   "status": "OK - group owned"} for r in rows[:20]]
        rec = None
        if individual:
            rec = Recommendation(
                action=f"Transfer ownership of {len(individual)} catalog(s) from individuals to groups.",
                impact="Individual ownership creates single points of failure.",
                priority="medium")
        return CheckResult("6.2.4", "Catalog/schema ownership by groups",
            "Access Control Patterns", score, status,
            f"{len(individual)}/{total} owned by individuals",
            "All owned by groups", details={"non_conforming": nc}, recommendation=rec)

    # ── 6.2.5 Fine-Grained Access Audit (Tier 2) ────────────────────

    def check_6_2_5_table_privilege_audit(self) -> CheckResult:
        """Tier 2: Audit table-level privileges for over-permissioning."""
        try:
            rows = self.executor.execute("""
                SELECT grantee, privilege_type, table_catalog, table_schema, table_name,
                    CASE WHEN grantee LIKE '%@%' THEN 'USER' ELSE 'GROUP/SP' END AS grantee_type
                FROM system.information_schema.table_privileges
                ORDER BY grantee, table_catalog, table_schema, table_name
                LIMIT 30""")
            stats = self.executor.execute("""
                SELECT COUNT(*) AS total_grants,
                    COUNT(DISTINCT grantee) AS grantees,
                    COUNT(DISTINCT CONCAT(table_catalog, '.', table_schema, '.', table_name)) AS tables_with_grants,
                    SUM(CASE WHEN grantee LIKE '%@%' THEN 1 ELSE 0 END) AS direct_user_grants
                FROM system.information_schema.table_privileges""")
        except Exception:
            return CheckResult("6.2.5", "Table-level privilege audit",
                "Access Control Patterns", 0, "not_evaluated",
                "Could not query", "Privileges granted via groups, not individuals")

        s = stats[0] if stats else {}
        total_grants = int(s.get("total_grants", 0))
        direct = int(s.get("direct_user_grants", 0))
        pct_direct = (direct / max(total_grants, 1)) * 100

        if pct_direct == 0: score, status = 100, "pass"
        elif pct_direct <= 30: score, status = 50, "partial"
        else: score, status = 0, "fail"

        nc = [{"grantee": r.get("grantee",""), "privilege": r.get("privilege_type",""),
               "table": f"{r.get('table_catalog','')}.{r.get('table_schema','')}.{r.get('table_name','')}",
               "grantee_type": r.get("grantee_type",""),
               "action": "REVOKE and re-grant to a group"} for r in rows if r.get("grantee_type") == "USER"]
        if not nc:
            nc = [{"grantee": r.get("grantee",""), "privilege": r.get("privilege_type",""),
                   "table": f"{r.get('table_catalog','')}.{r.get('table_schema','')}.{r.get('table_name','')}",
                   "status": "OK - group/SP"} for r in rows[:20]]

        rec = None
        if direct > 0:
            rec = Recommendation(
                action=f"{direct} direct-user table grants found ({pct_direct:.0f}%). Migrate to group-based grants.",
                impact="Direct user grants are hard to audit, don't scale, and persist after employee departure.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html")

        return CheckResult("6.2.5", "Table-level privilege audit",
            "Access Control Patterns", score, status,
            f"{total_grants} grants, {direct} direct-user ({pct_direct:.0f}%)",
            "Privileges granted via groups, not individuals",
            details={"non_conforming": nc}, recommendation=rec)

    def check_6_3_1_lineage_coverage(self) -> CheckResult:
        try:
            total_rows = self.executor.execute("""
                SELECT COUNT(*) AS total FROM system.information_schema.tables
                WHERE table_schema != 'information_schema'
                  AND table_catalog NOT LIKE '__databricks%'
                  AND table_catalog NOT IN ('system', 'samples')""")
            rows = self.executor.execute("""
                SELECT COUNT(DISTINCT l.target_table_full_name) AS lineage_tables
                FROM system.access.table_lineage l
                INNER JOIN system.information_schema.tables t
                  ON l.target_table_full_name = CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name)
                WHERE l.event_time >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND t.table_catalog NOT LIKE '__databricks%'
                  AND t.table_catalog NOT IN ('system', 'samples')""")
        except Exception:
            return CheckResult("6.3.1", "Table lineage coverage",
                "Lineage & Classification", 0, "not_evaluated",
                "Could not query lineage", ">80% coverage")
        lineage = int(rows[0].get("lineage_tables", 0)) if rows else 0
        total = int(total_rows[0].get("total", 0)) if total_rows else 1
        pct = lineage / max(total, 1) * 100
        if pct >= 80: score, status = 100, "pass"
        elif pct >= 30: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"metric": "Tables with lineage", "value": lineage},
              {"metric": "Total tables", "value": total},
              {"metric": "Coverage", "value": f"{pct:.0f}%"}]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"Lineage covers {pct:.0f}% of tables. Use UC-enabled compute for ETL to auto-capture lineage.",
                impact="Lineage enables impact analysis and regulatory compliance.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/data-governance/unity-catalog/data-lineage.html")
        return CheckResult("6.3.1", "Table lineage coverage",
            "Lineage & Classification", score, status,
            f"{pct:.0f}% of tables have lineage ({lineage}/{total})",
            ">80% coverage", details={"non_conforming": nc}, recommendation=rec)

    # ── 6.4 Volume & Storage Governance (Tier 2) ─────────────────────

    def check_6_4_1_volume_inventory(self) -> CheckResult:
        """Tier 2: Volume governance — inventory managed vs external."""
        try:
            rows = self.executor.execute("""
                SELECT volume_catalog, volume_schema, volume_name, volume_type, volume_owner
                FROM system.information_schema.volumes
                ORDER BY volume_type, volume_catalog, volume_schema
                LIMIT 30""")
            stats = self.executor.execute("""
                SELECT volume_type, COUNT(*) AS cnt
                FROM system.information_schema.volumes
                GROUP BY 1""")
        except Exception:
            return CheckResult("6.4.1", "Volume inventory & governance",
                "Volume & Storage Governance", 0, "not_evaluated",
                "Could not query volumes", "All volumes managed with proper ownership")

        counts = {r.get("volume_type",""): int(r.get("cnt",0)) for r in stats}
        total = sum(counts.values())
        external = counts.get("EXTERNAL", 0)
        managed = counts.get("MANAGED", 0)

        nc = [{"catalog": r.get("volume_catalog",""), "schema": r.get("volume_schema",""),
               "volume": r.get("volume_name",""), "type": r.get("volume_type",""),
               "owner": r.get("volume_owner","")} for r in rows[:20]]

        if total == 0:
            return CheckResult("6.4.1", "Volume inventory & governance",
                "Volume & Storage Governance", 0, "info",
                "No volumes found", "Volumes configured for unstructured data",
                details={"non_conforming": []})

        ext_pct = external / max(total, 1) * 100
        if ext_pct <= 20: score, status = 100, "pass"
        elif ext_pct <= 50: score, status = 50, "partial"
        else: score, status = 0, "fail"

        rec = None
        if external > 0:
            rec = Recommendation(
                action=f"{external} external volumes found ({ext_pct:.0f}%). Convert to managed volumes where possible.",
                impact="Managed volumes benefit from Unity Catalog governance and access control.",
                priority="low")

        return CheckResult("6.4.1", "Volume inventory & governance",
            "Volume & Storage Governance", score, status,
            f"{total} volumes ({managed} managed, {external} external)",
            "All volumes managed with proper ownership",
            details={"non_conforming": nc}, recommendation=rec)

    def check_6_4_2_external_locations(self) -> CheckResult:
        """Check external locations and storage credentials."""
        try:
            rows = self.executor.execute("""
                SELECT external_location_name, url, credential_name, external_location_owner
                FROM system.information_schema.external_locations
                LIMIT 20""")
            creds = self.executor.execute("""
                SELECT credential_name, credential_type, credential_owner
                FROM system.information_schema.storage_credentials
                LIMIT 20""")
        except Exception:
            return CheckResult("6.4.2", "External locations & credentials",
                "Volume & Storage Governance", 0, "not_evaluated",
                "Could not query", "External locations secured with named credentials")

        nc_locs = [{"location": r.get("external_location_name",""), "url": r.get("url","")[:60],
                    "credential": r.get("credential_name",""), "owner": r.get("external_location_owner","")} for r in rows[:10]]
        nc_creds = [{"credential": r.get("credential_name",""), "type": r.get("credential_type",""),
                     "owner": r.get("credential_owner","")} for r in creds[:10]]
        nc = nc_locs + nc_creds

        return CheckResult("6.4.2", "External locations & credentials",
            "Volume & Storage Governance", 0, "info",
            f"{len(rows)} external location(s), {len(creds)} storage credential(s)",
            "External locations secured with named credentials",
            details={"non_conforming": nc},
            recommendation=Recommendation(
                action="Review external locations and ensure each uses a dedicated storage credential with least-privilege access.",
                impact="Over-broad storage credentials can expose data outside Unity Catalog governance.",
                priority="low",
                docs_url="https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html"))

    # ── Tier 1: Table & Column Documentation ────────────────────────

    def check_6_5_1_table_documentation(self):
        """Tier 1: Only 13% of tables have comments."""
        rows = self.executor.execute("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN comment IS NOT NULL AND TRIM(comment) != '' THEN 1 ELSE 0 END) AS documented
            FROM system.information_schema.tables
            WHERE table_schema != 'information_schema'
              AND table_catalog NOT IN ('system', '__databricks_internal', 'hive_metastore')
              AND table_type IN ('MANAGED', 'EXTERNAL')
        """)
        total = rows[0]["total"] or 0
        documented = rows[0]["documented"] or 0
        rate = documented / max(total, 1) * 100
        if rate >= 50: score, status = 100, "pass"
        elif rate >= 20: score, status = 50, "partial"
        else: score, status = 0, "fail"

        undocumented = self.executor.execute("""
            SELECT table_catalog, table_schema, table_name
            FROM system.information_schema.tables
            WHERE table_schema != 'information_schema'
              AND table_catalog NOT IN ('system', '__databricks_internal', 'hive_metastore')
              AND table_type IN ('MANAGED', 'EXTERNAL')
              AND (comment IS NULL OR TRIM(comment) = '')
            ORDER BY table_catalog, table_schema, table_name
            LIMIT 20
        """)
        nc = [{"table": f"{r['table_catalog']}.{r['table_schema']}.{r['table_name']}"} for r in undocumented]
        if not nc:
            nc = [{"status": f"All {total} tables documented"}]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"Add table descriptions. Only {documented:,}/{total:,} tables ({rate:.0f}%) have comments.",
                impact="Documentation improves data discovery and reduces misuse across teams.",
                priority="medium",
                sql_command="COMMENT ON TABLE catalog.schema.table IS 'Description of the table'",
                docs_url="https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-comment.html")
        return CheckResult("6.5.1", "Table documentation coverage", "Data Documentation",
            score, status, f"{rate:.0f}% ({documented:,}/{total:,} tables)", "≥50% documented",
            details={"undocumented_tables": nc}, recommendation=rec)

    def check_6_5_2_column_documentation(self):
        """Tier 1: Only 1% of columns have comments."""
        rows = self.executor.execute("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN comment IS NOT NULL AND TRIM(comment) != '' THEN 1 ELSE 0 END) AS documented
            FROM system.information_schema.columns
            WHERE table_schema != 'information_schema'
              AND table_catalog NOT IN ('system', '__databricks_internal', 'hive_metastore')
        """)
        total = rows[0]["total"] or 0
        documented = rows[0]["documented"] or 0
        rate = documented / max(total, 1) * 100
        if rate >= 30: score, status = 100, "pass"
        elif rate >= 10: score, status = 50, "partial"
        else: score, status = 0, "fail"

        top_undoc = self.executor.execute("""
            SELECT table_catalog, table_schema, table_name,
                   COUNT(*) AS total_cols,
                   SUM(CASE WHEN comment IS NULL OR TRIM(comment) = '' THEN 1 ELSE 0 END) AS undoc_cols
            FROM system.information_schema.columns
            WHERE table_schema != 'information_schema'
              AND table_catalog NOT IN ('system', '__databricks_internal', 'hive_metastore')
            GROUP BY 1,2,3
            HAVING SUM(CASE WHEN comment IS NULL OR TRIM(comment) = '' THEN 1 ELSE 0 END) > 10
            ORDER BY undoc_cols DESC LIMIT 20
        """)
        nc = [{"table": f"{r['table_catalog']}.{r['table_schema']}.{r['table_name']}",
               "undocumented_columns": r["undoc_cols"], "total_columns": r["total_cols"]}
              for r in top_undoc] if top_undoc else [{"status": f"{documented:,}/{total:,} columns documented"}]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"Add column descriptions. Only {documented:,}/{total:,} columns ({rate:.0f}%) have comments.",
                impact="Column-level docs reduce data misinterpretation and improve data literacy.",
                priority="low",
                sql_command="ALTER TABLE catalog.schema.table ALTER COLUMN col_name COMMENT 'description'")
        return CheckResult("6.5.2", "Column documentation coverage", "Data Documentation",
            score, status, f"{rate:.0f}% ({documented:,}/{total:,} columns)", "≥30% documented",
            details={"tables_with_undocumented_columns": nc}, recommendation=rec)

    # ── Tier 2: Stale Table Detection ────────────────────────────────

    def check_6_5_3_stale_tables(self):
        """Tier 2: Tables not altered in 90+ days."""
        rows = self.executor.execute("""
            SELECT table_catalog, table_schema, table_name, last_altered,
                   DATEDIFF(DAY, last_altered, CURRENT_DATE()) AS days_since_altered
            FROM system.information_schema.tables
            WHERE table_schema != 'information_schema'
              AND table_catalog NOT IN ('system', '__databricks_internal', 'hive_metastore')
              AND table_type IN ('MANAGED', 'EXTERNAL')
              AND last_altered IS NOT NULL
              AND DATEDIFF(DAY, last_altered, CURRENT_DATE()) > 90
            ORDER BY days_since_altered DESC
            LIMIT 30
        """)
        total_r = self.executor.execute("""
            SELECT COUNT(*) AS total FROM system.information_schema.tables
            WHERE table_schema != 'information_schema'
              AND table_catalog NOT IN ('system', '__databricks_internal', 'hive_metastore')
              AND table_type IN ('MANAGED', 'EXTERNAL')
              AND last_altered IS NOT NULL
        """)
        total = total_r[0]["total"] or 0
        stale_count = len(rows)
        # We need total stale, not just top 30
        stale_total_r = self.executor.execute("""
            SELECT COUNT(*) AS cnt FROM system.information_schema.tables
            WHERE table_schema != 'information_schema'
              AND table_catalog NOT IN ('system', '__databricks_internal', 'hive_metastore')
              AND table_type IN ('MANAGED', 'EXTERNAL')
              AND last_altered IS NOT NULL
              AND DATEDIFF(DAY, last_altered, CURRENT_DATE()) > 90
        """)
        stale_total = stale_total_r[0]["cnt"] or 0
        rate = stale_total / max(total, 1) * 100
        if rate <= 20: score, status = 100, "pass"
        elif rate <= 40: score, status = 50, "partial"
        else: score, status = 0, "fail"

        nc = [{"table": f"{r['table_catalog']}.{r['table_schema']}.{r['table_name']}",
               "days_stale": r["days_since_altered"],
               "last_altered": str(r["last_altered"])[:10]}
              for r in rows] if rows else [{"status": "No stale tables"}]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"{stale_total:,} tables ({rate:.0f}%) haven't been altered in 90+ days. Review for deprecation or archival.",
                impact="Stale tables waste storage and confuse data consumers.",
                priority="low")
        return CheckResult("6.5.3", "Stale tables (>90 days unaltered)", "Data Documentation",
            score, status, f"{stale_total:,}/{total:,} tables ({rate:.0f}%)", "≤20% stale",
            details={"stale_tables": nc}, recommendation=rec)

    # ── Tier 2: Table Format Distribution ────────────────────────────

    def check_6_5_4_table_format_distribution(self):
        """Tier 2: Delta vs other table formats."""
        rows = self.executor.execute("""
            SELECT COALESCE(data_source_format, 'UNKNOWN') AS format,
                   COUNT(*) AS table_count
            FROM system.information_schema.tables
            WHERE table_schema != 'information_schema'
              AND table_catalog NOT IN ('system', '__databricks_internal', 'hive_metastore')
              AND table_type IN ('MANAGED', 'EXTERNAL')
            GROUP BY 1 ORDER BY table_count DESC
        """)
        total = sum(r["table_count"] for r in rows)
        delta_row = next((r for r in rows if r["format"] in ('DELTA', 'delta')), None)
        delta_count = delta_row["table_count"] if delta_row else 0
        delta_pct = delta_count / max(total, 1) * 100
        if delta_pct >= 90: score, status = 100, "pass"
        elif delta_pct >= 70: score, status = 50, "partial"
        else: score, status = 0, "fail"

        nc = [{"format": r["format"], "tables": r["table_count"],
               "percentage": f"{r['table_count']/max(total,1)*100:.1f}%"} for r in rows]
        rec = None
        if score < 100:
            non_delta = total - delta_count
            rec = Recommendation(
                action=f"Migrate {non_delta:,} non-Delta tables to Delta format for ACID transactions, time travel, and better performance.",
                impact="Delta format provides reliability, performance, and governance features.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/delta/index.html")
        return CheckResult("6.5.4", "Delta format adoption", "Data Documentation",
            score, status, f"{delta_pct:.0f}% Delta ({delta_count:,}/{total:,})", "≥90% Delta",
            details={"format_distribution": nc}, recommendation=rec)

    # ── 6.6 Migration & Workspace Governance ──────────────────────────

    def check_6_6_1_uc_migration_progress(self) -> CheckResult:
        """Measure Unity Catalog migration progress — what % of tables are in UC vs legacy hive_metastore."""
        try:
            rows = self.executor.execute("""
                SELECT 
                    COUNT(*) AS total_tables,
                    COUNT(CASE WHEN table_catalog != 'hive_metastore' AND table_catalog != 'system' THEN 1 END) AS uc_tables,
                    COUNT(CASE WHEN table_catalog = 'hive_metastore' THEN 1 END) AS hms_tables,
                    COUNT(CASE WHEN table_type = 'MANAGED' AND table_catalog != 'hive_metastore' AND table_catalog != 'system' THEN 1 END) AS uc_managed,
                    COUNT(CASE WHEN table_type = 'EXTERNAL' AND table_catalog != 'hive_metastore' AND table_catalog != 'system' THEN 1 END) AS uc_external,
                    COUNT(DISTINCT CASE WHEN table_catalog != 'hive_metastore' AND table_catalog != 'system' THEN table_catalog END) AS uc_catalogs,
                    COUNT(DISTINCT CASE WHEN table_catalog != 'hive_metastore' AND table_catalog != 'system' THEN CONCAT(table_catalog, '.', table_schema) END) AS uc_schemas
                FROM system.information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'default')
            """)
        except Exception as e:
            return CheckResult("6.6.1", "Unity Catalog migration progress", "Migration & Workspace Governance",
                0, "not_evaluated", f"Could not query: {str(e)[:80]}", "N/A")

        if not rows:
            return CheckResult("6.6.1", "Unity Catalog migration progress", "Migration & Workspace Governance",
                None, "info", "No table metadata available", "100% of tables in Unity Catalog")

        r = rows[0]
        total = int(r.get("total_tables", 0) or 0)
        uc = int(r.get("uc_tables", 0) or 0)
        hms = int(r.get("hms_tables", 0) or 0)
        uc_managed = int(r.get("uc_managed", 0) or 0)
        uc_external = int(r.get("uc_external", 0) or 0)
        catalogs = int(r.get("uc_catalogs", 0) or 0)
        schemas = int(r.get("uc_schemas", 0) or 0)
        uc_pct = uc / total * 100 if total > 0 else 100

        score = min(100, int(uc_pct))
        status = "pass" if hms == 0 else "partial" if uc_pct > 50 else "fail"

        nc = [{"metric": "Total tables", "value": f"{total:,}"},
              {"metric": "Unity Catalog tables", "value": f"{uc:,} ({uc_pct:.1f}%)"},
              {"metric": "hive_metastore (legacy)", "value": f"{hms:,}"},
              {"metric": "UC managed tables", "value": f"{uc_managed:,}"},
              {"metric": "UC external tables", "value": f"{uc_external:,}"},
              {"metric": "UC catalogs", "value": f"{catalogs:,}"},
              {"metric": "UC schemas", "value": f"{schemas:,}"}]

        rec = Recommendation(
            action=f"{uc_pct:.0f}% of tables are in Unity Catalog ({uc:,}/{total:,}). "
                   + (f"{hms:,} tables remain in hive_metastore — plan migration using UCX toolkit." if hms > 0
                      else "All tables are in Unity Catalog — migration complete."),
            impact="Unity Catalog provides centralized governance, fine-grained access control, lineage tracking, and data classification. "
                   "Legacy hive_metastore tables lack these protections.",
            priority="high" if hms > 50 else "medium" if hms > 0 else "low",
            docs_url="https://docs.databricks.com/en/data-governance/unity-catalog/index.html")

        return CheckResult("6.6.1", "Unity Catalog migration progress", "Migration & Workspace Governance",
            score, status,
            f"{uc_pct:.0f}% in Unity Catalog ({uc:,} UC, {hms:,} legacy) across {catalogs} catalogs",
            "100% of tables in Unity Catalog",
            details={"non_conforming": nc},
            recommendation=rec)

    def check_6_6_2_cross_workspace_governance(self) -> CheckResult:
        """Assess cross-workspace governance — workspace count, naming, and activity patterns."""
        try:
            rows = self.executor.execute("""
                SELECT workspace_id, workspace_name, workspace_url,
                       create_time, status
                FROM system.access.workspaces_latest
                ORDER BY create_time DESC
            """)
        except Exception as e:
            return CheckResult("6.6.2", "Cross-workspace governance", "Migration & Workspace Governance",
                0, "not_evaluated", f"Could not query: {str(e)[:80]}", "N/A")

        if not rows:
            return CheckResult("6.6.2", "Cross-workspace governance", "Migration & Workspace Governance",
                None, "info", "No workspace data available", "Maintain governed workspace topology")

        total_ws = len(rows)
        active_ws = sum(1 for r in rows if r.get("status", "").upper() == "RUNNING")
        inactive_ws = total_ws - active_ws

        # Check naming conventions (good governance = consistent naming patterns)
        names = [r.get("workspace_name", "") or "" for r in rows]
        has_env_tag = sum(1 for n in names if any(tag in n.lower() for tag in ["prod", "dev", "staging", "test", "sandbox"]))
        naming_pct = has_env_tag / total_ws * 100 if total_ws > 0 else 0

        # Score: fewer is better for governance, naming conventions help
        score = 90 if total_ws <= 10 else 75 if total_ws <= 50 else 60 if naming_pct > 50 else 45
        status = "pass" if score >= 80 else "partial" if score >= 50 else "fail"

        nc = [{"workspace_name": (r.get("workspace_name", "N/A") or "unnamed")[:60],
               "workspace_id": r.get("workspace_id", "N/A"),
               "status": r.get("status", "N/A"),
               "created": str(r.get("create_time", "N/A"))[:10]}
              for r in rows[:20]]

        rec = Recommendation(
            action=f"{total_ws} workspaces ({active_ws} active, {inactive_ws} inactive). "
                   f"{has_env_tag}/{total_ws} follow environment naming conventions. "
                   f"Establish workspace provisioning policies, decommission unused workspaces, "
                   f"and enforce consistent naming (e.g., team-env-purpose).",
            impact="Workspace sprawl increases attack surface, complicates governance, and makes cost attribution difficult. "
                   "Consolidation reduces overhead and improves security posture.",
            priority="high" if total_ws > 100 else "medium" if total_ws > 20 else "low",
            docs_url="https://docs.databricks.com/en/admin/account-settings-e2/workspaces.html")

        return CheckResult("6.6.2", "Cross-workspace governance", "Migration & Workspace Governance",
            score, status,
            f"{total_ws} workspaces ({active_ws} active) — {naming_pct:.0f}% follow naming conventions",
            "Consistent naming + <20 active workspaces",
            details={"non_conforming": nc, "summary": f"{total_ws} total, {has_env_tag} with env tags in name"},
            recommendation=rec)

