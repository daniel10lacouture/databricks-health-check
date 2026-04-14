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
                "Volume & Storage Governance"]

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
