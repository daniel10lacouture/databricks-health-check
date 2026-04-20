"""
Section 13: Platform Adoption — advisory checks that detect feature adoption gaps.
These checks do NOT affect the overall health score. Instead, each computes a 
projected score boost showing what the customer's score WOULD be if they adopted.
The Score Booster on the dashboard uses this to show "current → potential" with
actionable opportunities sorted by point impact.

Each check follows the pattern:
  1. Detect low/no adoption of a high-value feature
  2. Score as opportunity (0 = not adopted, 50 = partially, 100 = fully adopted)
  3. Include peer-tier benchmark (directional, no hard numbers)
  4. Include projected_score_boost in details
  5. Recommend with getting-started doc link
"""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


# Peer-tier thresholds: (metric, min_value, tier_label)
# We pick the tier dynamically based on the customer's own metrics
PEER_TIERS = {
    "users": [(5000, "enterprise-scale"), (1000, "large"), (100, "mid-size")],
    "workspaces": [(500, "enterprise-scale"), (100, "large"), (10, "mid-size")],
    "jobs": [(10000, "high-volume"), (1000, "active"), (100, "growing")],
}


def get_peer_tier(metric_name, value):
    """Return a human-readable tier label for the customer's scale."""
    for threshold, label in PEER_TIERS.get(metric_name, []):
        if value >= threshold:
            return label
    return "growing"


class AdoptionCheckRunner(BaseCheckRunner):
    section_id = "adoption"
    section_name = "Platform Adoption"
    section_type = "advisory"
    icon = "rocket"

    def get_subsections(self):
        return [
            "AI & BI Activation",
            "Compute Modernization",
            "Governance & Security Maturity",
            "Operational Excellence",
            "Data Collaboration",
            "AI & ML Expansion",
        ]

    def _get_account_profile(self):
        """Gather account-scale metrics for peer-tier benchmarking."""
        if hasattr(self, "_profile"):
            return self._profile
        profile = {"users": 0, "workspaces": 0, "jobs": 0}
        try:
            rows = self.executor.execute("""
                SELECT COUNT(DISTINCT executed_by) AS users
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())""")
            profile["users"] = rows[0]["users"] if rows else 0
        except Exception:
            pass
        try:
            rows = self.executor.execute("""
                SELECT COUNT(*) AS cnt FROM system.access.workspaces_latest""")
            profile["workspaces"] = rows[0]["cnt"] if rows else 0
        except Exception:
            pass
        try:
            rows = self.executor.execute("""
                SELECT COUNT(DISTINCT job_id) AS cnt
                FROM system.lakeflow.job_run_timeline
                WHERE period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())""")
            profile["jobs"] = rows[0]["cnt"] if rows else 0
        except Exception:
            pass
        self._profile = profile
        return profile

    def _peer_benchmark(self, metric_name, feature_description):
        """Generate a directional peer-tier benchmark string."""
        profile = self._get_account_profile()
        value = profile.get(metric_name, 0)
        tier = get_peer_tier(metric_name, value)
        return f"Accounts with {tier} usage profiles like yours typically have {feature_description}"

    # ── AI & BI Activation ──────────────────────────────────────────

    def check_13_1_1_aibi_dashboard_adoption(self) -> CheckResult:
        """Check if AI/BI Dashboards are being used vs external BI tools."""
        try:
            rows = self.executor.execute("""
                SELECT
                    COUNT(*) AS total_queries,
                    COUNT(CASE WHEN client_application = 'Databricks SQL Dashboard'
                               OR client_application = 'Databricks SQL Genie Space'
                               OR client_application LIKE '%lakeview%' THEN 1 END) AS native_dash,
                    COUNT(CASE WHEN client_application LIKE '%tableau%'
                               OR client_application LIKE '%power%bi%'
                               OR client_application LIKE '%looker%'
                               OR client_application LIKE '%Databricks Connector%' THEN 1 END) AS external_bi
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND statement_type = 'SELECT'""")
        except Exception:
            return CheckResult("13.1.1", "AI/BI Dashboard Adoption",
                "AI & BI Activation", 0, "info",
                "Could not query dashboard usage", "Active AI/BI Dashboard usage")

        r = rows[0] if rows else {}
        native = r.get("native_dash", 0) or 0
        external = r.get("external_bi", 0) or 0
        total = r.get("total_queries", 1) or 1

        if native > 0 and native >= external:
            score, status = 100, "pass"
        elif native > 0:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        benchmark = self._peer_benchmark("users", "higher AI/BI Dashboard adoption for self-serve analytics")

        rec = None
        if native == 0:
            rec = Recommendation(
                action="Set up AI/BI Dashboards to replace external BI tool dependencies. Start by converting your most-viewed Tableau/Power BI reports.",
                impact="Eliminates per-seat BI licensing costs and provides governed, AI-powered analytics directly on your lakehouse data.",
                priority="high",
                docs_url="https://docs.databricks.com/en/dashboards/index.html")
        elif native < external:
            rec = Recommendation(
                action=f"Expand AI/BI Dashboard usage. Currently {native} native vs {external} external BI queries. Migrate top external reports.",
                impact="Reduces data movement and licensing costs while improving data freshness.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/dashboards/index.html")

        return CheckResult("13.1.1", "AI/BI Dashboard Adoption",
            "AI & BI Activation", score, status,
            f"{native} native dashboard queries vs {external} external BI queries (30d)",
            "Majority of analytics served through AI/BI Dashboards",
            details={"native_queries": native, "external_bi_queries": external,
                     "total_queries": total, "peer_benchmark": benchmark,
                     "projected_score_boost": 3 if score < 100 else 0},
            recommendation=rec)

    def check_13_1_2_genie_spaces(self) -> CheckResult:
        """Check for Genie space / AI assistant adoption."""
        try:
            rows = self.executor.execute("""
                SELECT COUNT(DISTINCT initiated_by) AS users, COUNT(*) AS events
                FROM system.access.assistant_events
                WHERE event_time >= DATEADD(DAY, -30, CURRENT_DATE())""")
        except Exception:
            return CheckResult("13.1.2", "Databricks Assistant & Genie Adoption",
                "AI & BI Activation", 0, "info",
                "Could not query assistant events", "Active assistant/Genie usage")

        r = rows[0] if rows else {}
        users = r.get("users", 0) or 0
        events = r.get("events", 0) or 0

        profile = self._get_account_profile()
        total_users = profile.get("users", 1) or 1
        adoption_pct = round(users / total_users * 100, 1)

        if adoption_pct >= 50:
            score, status = 100, "pass"
        elif adoption_pct >= 20:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        benchmark = self._peer_benchmark("users", "broader Databricks Assistant and Genie adoption across their user base")

        rec = None
        if adoption_pct < 50:
            rec = Recommendation(
                action=f"Enable Databricks Assistant for all users. Currently {users}/{total_users} users ({adoption_pct}%) are active. Set up Genie spaces for business user self-serve.",
                impact="Accelerates productivity across data teams and enables non-technical users to query data via natural language.",
                priority="medium" if adoption_pct >= 20 else "high",
                docs_url="https://docs.databricks.com/en/genie/index.html")

        return CheckResult("13.1.2", "Databricks Assistant & Genie Adoption",
            "AI & BI Activation", score, status,
            f"{users}/{total_users} users ({adoption_pct}%) using Assistant/Genie (30d)",
            "50%+ of active users engaging with Assistant or Genie",
            details={"active_assistant_users": users, "total_users": total_users,
                     "adoption_pct": adoption_pct, "peer_benchmark": benchmark,
                     "projected_score_boost": 2 if score < 100 else 0},
            recommendation=rec)

    def check_13_1_3_consumer_role_users(self) -> CheckResult:
        """Check for consumer-role users accessing dashboards/Genie (indicates AI/BI sharing)."""
        try:
            rows = self.executor.execute("""
                SELECT COUNT(DISTINCT executed_by) AS dashboard_users
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND (client_application LIKE '%lakeview%'
                       OR client_application LIKE '%dashboard%'
                       OR client_application LIKE '%sqlAgent%')""")
        except Exception:
            return CheckResult("13.1.3", "Consumer User Activation",
                "AI & BI Activation", 0, "info",
                "Could not query consumer usage", "Active consumer-role users on dashboards")

        r = rows[0] if rows else {}
        dash_users = r.get("dashboard_users", 0) or 0
        profile = self._get_account_profile()
        total = profile.get("users", 1) or 1

        if dash_users >= 10:
            score, status = 100, "pass"
        elif dash_users > 0:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        benchmark = self._peer_benchmark("users", "dedicated consumer-role users accessing AI/BI Dashboards and Genie spaces")

        rec = None
        if dash_users < 10:
            rec = Recommendation(
                action="Set up consumer-role access for business stakeholders. Create shared AI/BI Dashboards and Genie spaces with appropriate permissions.",
                impact="Extends data access to decision-makers without requiring SQL skills. Drives self-serve analytics adoption.",
                priority="high" if dash_users == 0 else "medium",
                docs_url="https://docs.databricks.com/en/dashboards/tutorials/share-dashboard.html")

        return CheckResult("13.1.3", "Consumer User Activation",
            "AI & BI Activation", score, status,
            f"{dash_users} users accessing dashboards/Genie (30d)",
            "10+ consumer-role users actively using AI/BI Dashboards",
            details={"dashboard_consumers": dash_users, "total_users": total,
                     "peer_benchmark": benchmark,
                     "projected_score_boost": 2 if score < 100 else 0},
            recommendation=rec)

    # ── Compute Modernization ───────────────────────────────────────

    def check_13_2_1_serverless_warehouse_adoption(self) -> CheckResult:
        """Check serverless vs classic warehouse ratio."""
        try:
            rows = self.executor.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(CASE WHEN warehouse_type = 'PRO' THEN 1 END) AS serverless,
                    COUNT(CASE WHEN warehouse_type != 'PRO' THEN 1 END) AS classic
                FROM system.compute.warehouses
                WHERE delete_time IS NULL""")
        except Exception:
            return CheckResult("13.2.1", "Serverless Warehouse Adoption",
                "Compute Modernization", 0, "info",
                "Could not query warehouses", "Serverless-first warehouse strategy")

        r = rows[0] if rows else {}
        total = r.get("total", 0) or 1
        serverless = r.get("serverless", 0) or 0
        classic = r.get("classic", 0) or 0
        pct = round(serverless / total * 100, 1)

        if pct >= 50:
            score, status = 100, "pass"
        elif pct >= 10:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        benchmark = self._peer_benchmark("workspaces", "a higher serverless warehouse adoption rate")

        rec = None
        if pct < 50:
            rec = Recommendation(
                action=f"Migrate classic warehouses to serverless. Currently {serverless}/{total} ({pct}%) are serverless. Start with dev/test warehouses, then production.",
                impact="Serverless warehouses eliminate idle costs, start instantly, and scale automatically. Typical savings of 30-50% on warehouse spend.",
                priority="high",
                docs_url="https://docs.databricks.com/en/compute/sql-warehouse/serverless.html")

        return CheckResult("13.2.1", "Serverless Warehouse Adoption",
            "Compute Modernization", score, status,
            f"{serverless}/{total} warehouses are serverless ({pct}%)",
            "50%+ warehouses running serverless",
            details={"serverless": serverless, "classic": classic, "total": total,
                     "serverless_pct": pct, "peer_benchmark": benchmark,
                     "projected_score_boost": 4 if score < 100 else 0},
            recommendation=rec)

    def check_13_2_2_photon_adoption(self) -> CheckResult:
        """Check Photon-accelerated vs non-Photon compute usage."""
        try:
            rows = self.executor.execute("""
                SELECT
                    SUM(usage_quantity) AS total_dbus,
                    SUM(CASE WHEN sku_name LIKE '%PHOTON%' THEN usage_quantity ELSE 0 END) AS photon_dbus
                FROM system.billing.usage
                WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND usage_unit = 'DBU'""")
        except Exception:
            return CheckResult("13.2.2", "Photon Acceleration Adoption",
                "Compute Modernization", 0, "info",
                "Could not query billing usage", "Photon enabled across workloads")

        r = rows[0] if rows else {}
        total = r.get("total_dbus", 0) or 1
        photon = r.get("photon_dbus", 0) or 0
        pct = round(photon / total * 100, 1)

        if pct >= 50:
            score, status = 100, "pass"
        elif pct >= 10:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        benchmark = self._peer_benchmark("jobs", "a higher share of Photon-accelerated workloads")

        rec = None
        if pct < 50:
            rec = Recommendation(
                action=f"Enable Photon runtime across more workloads. Currently {pct}% of DBUs are Photon-accelerated.",
                impact="Photon delivers 2-8x faster performance for SQL and Spark workloads with zero code changes. Faster queries complete sooner, reducing cost per query.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/compute/photon.html")

        return CheckResult("13.2.2", "Photon Acceleration Adoption",
            "Compute Modernization", score, status,
            f"{pct}% of DBUs running on Photon (30d)",
            "50%+ of DBU consumption using Photon runtime",
            details={"photon_dbus": photon, "total_dbus": total, "photon_pct": pct,
                     "peer_benchmark": benchmark,
                     "projected_score_boost": 3 if score < 100 else 0},
            recommendation=rec)

    def check_13_2_3_jobs_on_allpurpose(self) -> CheckResult:
        """Check for scheduled jobs running on all-purpose (interactive) clusters."""
        try:
            rows = self.executor.execute("""
                SELECT
                    SUM(usage_quantity) AS total_job_dbus,
                    SUM(CASE WHEN sku_name LIKE '%ALL_PURPOSE%' OR sku_name LIKE '%ALL PURPOSE%'
                             THEN usage_quantity ELSE 0 END) AS allpurpose_dbus
                FROM system.billing.usage
                WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND usage_unit = 'DBU'
                  AND usage_metadata.job_id IS NOT NULL""")
        except Exception:
            return CheckResult("13.2.3", "Jobs on Dedicated Compute",
                "Compute Modernization", 0, "info",
                "Could not query job compute usage", "All jobs running on jobs compute")

        r = rows[0] if rows else {}
        total = r.get("total_job_dbus", 0) or 1
        allpurpose = r.get("allpurpose_dbus", 0) or 0
        waste_pct = round(allpurpose / total * 100, 1) if total > 0 else 0

        if waste_pct <= 5:
            score, status = 100, "pass"
        elif waste_pct <= 20:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        benchmark = self._peer_benchmark("jobs", "minimal all-purpose compute usage for scheduled jobs")

        rec = None
        if waste_pct > 5:
            rec = Recommendation(
                action=f"Migrate scheduled jobs off all-purpose clusters. {waste_pct}% of job DBUs are running on expensive interactive compute.",
                impact="Jobs compute is 2-4x cheaper per DBU than all-purpose compute. Migrating yields immediate cost savings.",
                priority="high" if waste_pct > 20 else "medium",
                docs_url="https://docs.databricks.com/en/compute/configure.html")

        return CheckResult("13.2.3", "Jobs on Dedicated Compute",
            "Compute Modernization", score, status,
            f"{waste_pct}% of job DBUs running on all-purpose compute (30d)",
            "Less than 5% of job DBUs on all-purpose clusters",
            details={"allpurpose_job_dbus": allpurpose, "total_job_dbus": total,
                     "waste_pct": waste_pct, "peer_benchmark": benchmark,
                     "projected_score_boost": 3 if score < 100 else 0},
            recommendation=rec)

    # ── Governance & Security Maturity ──────────────────────────────

    def check_13_3_1_identity_management(self) -> CheckResult:
        """Check for SSO adoption and identity management maturity."""
        try:
            rows = self.executor.execute("""
                SELECT
                    COUNT(CASE WHEN action_name = 'samlLogin' THEN 1 END) AS sso_logins,
                    COUNT(DISTINCT CASE WHEN action_name = 'samlLogin' THEN user_identity.email END) AS sso_users,
                    COUNT(CASE WHEN action_name IN ('add', 'updateUser', 'deactivateUser', 'activateUser') THEN 1 END) AS user_mgmt_events,
                    COUNT(CASE WHEN action_name IN ('createGroup', 'addPrincipalToGroup', 'addPrincipalsToGroup', 'removePrincipalFromGroup') THEN 1 END) AS group_mgmt_events
                FROM system.access.audit
                WHERE event_time >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND service_name = 'accounts'""")
        except Exception:
            return CheckResult("13.3.1", "Identity Management Maturity",
                "Governance & Security Maturity", 0, "info",
                "Could not query audit logs for identity events", "SSO + automated provisioning")

        r = rows[0] if rows else {}
        sso_logins = int(r.get("sso_logins", 0) or 0)
        sso_users = int(r.get("sso_users", 0) or 0)
        user_mgmt = int(r.get("user_mgmt_events", 0) or 0)
        group_mgmt = int(r.get("group_mgmt_events", 0) or 0)
        has_sso = sso_logins > 0
        has_automated_provisioning = user_mgmt > 100
        has_group_mgmt = group_mgmt > 0

        if has_sso and has_automated_provisioning and has_group_mgmt:
            score, status = 100, "pass"
        elif has_sso and (has_automated_provisioning or has_group_mgmt):
            score, status = 75, "partial"
        elif has_sso:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        rec = None
        if score < 100:
            actions = []
            if not has_sso: actions.append("Enable SSO for all users")
            if not has_automated_provisioning: actions.append("Enable Automatic Identity Management for automated user/group sync")
            if not has_group_mgmt: actions.append("Set up group-based access management from your IdP")
            rec = Recommendation(
                action=". ".join(actions) + ".",
                impact="SSO with automated provisioning ensures consistent identity management and improves security.",
                priority="high" if not has_sso else "medium",
                docs_url="https://docs.databricks.com/en/admin/users-groups/best-practices.html")

        return CheckResult("13.3.1", "Identity Management Maturity",
            "Governance & Security Maturity", score, status,
            f"SSO active: {sso_logins:,} logins from {sso_users} users, {user_mgmt:,} provisioning events, {group_mgmt:,} group events (30d)",
            "SSO + automated identity provisioning",
            details={"sso_logins": sso_logins, "sso_users": sso_users,
                     "user_mgmt_events": user_mgmt, "group_mgmt_events": group_mgmt},
            recommendation=rec)


    def check_13_3_2_service_principal_usage(self) -> CheckResult:
        """Check if production workloads use service principals vs personal credentials."""
        try:
            rows = self.executor.execute("""
                SELECT
                    COUNT(DISTINCT job_id) AS total_jobs,
                    COUNT(DISTINCT CASE WHEN run_as LIKE '%@%' THEN job_id END) AS user_jobs,
                    COUNT(DISTINCT CASE WHEN run_as NOT LIKE '%@%' AND run_as IS NOT NULL THEN job_id END) AS sp_jobs
                FROM system.lakeflow.jobs
                WHERE change_time >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND delete_time IS NULL""")
        except Exception:
            return CheckResult("13.3.2", "Service Principal Usage",
                "Governance & Security Maturity", 0, "info",
                "Could not query job ownership", "Production jobs using service principals")

        r = rows[0] if rows else {}
        total = r.get("total_jobs", 0) or 1
        sp_jobs = r.get("sp_jobs", 0) or 0
        user_jobs = r.get("user_jobs", 0) or 0
        sp_pct = round(sp_jobs / total * 100, 1)

        if sp_pct >= 50:
            score, status = 100, "pass"
        elif sp_pct >= 10:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        benchmark = self._peer_benchmark("jobs", "production workloads running under service principals rather than personal credentials")

        rec = None
        if sp_pct < 50:
            rec = Recommendation(
                action=f"Migrate production jobs to service principals. Currently {sp_jobs}/{total} jobs ({sp_pct}%) use service principals.",
                impact="Service principals decouple job execution from individual users, preventing failures when employees leave and improving audit trails.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/admin/users-groups/service-principals.html")

        return CheckResult("13.3.2", "Service Principal Usage",
            "Governance & Security Maturity", score, status,
            f"{sp_jobs}/{total} jobs ({sp_pct}%) using service principals",
            "50%+ production jobs running under service principals",
            details={"sp_jobs": sp_jobs, "user_jobs": user_jobs, "sp_pct": sp_pct,
                     "peer_benchmark": benchmark,
                     "projected_score_boost": 2 if score < 100 else 0},
            recommendation=rec)

    def check_13_3_3_cost_tagging(self) -> CheckResult:
        """Check if compute resources have custom cost-attribution tags."""
        try:
            rows = self.executor.execute("""
                SELECT
                    COUNT(*) AS total_records,
                    COUNT(CASE WHEN usage_metadata.cluster_id IS NOT NULL
                               AND custom_tags IS NOT NULL
                               AND size(custom_tags) > 0 THEN 1 END) AS tagged
                FROM system.billing.usage
                WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND usage_unit = 'DBU'
                  AND usage_metadata.cluster_id IS NOT NULL""")
        except Exception:
            return CheckResult("13.3.3", "Cost Attribution Tagging",
                "Operational Excellence", 0, "info",
                "Could not query billing tags", "All resources tagged for cost attribution")

        r = rows[0] if rows else {}
        total = r.get("total_records", 0) or 1
        tagged = r.get("tagged", 0) or 0
        pct = round(tagged / total * 100, 1)

        if pct >= 80:
            score, status = 100, "pass"
        elif pct >= 30:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        benchmark = self._peer_benchmark("workspaces", "comprehensive cost-attribution tagging for chargeback and budget management")

        rec = None
        if pct < 80:
            rec = Recommendation(
                action=f"Add custom tags to compute resources for cost attribution. Only {pct}% of billed usage has cost tags.",
                impact="Enables team/project chargeback, identifies cost drivers, and makes cloud spend visible to stakeholders.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/admin/account-settings/usage-detail-tags.html")

        return CheckResult("13.3.3", "Cost Attribution Tagging",
            "Operational Excellence", score, status,
            f"{pct}% of billed compute usage has cost-attribution tags (30d)",
            "80%+ of compute usage tagged for chargeback",
            details={"tagged_records": tagged, "total_records": total, "tag_pct": pct,
                     "peer_benchmark": benchmark,
                     "projected_score_boost": 2 if score < 100 else 0},
            recommendation=rec)

    # ── Operational Excellence ──────────────────────────────────────

    def check_13_4_1_predictive_optimization_coverage(self) -> CheckResult:
        """Check if Predictive Optimization covers eligible tables."""
        try:
            rows = self.executor.execute("""
                WITH eligible AS (
                    SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name) AS fqn
                    FROM system.information_schema.tables
                    WHERE table_type = 'MANAGED' AND data_source_format = 'DELTA'
                      AND table_schema != 'information_schema'
                ),
                optimized AS (
                    SELECT DISTINCT CONCAT(catalog_name, '.', schema_name, '.', table_name) AS fqn
                    FROM system.storage.predictive_optimization_operations_history
                    WHERE start_time >= DATEADD(DAY, -90, CURRENT_DATE())
                )
                SELECT COUNT(DISTINCT e.fqn) AS eligible,
                       COUNT(DISTINCT o.fqn) AS optimized
                FROM eligible e
                LEFT JOIN optimized o ON e.fqn = o.fqn""")
        except Exception:
            return CheckResult("13.4.1", "Predictive Optimization Coverage",
                "Optimization & Performance", 0, "not_evaluated",
                "Could not query Predictive Optimization", "50%+ tables enrolled")

        eligible = int(rows[0].get("eligible", 0)) if rows else 0
        optimized = int(rows[0].get("optimized", 0)) if rows else 0
        rate = (optimized / eligible * 100) if eligible > 0 else 0

        if rate >= 50: score, status = 100, "pass"
        elif rate >= 20: score, status = 50, "partial"
        else: score, status = 0, "fail"

        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"Enable Predictive Optimization for more tables. Currently {optimized}/{eligible} eligible managed Delta tables ({rate:.1f}%) are optimized.",
                impact="Predictive Optimization automatically compacts and z-orders tables, improving query performance and reducing storage costs.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/optimizations/predictive-optimization.html")

        return CheckResult("13.4.1", "Predictive Optimization Coverage",
            "Optimization & Performance", score, status,
            f"{optimized}/{eligible} eligible tables ({rate:.1f}%) with Predictive Optimization (90d)",
            "50%+ of managed Delta tables enrolled in Predictive Optimization",
            details={"non_conforming": [{"eligible": eligible, "optimized": optimized, "rate": f"{rate:.1f}%"}]},
            recommendation=rec)


    def check_13_4_2_runtime_currency(self) -> CheckResult:
        """Check if clusters are running recent Databricks Runtime versions."""
        try:
            rows = self.executor.execute("""
                SELECT dbr_version, cluster_name, cluster_id
                FROM system.compute.clusters
                WHERE delete_time IS NULL
                  AND cluster_source IN ('UI', 'API')""")
        except Exception:
            return CheckResult("13.4.2", "Runtime Version Currency",
                "Operational Excellence", 0, "info",
                "Could not query cluster versions", "All clusters on recent LTS runtime")

        total = len(rows) or 1
        # Consider anything older than DBR 14.x as outdated
        outdated = []
        for r in rows:
            sv = r.get("dbr_version", "") or ""
            # Extract major version number
            try:
                major = int(sv.split(".")[0])
                if major < 14:
                    outdated.append(r)
            except (ValueError, IndexError):
                if "custom" not in sv.lower():
                    outdated.append(r)

        pct_current = round((total - len(outdated)) / total * 100, 1)
        if pct_current >= 80:
            score, status = 100, "pass"
        elif pct_current >= 50:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        benchmark = self._peer_benchmark("workspaces", "clusters running on current LTS Databricks Runtime versions")

        nc = [{"cluster_name": r.get("cluster_name", ""), "dbr_version": r.get("dbr_version", ""),
               "action": "Upgrade to latest LTS runtime"} for r in outdated[:15]]

        rec = None
        if outdated:
            rec = Recommendation(
                action=f"Upgrade {len(outdated)} cluster(s) from outdated runtime versions to the latest LTS (DBR 15.4+).",
                impact="Newer runtimes include performance improvements, security patches, and new features like Liquid Clustering and AI functions.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/release-notes/runtime/index.html")

        return CheckResult("13.4.2", "Runtime Version Currency",
            "Operational Excellence", score, status,
            f"{len(outdated)}/{total} clusters on outdated runtime versions",
            "All clusters on DBR 14.x+ LTS",
            details={"outdated_clusters": len(outdated), "total_clusters": total,
                     "pct_current": pct_current, "non_conforming": nc,
                     "peer_benchmark": benchmark,
                     "projected_score_boost": 1 if score < 100 else 0},
            recommendation=rec)

    # ── AI & ML Expansion ───────────────────────────────────────────

    def check_13_5_1_ai_gateway_adoption(self) -> CheckResult:
        """Check AI Gateway usage relative to total user base."""
        try:
            rows = self.executor.execute("""
                SELECT COUNT(DISTINCT requester) AS ai_users
                FROM system.ai_gateway.usage
                WHERE event_time >= DATEADD(DAY, -30, CURRENT_DATE())""")
        except Exception:
            return CheckResult("13.5.1", "AI Gateway Adoption",
                "AI & ML Expansion", 0, "info",
                "Could not query AI Gateway usage", "Broad AI Gateway adoption")

        r = rows[0] if rows else {}
        ai_users = r.get("ai_users", 0) or 0
        profile = self._get_account_profile()
        total = profile.get("users", 1) or 1
        pct = round(ai_users / total * 100, 1)

        if pct >= 20:
            score, status = 100, "pass"
        elif pct >= 5:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        benchmark = self._peer_benchmark("users", "broader AI Gateway adoption for governed GenAI access")

        rec = None
        if pct < 20:
            rec = Recommendation(
                action=f"Expand AI Gateway access. Only {ai_users}/{total} users ({pct}%) are using it. Configure endpoints for common LLM use cases.",
                impact="AI Gateway provides governed, auditable access to foundation models with rate limiting and cost controls built in.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/ai-gateway/index.html")

        return CheckResult("13.5.1", "AI Gateway Adoption",
            "AI & ML Expansion", score, status,
            f"{ai_users}/{total} users ({pct}%) using AI Gateway (30d)",
            "20%+ of active users accessing AI Gateway",
            details={"ai_gateway_users": ai_users, "total_users": total, "adoption_pct": pct,
                     "peer_benchmark": benchmark,
                     "projected_score_boost": 2 if score < 100 else 0},
            recommendation=rec)

    def check_13_5_2_ai_functions_sql(self) -> CheckResult:
        """Check adoption of SQL AI functions (ai_query, ai_generate, etc.)."""
        try:
            rows = self.executor.execute("""
                SELECT
                    COUNT(*) AS total_queries,
                    COUNT(CASE WHEN LOWER(statement_text) LIKE '%ai_query(%'
                               OR LOWER(statement_text) LIKE '%ai_generate(%'
                               OR LOWER(statement_text) LIKE '%ai_classify(%'
                               OR LOWER(statement_text) LIKE '%ai_extract(%'
                               OR LOWER(statement_text) LIKE '%ai_summarize(%'
                               OR LOWER(statement_text) LIKE '%ai_translate(%'
                               OR LOWER(statement_text) LIKE '%ai_forecast(%'
                               OR LOWER(statement_text) LIKE '%ai_similarity(%'
                          THEN 1 END) AS ai_queries
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())""")
        except Exception:
            return CheckResult("13.5.2", "SQL AI Functions Usage",
                "AI & ML Expansion", 0, "info",
                "Could not query AI function usage", "Active usage of SQL AI functions")

        r = rows[0] if rows else {}
        total = r.get("total_queries", 0) or 1
        ai_q = r.get("ai_queries", 0) or 0

        if ai_q >= 100:
            score, status = 100, "pass"
        elif ai_q > 0:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        benchmark = self._peer_benchmark("users", "SQL AI functions integrated into their data pipelines and analytics workflows")

        rec = None
        if ai_q < 100:
            rec = Recommendation(
                action=f"Adopt SQL AI functions (ai_query, ai_classify, ai_extract, ai_forecast) in your data pipelines. {'No' if ai_q == 0 else 'Limited'} usage detected.",
                impact="AI functions let you enrich data with LLM intelligence directly in SQL — sentiment analysis, classification, entity extraction, and forecasting without Python/ML expertise.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/large-language-models/ai-functions.html")

        return CheckResult("13.5.2", "SQL AI Functions Usage",
            "AI & ML Expansion", score, status,
            f"{ai_q} SQL AI function calls (30d)",
            "100+ AI function calls indicating active integration",
            details={"ai_function_queries": ai_q, "total_queries": total,
                     "peer_benchmark": benchmark,
                     "projected_score_boost": 2 if score < 100 else 0},
            recommendation=rec)

    # ── Data Collaboration ──────────────────────────────────────────

    def check_13_6_1_delta_sharing_activity(self) -> CheckResult:
        """Check Delta Sharing usage for data collaboration."""
        try:
            rows = self.executor.execute("""
                SELECT COUNT(*) AS cnt
                FROM system.sharing.materialization_history""")
        except Exception:
            return CheckResult("13.6.1", "Delta Sharing Activity",
                "Data Collaboration", 0, "info",
                "Could not query Delta Sharing data", "Active Delta Sharing usage")

        cnt = rows[0]["cnt"] if rows else 0

        if cnt >= 100:
            score, status = 100, "pass"
        elif cnt > 0:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        benchmark = self._peer_benchmark("workspaces", "active Delta Sharing for cross-organization data collaboration")

        rec = None
        if cnt == 0:
            return CheckResult("13.6.1", "Delta Sharing Activity",
                "Data Collaboration", score, status,
                "No materialization events detected",
                "Active Delta Sharing with regular materialization",
                details={"materialization_events": 0,
                         "summary": "No Delta Sharing materializations found. This may mean Delta Sharing is not yet configured or recipients have not accessed shared data.",
                         "peer_benchmark": benchmark,
                         "projected_score_boost": 1},
                recommendation=Recommendation(
                    action="Set up Delta Sharing to enable secure data collaboration with partners and across teams without copying data.",
                    impact="Enables cross-organization data exchange, multi-cloud access, and data monetization without ETL.",
                    priority="medium",
                    docs_url="https://docs.databricks.com/en/delta-sharing/index.html"))

        if cnt < 100:
            rec = Recommendation(
                action="Expand Delta Sharing for secure cross-team and cross-organization data collaboration. Open protocol, no data copying required.",
                impact="Enables partner data exchange, multi-cloud data access, and external data monetization without ETL pipelines.",
                priority="low" if cnt > 0 else "medium",
                docs_url="https://docs.databricks.com/en/delta-sharing/index.html")

        return CheckResult("13.6.1", "Delta Sharing Activity",
            "Data Collaboration", score, status,
            f"{cnt} materialization events in Delta Sharing",
            "Active Delta Sharing with regular materialization",
            details={"materialization_events": cnt,
                     "peer_benchmark": benchmark,
                     "projected_score_boost": 1 if score < 100 else 0},
            recommendation=rec)

    def check_13_6_2_external_bi_migration(self) -> CheckResult:
        """Detect external BI tool connections that could migrate to native dashboards."""
        try:
            rows = self.executor.execute("""
                SELECT
                    client_application,
                    COUNT(*) AS query_count,
                    COUNT(DISTINCT executed_by) AS user_count
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND (client_application LIKE '%tableau%'
                       OR client_application LIKE '%power%bi%'
                       OR client_application LIKE '%looker%'
                       OR client_application LIKE '%Databricks Connector%'
                       OR client_application LIKE '%ThoughtSpot%'
                       OR client_application LIKE '%Sigma%')
                GROUP BY client_application
                ORDER BY query_count DESC""")
        except Exception:
            return CheckResult("13.6.2", "External BI Tool Migration Opportunity",
                "AI & BI Activation", 0, "info",
                "Could not query external BI usage", "Analytics consolidated on native AI/BI")

        total_queries = sum(r.get("query_count", 0) for r in rows)
        total_users = sum(r.get("user_count", 0) for r in rows)
        tools = [r.get("client_application", "unknown") for r in rows[:5]]

        if total_queries == 0:
            score, status = 100, "pass"
        elif total_queries < 1000:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        benchmark = self._peer_benchmark("users", "consolidated more of their analytics onto native AI/BI Dashboards")

        nc = [{"tool": r.get("client_application", ""), "queries": r.get("query_count", 0),
               "users": r.get("user_count", 0)} for r in rows[:10]]

        rec = None
        if total_queries > 0:
            rec = Recommendation(
                action=f"Consider migrating external BI workloads to AI/BI Dashboards. Detected {total_queries} queries from {len(tools)} external tool(s): {', '.join(tools[:3])}.",
                impact="Eliminates per-seat BI licensing, reduces data latency (no extracts), and provides AI-powered analytics with Genie.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/dashboards/index.html")

        return CheckResult("13.6.2", "External BI Tool Migration Opportunity",
            "AI & BI Activation", score, status,
            f"{total_queries} queries from external BI tools, {total_users} users (30d)",
            "Analytics consolidated on native AI/BI Dashboards",
            details={"external_bi_queries": total_queries, "external_bi_users": total_users,
                     "tools_detected": tools, "non_conforming": nc,
                     "peer_benchmark": benchmark,
                     "projected_score_boost": 3 if score < 100 else 0},
            recommendation=rec)

    def check_13_6_3_streaming_adoption(self) -> CheckResult:
        """Check if streaming workloads exist or if everything is batch-only."""
        try:
            rows = self.executor.execute("""
                SELECT
                    SUM(usage_quantity) AS total_dbus,
                    SUM(CASE WHEN sku_name LIKE '%STREAMING%' OR sku_name LIKE '%DLT%'
                             THEN usage_quantity ELSE 0 END) AS streaming_dbus
                FROM system.billing.usage
                WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND usage_unit = 'DBU'""")
        except Exception:
            return CheckResult("13.6.3", "Streaming Workload Adoption",
                "Operational Excellence", 0, "info",
                "Could not query streaming usage", "Streaming workloads active")

        r = rows[0] if rows else {}
        total = r.get("total_dbus", 0) or 1
        streaming = r.get("streaming_dbus", 0) or 0
        pct = round(streaming / total * 100, 1)

        if pct >= 5:
            score, status = 100, "pass"
        elif pct > 0:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        benchmark = self._peer_benchmark("jobs", "streaming workloads for real-time data freshness")

        rec = None
        if pct < 5:
            rec = Recommendation(
                action="Evaluate batch pipelines that could benefit from streaming for fresher data. Start with Structured Streaming or Declarative Pipelines.",
                impact="Streaming reduces data latency from hours to minutes/seconds, enabling real-time dashboards and operational analytics.",
                priority="low",
                docs_url="https://docs.databricks.com/en/structured-streaming/index.html")

        return CheckResult("13.6.3", "Streaming Workload Adoption",
            "Operational Excellence", score, status,
            f"{pct}% of DBUs from streaming workloads (30d)",
            "5%+ of workloads leveraging streaming for real-time data",
            details={"streaming_dbus": streaming, "total_dbus": total, "streaming_pct": pct,
                     "peer_benchmark": benchmark,
                     "projected_score_boost": 1 if score < 100 else 0},
            recommendation=rec)
