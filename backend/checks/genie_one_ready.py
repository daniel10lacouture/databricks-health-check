"""
Genie ONE Ready — a stupid-simple, do-it-in-your-account checklist for
onboarding business (consumer) users to Genie, based on the internal
"Onboarding Business Users to Genie" guide (go/genieready).

This is a guide, not a scorecard: every item is informational (not scored),
so the tab renders as an ordered set of steps with concrete clicks and
PUBLIC documentation links only. No internal links, no warehouse or
table-count "readiness" scoring — those have nothing to do with Genie ONE.
"""
from checks.base import BaseCheckRunner, CheckResult, Recommendation
from concurrent.futures import ThreadPoolExecutor


class GenieOneReadyCheckRunner(BaseCheckRunner):
    section_id = "genie_one_ready"
    section_name = "Genie One Ready"
    section_type = "advisory"
    icon = "sparkle"

    def get_subsections(self):
        return ["Onboard business users to Genie"]

    def is_active(self) -> bool:
        return True

    def run_checks(self):
        # These items are static guidance (no I/O), so run them in a fixed order
        # rather than the base class's parallel/as-completed order — a numbered
        # guide must render in sequence.
        methods = sorted([m for m in dir(self) if m.startswith("check_")])
        return [getattr(self, m)() for m in methods]

    _SUB = "Onboard business users to Genie"

    def check_1_free(self) -> CheckResult:
        return CheckResult("gr1", "Genie is free through January 31, 2027", self._SUB,
            0, "info",
            "No cost for business users, no seat licenses",
            "Roll Genie out at no cost during the promo",
            recommendation=Recommendation(
                action="Genie (the business-user chat, fka Databricks One / One Chat) is free for identified users through Jan 31, 2027. Genie Code additionally gives every user 150 free DBUs/month, with 25% off any overage through the same date. Only service-principal (automated) usage is billed.",
                impact="You can onboard your whole business-user population now at no cost — lead with this.",
                priority="low",
                docs_url="https://docs.databricks.com/aws/en/ai-bi/release-notes"))

    def check_2_prereqs(self) -> CheckResult:
        return CheckResult("gr2", "Prerequisites", self._SUB,
            0, "info",
            "Premium+ plan · Unity Catalog + identity federation · SSO",
            "Confirm before you start",
            recommendation=Recommendation(
                action="Confirm: (1) Databricks account on Premium plan or above, (2) Unity Catalog enabled with identity federation on the target workspace, (3) Account SSO / Unified Login enabled (AWS). You'll need account-admin access for Step 1 and workspace-admin access for Step 2.",
                impact="These are the baseline requirements for the seamless Genie experience.",
                priority="medium",
                docs_url="https://docs.databricks.com/aws/en/admin/users-groups/"))

    def check_3_provision(self) -> CheckResult:
        return CheckResult("gr3", "Step 1 — Provision users at the account level", self._SUB,
            0, "info",
            "Get every business user into the account (no seat limits)",
            "Users exist as account-level identities",
            recommendation=Recommendation(
                action="Easiest path (Entra ID): turn on Automatic Identity Management — Account Console → Security → User Provisioning → toggle on Automatic Identity Management (make sure SSO is enabled first). AIM is GA and on by default for accounts created after Aug 1, 2025; users and groups then sync automatically with no IdP app config. No AIM? Use account-level SCIM (Okta / OneLogin / generic): Security → User Provisioning → Enable, then paste the SCIM URL + token into your IdP.",
                impact="Provisions as many business users as possible — all data stays governed by Unity Catalog.",
                priority="high",
                docs_url="https://docs.databricks.com/aws/en/admin/users-groups/automatic-identity-management"))

    def check_4_consumer_access(self) -> CheckResult:
        return CheckResult("gr4", "Step 2 — Give business users Consumer Access", self._SUB,
            0, "info",
            "Add users to a workspace with the Consumer Access entitlement",
            "Business users have Consumer Access",
            recommendation=Recommendation(
                action="In the target workspace: Settings → Advanced → 'Choose entitlements when adding principals to workspaces' → Manage (one-time migration). Then Settings → Identity and access → Users (or Groups) → Add → search the user/group (or add 'All account users' at once) → grant Consumer Access only. Consumer Access gives a simplified, view-only Genie / dashboards / apps experience — no authoring UI.",
                impact="This is what actually lets non-technical users open Genie and ask questions.",
                priority="high",
                docs_url="https://docs.databricks.com/aws/en/ai-bi/consumers/"))

    def check_5_custom_url(self) -> CheckResult:
        return CheckResult("gr5", "Step 3 (optional) — Set a custom URL", self._SUB,
            0, "info",
            "companyname.databricks.com — log in once, no re-auth",
            "Custom URL + auto-redirect enabled",
            recommendation=Recommendation(
                action="Give users a memorable URL (companyname.databricks.com) so they log in once and click through any asset in any workspace without re-authenticating. Account Console → Settings → Account Settings → enable 'Custom URL' and 'Auto-redirect', then send users to companyname.databricks.com/one.",
                impact="Removes the repeated-login friction that trips up business users the most.",
                priority="low",
                docs_url="https://docs.databricks.com/aws/en/workspace/"))

