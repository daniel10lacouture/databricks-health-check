"""Section: Delta Sharing — checks with drill-downs."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class DeltaSharingCheckRunner(BaseCheckRunner):
    section_id = "data_engineering"
    section_name = "Data Engineering"
    section_type = "conditional"
    icon = "share-2"

    def get_subsections(self):
        return ["Share Inventory", "Recipients & Providers", "Marketplace Opportunities"]

    def is_active(self) -> bool:
        """Active if shares, recipients, or providers exist."""
        try:
            r = self.executor.execute("""
                SELECT (SELECT COUNT(*) FROM system.information_schema.shares) +
                       (SELECT COUNT(*) FROM system.information_schema.recipients) +
                       (SELECT COUNT(*) FROM system.information_schema.providers) AS total
            """)
            return r[0]["total"] > 0
        except Exception:
            return False

    def check_10_1_1_share_inventory(self) -> CheckResult:
        """Inventory of Delta Shares — informational."""
        try:
            rows = self.executor.execute("""
                SELECT share_name, share_owner, comment,
                       DATE(created) AS created_date
                FROM system.information_schema.shares
                ORDER BY created DESC
            """)
            count = len(rows) if rows else 0
            nc = [{"share_name": r["share_name"], "owner": r["share_owner"],
                   "comment": (r.get("comment") or "")[:60] or "No description",
                   "created": str(r["created_date"])} for r in rows[:20]] if rows else []

            return CheckResult("10.1.1", "Delta Shares inventory",
                "Share Inventory", None, "info",
                f"{count} share(s) configured",
                "Track active shares",
                details={"shares": nc, "summary": f"{count} Delta Sharing shares available"},
                recommendation=None)
        except Exception as e:
            return CheckResult("10.1.1", "Delta Shares inventory",
                "Share Inventory", None, "info",
                f"Query failed: {str(e)[:60]}", "N/A")

    def check_10_1_2_recipient_inventory(self) -> CheckResult:
        """Inventory of share recipients — informational."""
        try:
            rows = self.executor.execute("""
                SELECT recipient_name, authentication_type, cloud, region,
                       DATE(created) AS created_date, comment
                FROM system.information_schema.recipients
                ORDER BY created DESC
            """)
            count = len(rows) if rows else 0
            
            # Group by auth type
            auth_types = {}
            for r in (rows or []):
                at = r.get("authentication_type", "UNKNOWN")
                auth_types[at] = auth_types.get(at, 0) + 1
            
            nc = [{"recipient": r["recipient_name"], "auth_type": r["authentication_type"],
                   "cloud": r.get("cloud", ""), "region": r.get("region", ""),
                   "created": str(r["created_date"])} for r in rows[:20]] if rows else []

            rec = None
            if count > 0:
                token_count = auth_types.get("TOKEN", 0)
                if token_count > 0:
                    rec = Recommendation(
                        action=f"Consider migrating {token_count} token-based recipients to Databricks-to-Databricks sharing.",
                        impact="D2D sharing is more secure and supports real-time updates without token management.",
                        priority="low",
                        docs_url="https://docs.databricks.com/en/data-sharing/index.html")

            summary = ", ".join([f"{v} {k}" for k, v in auth_types.items()]) if auth_types else "No recipients"
            return CheckResult("10.1.2", "Share recipients inventory",
                "Recipients & Providers", None, "info",
                f"{count} recipient(s): {summary}",
                "Track external data consumers",
                details={"recipients": nc},
                recommendation=rec)
        except Exception as e:
            return CheckResult("10.1.2", "Share recipients inventory",
                "Recipients & Providers", None, "info",
                f"Query failed: {str(e)[:60]}", "N/A")

    def check_10_1_3_provider_inventory(self) -> CheckResult:
        """Inventory of data providers (marketplace, external shares consumed)."""
        try:
            rows = self.executor.execute("""
                SELECT provider_name, authentication_type, cloud, region,
                       DATE(created) AS created_date, comment
                FROM system.information_schema.providers
                ORDER BY created DESC
            """)
            count = len(rows) if rows else 0
            nc = [{"provider": r["provider_name"], "auth_type": r["authentication_type"],
                   "cloud": r.get("cloud", ""), "comment": (r.get("comment") or "")[:50]} 
                  for r in rows[:20]] if rows else []

            return CheckResult("10.1.3", "Data providers (consumed shares)",
                "Recipients & Providers", None, "info",
                f"{count} provider(s) configured",
                "Track external data sources",
                details={"providers": nc, "summary": f"Consuming data from {count} external providers"},
                recommendation=None)
        except Exception as e:
            return CheckResult("10.1.3", "Data providers inventory",
                "Recipients & Providers", None, "info",
                f"Query failed: {str(e)[:60]}", "N/A")

    def check_10_1_4_share_documentation(self) -> CheckResult:
        """Check if shares have descriptions/comments."""
        try:
            rows = self.executor.execute("""
                SELECT share_name,
                       CASE WHEN comment IS NOT NULL AND comment != '' THEN 1 ELSE 0 END AS has_comment
                FROM system.information_schema.shares
            """)
            if not rows:
                return CheckResult("10.1.4", "Share documentation",
                    "Share Inventory", 100, "pass", "No shares to document", "All shares documented")
            
            total = len(rows)
            documented = sum(r["has_comment"] for r in rows)
            pct = documented / total * 100 if total > 0 else 100
            
            if pct >= 80:
                score, status = 100, "pass"
            elif pct >= 50:
                score, status = 50, "partial"
            else:
                score, status = 0, "fail"
            
            undoc = [{"share_name": r["share_name"], "action": "Add a comment describing the share purpose"}
                     for r in rows if not r["has_comment"]][:15]

            rec = None
            if score < 100:
                rec = Recommendation(
                    action=f"Document {total - documented} undocumented shares with descriptive comments.",
                    impact="Clear documentation helps recipients understand what data they're receiving.",
                    priority="low",
                    docs_url="https://docs.databricks.com/en/data-sharing/create-share.html")

            return CheckResult("10.1.4", "Share documentation coverage",
                "Share Inventory", score, status,
                f"{pct:.0f}% ({documented}/{total} shares documented)",
                "≥80% shares have descriptions",
                details={"undocumented_shares": undoc} if undoc else {},
                recommendation=rec)
        except Exception as e:
            return CheckResult("10.1.4", "Share documentation",
                "Share Inventory", 0, "not_evaluated",
                f"Query failed: {str(e)[:60]}", "≥80% documented")

    def check_10_1_5_marketplace_opportunities(self) -> CheckResult:
        """Suggest marketplace datasets based on customer's data patterns — informational."""
        try:
            # Get the catalogs/schemas being used to infer industry/domain
            rows = self.executor.execute("""
                SELECT LOWER(table_catalog) AS catalog, LOWER(table_schema) AS schema, COUNT(*) AS tables
                FROM system.information_schema.tables
                WHERE table_catalog NOT IN ('system', '__databricks_internal', 'hive_metastore', 'samples')
                GROUP BY 1, 2 ORDER BY tables DESC LIMIT 30
            """)
            
            # Simple keyword matching to suggest marketplace categories
            keywords_found = set()
            suggestions = []
            
            keyword_map = {
                "healthcare": ["health", "patient", "claim", "pharmacy", "medical", "hospital", "diagnosis"],
                "financial": ["finance", "bank", "transaction", "payment", "loan", "credit", "trading"],
                "retail": ["retail", "product", "order", "customer", "inventory", "sales", "ecommerce"],
                "geospatial": ["geo", "location", "address", "map", "coordinate", "zip", "postal"],
                "weather": ["weather", "climate", "temperature", "forecast"],
                "demographics": ["census", "population", "demographic", "income"],
            }
            
            for r in rows or []:
                combined = f"{r['catalog']} {r['schema']}"
                for category, terms in keyword_map.items():
                    if any(term in combined for term in terms):
                        keywords_found.add(category)
            
            marketplace_suggestions = {
                "healthcare": "Healthcare datasets (ICD codes, drug databases, provider directories)",
                "financial": "Financial data (stock prices, economic indicators, exchange rates)", 
                "retail": "Retail insights (consumer trends, market research)",
                "geospatial": "Geospatial data (boundaries, POIs, address validation)",
                "weather": "Weather & climate datasets",
                "demographics": "Census & demographic data",
            }
            
            for kw in keywords_found:
                if kw in marketplace_suggestions:
                    suggestions.append({"category": kw.title(), 
                                        "suggestion": marketplace_suggestions[kw],
                                        "marketplace_url": "https://marketplace.databricks.com"})
            
            if not suggestions:
                suggestions = [{"category": "General", 
                               "suggestion": "Explore Databricks Marketplace for public datasets",
                               "marketplace_url": "https://marketplace.databricks.com"}]

            return CheckResult("10.1.5", "Marketplace data opportunities",
                "Marketplace Opportunities", None, "info",
                f"{len(suggestions)} potential marketplace categories identified",
                "Explore external datasets",
                details={"suggested_categories": suggestions,
                         "summary": "Based on your data patterns, consider these marketplace datasets"},
                recommendation=Recommendation(
                    action="Explore Databricks Marketplace for datasets that complement your data.",
                    impact="Enrich your analytics with external data sources.",
                    priority="low",
                    docs_url="https://marketplace.databricks.com"))
        except Exception as e:
            return CheckResult("10.1.5", "Marketplace opportunities",
                "Marketplace Opportunities", None, "info",
                f"Analysis failed: {str(e)[:60]}", "N/A")
