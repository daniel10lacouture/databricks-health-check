// ============================================================
// Databricks Account Health Check — TypeScript Types
// ============================================================

export type ScoreLabel = 'Excellent' | 'Good' | 'Needs Attention' | 'Critical';
export type CheckStatus = 'pass' | 'partial' | 'fail' | 'not_evaluated' | 'info';
export type Priority = 'high' | 'medium' | 'low';
export type SectionType = 'core' | 'conditional' | 'advisory';
export type SizingVerdict = 'undersized' | 'right-sized' | 'oversized';

export interface Recommendation {
  action: string;
  impact: string;
  priority: Priority;
  estimated_savings?: string;
  docs_url?: string;
  sql_command?: string;
}

export interface CheckResult {
  check_id: string;
  name: string;
  subsection: string;
  score: number;            // 0 | 50 | 100
  status: CheckStatus;
  current_value: string;
  target_value: string;
  details?: Record<string, unknown>;
  recommendation?: Recommendation;
}

export interface SectionResult {
  section_id: string;
  section_key: string;
  name: string;
  description: string;
  icon: string;
  section_type: SectionType;
  active: boolean;
  score: number;
  checks: CheckResult[];
  subsections: string[];
}

export interface WarehouseSizingResult {
  warehouse_id: string;
  warehouse_name: string;
  warehouse_type: string;
  warehouse_size: string;
  sizing_score: number;
  verdict: SizingVerdict;
  metrics: {
    spill_pct: number;
    queue_p95_ms: number;
    p95_duration_ms: number;
    utilization_pct: number;
    idle_pct: number;
  };
  recommendation: string;
}

export interface HealthCheckResults {
  overall_score: number;
  score_label: ScoreLabel;
  sections: SectionResult[];
  top_recommendations: Recommendation[];
  warehouse_sizing?: WarehouseSizingResult[];
  run_timestamp: string;
  duration_seconds: number;
}

export interface Warehouse {
  id: string;
  name: string;
  type: string;
  size: string;
  state: string;
}

export interface ProgressEvent {
  type: 'progress' | 'section_complete' | 'complete' | 'error';
  section?: string;
  section_name?: string;
  section_index?: number;
  total_sections?: number;
  status?: string;
  score?: number;
  section_result?: SectionResult;
  overall_score?: number;
  results?: HealthCheckResults;
  message?: string;
}

export type AppView = 'setup' | 'running' | 'dashboard' | 'section';

export function getScoreLabel(score: number): ScoreLabel {
  if (score >= 90) return 'Excellent';
  if (score >= 70) return 'Good';
  if (score >= 50) return 'Needs Attention';
  return 'Critical';
}

export function getScoreColor(score: number): string {
  if (score >= 90) return '#059669';
  if (score >= 70) return '#2563EB';
  if (score >= 50) return '#D97706';
  return '#DC2626';
}

export const SECTION_META: Record<string, { name: string; icon: string; description: string; type: SectionType }> = {
  data_engineering:  { name: 'Data Engineering & Table Health', icon: 'database', description: 'Delta tables, ETL pipelines, data layout, maintenance, and data quality.', type: 'core' },
  sql_analytics:     { name: 'Data Warehousing / SQL Analytics', icon: 'bar-chart-3', description: 'SQL warehouses, query performance, warehouse sizing, semantic layer.', type: 'core' },
  compute:           { name: 'Compute Management', icon: 'cpu', description: 'Cluster configuration, policies, right-sizing, runtime versions.', type: 'core' },
  cost:              { name: 'Cost Optimization', icon: 'dollar-sign', description: 'Cross-workload cost analysis, idle resource detection, spend efficiency.', type: 'core' },
  security:          { name: 'Security & Compliance', icon: 'shield', description: 'Network security, identity management, access controls, audit logging.', type: 'core' },
  governance:        { name: 'Governance (Unity Catalog)', icon: 'lock', description: 'UC adoption, catalog/schema organization, access control, lineage.', type: 'core' },
  ai_ml:             { name: 'AI & ML Workloads', icon: 'brain', description: 'Model registry, serving endpoints, experiment tracking, AI cost.', type: 'conditional' },
  apps:              { name: 'Databricks Apps', icon: 'layout-grid', description: 'App deployment health, security, resource usage, configuration.', type: 'conditional' },
  lakebase:          { name: 'OLTP / Lakebase', icon: 'server', description: 'Lakebase instance health, performance, connection management.', type: 'conditional' },
  delta_sharing:     { name: 'Delta Sharing', icon: 'share-2', description: 'Share governance, recipient management, security, activity.', type: 'conditional' },
  ingestion:         { name: 'Data Ingestion', icon: 'download', description: 'Ingestion pipeline health, connector inventory, throughput, freshness.', type: 'conditional' },
  workspace_admin:   { name: 'Workspace Administration', icon: 'settings', description: 'Multi-workspace governance, user activity, resource hygiene.', type: 'core' },
  cicd:              { name: 'CI/CD & DevOps', icon: 'git-branch', description: 'Deployment practices, Git integration, Asset Bundles.', type: 'advisory' },
};

export const SECTION_ORDER = [
  'data_engineering', 'sql_analytics', 'compute', 'cost', 'security',
  'governance', 'ai_ml', 'apps', 'lakebase', 'delta_sharing',
  'ingestion', 'workspace_admin', 'cicd',
];
