import { CheckResult as CheckResultType } from '../types';
import { colors, shadows, radii } from '../styles/theme';
import RecommendationCard from './RecommendationCard';
import { CheckCircle, AlertTriangle, XCircle, Minus } from 'lucide-react';

interface Props {
  check: CheckResultType;
}

const STATUS_CONFIG: Record<string, { icon: typeof CheckCircle; color: string }> = {
  pass:          { icon: CheckCircle, color: colors.pass },
  partial:       { icon: AlertTriangle, color: colors.partial },
  fail:          { icon: XCircle, color: colors.fail },
  not_evaluated: { icon: Minus, color: colors.notEvaluated },
  info:          { icon: Minus, color: colors.good },
};

export default function CheckResultRow({ check }: Props) {
  const cfg = STATUS_CONFIG[check.status] || STATUS_CONFIG.not_evaluated;
  const Icon = cfg.icon;

  return (
    <div style={{
      background: colors.white,
      borderRadius: radii.md,
      border: `1px solid ${colors.border}`,
      padding: 16,
      marginBottom: 8,
      boxShadow: shadows.sm,
    }}>
      <div style={{ display: 'flex', alignItems: 'flex-start', gap: 12 }}>
        <div style={{ flexShrink: 0, marginTop: 2 }}>
          <Icon size={18} color={cfg.color} />
        </div>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
            <span style={{ fontWeight: 600, fontSize: 14, color: colors.textPrimary }}>
              <span style={{ color: colors.textSecondary, fontWeight: 400, marginRight: 8 }}>{check.check_id}</span>
              {check.name}
            </span>
            {check.status !== 'not_evaluated' && check.status !== 'info' && (
              <span style={{
                fontWeight: 600,
                fontSize: 13,
                color: cfg.color,
                fontFeatureSettings: '"tnum"',
                flexShrink: 0,
                marginLeft: 12,
              }}>
                {check.score}/100
              </span>
            )}
          </div>
          <div style={{ display: 'flex', gap: 24, fontSize: 13, color: colors.textSecondary }}>
            <span><strong>Current:</strong> {check.current_value}</span>
            {check.target_value && <span><strong>Target:</strong> {check.target_value}</span>}
          </div>
        </div>
      </div>
      {check.recommendation && (check.status === 'partial' || check.status === 'fail') && (
        <div style={{ marginTop: 12, marginLeft: 30 }}>
          <RecommendationCard recommendation={check.recommendation} compact />
        </div>
      )}
    </div>
  );
}
