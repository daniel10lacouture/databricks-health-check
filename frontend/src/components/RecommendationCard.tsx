import { Recommendation } from '../types';
import { colors, radii } from '../styles/theme';
import { ArrowUpRight } from 'lucide-react';

interface Props {
  recommendation: Recommendation;
  compact?: boolean;
}

const PRIORITY_COLORS: Record<string, string> = {
  high: colors.critical,
  medium: colors.attention,
  low: colors.good,
};

export default function RecommendationCard({ recommendation, compact = false }: Props) {
  const borderColor = PRIORITY_COLORS[recommendation.priority] || colors.good;

  return (
    <div style={{
      borderLeft: `3px solid ${borderColor}`,
      borderRadius: radii.sm,
      background: colors.surface,
      padding: compact ? '10px 14px' : '14px 18px',
      fontSize: compact ? 13 : 14,
    }}>
      <div style={{ marginBottom: compact ? 4 : 8 }}>
        <span style={{
          fontSize: 10,
          fontWeight: 600,
          textTransform: 'uppercase',
          letterSpacing: '0.05em',
          color: borderColor,
          marginRight: 8,
        }}>
          {recommendation.priority} priority
        </span>
        {recommendation.estimated_savings && (
          <span style={{ fontSize: 12, color: colors.excellent, fontWeight: 600 }}>
            {recommendation.estimated_savings}
          </span>
        )}
      </div>
      <p style={{ color: colors.textPrimary, lineHeight: 1.5, marginBottom: compact ? 4 : 8 }}>
        {recommendation.action}
      </p>
      {!compact && recommendation.impact && (
        <p style={{ color: colors.textSecondary, fontSize: 13, marginBottom: 8 }}>
          <strong>Impact:</strong> {recommendation.impact}
        </p>
      )}
      {recommendation.sql_command && (
        <pre style={{
          background: colors.dark,
          color: '#E5E7EB',
          padding: '8px 12px',
          borderRadius: radii.sm,
          fontSize: 12,
          overflow: 'auto',
          marginBottom: 8,
        }}>
          {recommendation.sql_command}
        </pre>
      )}
      {recommendation.docs_url && (
        <a
          href={recommendation.docs_url}
          target="_blank"
          rel="noopener noreferrer"
          style={{
            fontSize: 12,
            color: colors.good,
            textDecoration: 'none',
            display: 'inline-flex',
            alignItems: 'center',
            gap: 4,
          }}
        >
          Documentation <ArrowUpRight size={12} />
        </a>
      )}
    </div>
  );
}
