import { WarehouseSizingResult, getScoreColor } from '../types';
import { colors, shadows, radii } from '../styles/theme';
import ScoreGauge from './ScoreGauge';
import { ArrowUp, ArrowDown, Check } from 'lucide-react';

interface Props {
  warehouses: WarehouseSizingResult[];
}

const verdictConfig = {
  undersized: { color: '#DC2626', bg: '#FEF2F2', icon: ArrowUp, label: 'Undersized' },
  'right-sized': { color: '#059669', bg: '#ECFDF5', icon: Check, label: 'Right-sized' },
  oversized: { color: '#D97706', bg: '#FFFBEB', icon: ArrowDown, label: 'Oversized' },
};

export default function WarehouseSizing({ warehouses }: Props) {
  if (!warehouses || warehouses.length === 0) return null;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {warehouses.map(wh => {
        const config = verdictConfig[wh.verdict] || verdictConfig['right-sized'];
        const VerdictIcon = config.icon;

        return (
          <div key={wh.warehouse_id} style={{
            background: colors.white, borderRadius: radii.md, padding: 24,
            border: `1px solid ${colors.border}`, boxShadow: shadows.sm,
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <div>
                <div style={{ fontSize: 16, fontWeight: 600, color: colors.textPrimary }}>{wh.warehouse_name}</div>
                <div style={{ fontSize: 13, color: colors.textSecondary, marginTop: 2 }}>
                  {wh.warehouse_type} · {wh.warehouse_size}
                </div>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                <div style={{
                  display: 'flex', alignItems: 'center', gap: 6, padding: '4px 12px',
                  borderRadius: 16, background: config.bg, color: config.color,
                  fontSize: 13, fontWeight: 600,
                }}>
                  <VerdictIcon size={14} /> {config.label}
                </div>
                <ScoreGauge score={wh.sizing_score} size="sm" />
              </div>
            </div>

            {/* Metrics grid */}
            <div style={{
              display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 16, marginTop: 20,
              padding: 16, background: colors.surface, borderRadius: radii.md,
            }}>
              <Metric label="Spill %" value={`${wh.metrics.spill_pct.toFixed(1)}%`}
                color={wh.metrics.spill_pct > 20 ? '#DC2626' : wh.metrics.spill_pct > 5 ? '#D97706' : '#059669'} />
              <Metric label="Queue p95" value={`${(wh.metrics.queue_p95_ms / 1000).toFixed(1)}s`}
                color={wh.metrics.queue_p95_ms > 300000 ? '#DC2626' : wh.metrics.queue_p95_ms > 30000 ? '#D97706' : '#059669'} />
              <Metric label="p95 Duration" value={`${(wh.metrics.p95_duration_ms / 1000).toFixed(1)}s`}
                color={wh.metrics.p95_duration_ms > 300000 ? '#DC2626' : wh.metrics.p95_duration_ms > 60000 ? '#D97706' : '#059669'} />
              <Metric label="Utilization" value={`${wh.metrics.utilization_pct.toFixed(0)}%`}
                color={wh.metrics.utilization_pct < 20 ? '#DC2626' : wh.metrics.utilization_pct < 50 ? '#D97706' : '#059669'} />
              <Metric label="Idle %" value={`${wh.metrics.idle_pct.toFixed(0)}%`}
                color={wh.metrics.idle_pct > 30 ? '#DC2626' : wh.metrics.idle_pct > 10 ? '#D97706' : '#059669'} />
            </div>

            {wh.recommendation && (
              <p style={{ marginTop: 12, fontSize: 13, color: colors.textSecondary, lineHeight: 1.5 }}>
                {wh.recommendation}
              </p>
            )}
          </div>
        );
      })}
    </div>
  );
}

function Metric({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <div style={{ textAlign: 'center' }}>
      <div style={{ fontSize: 18, fontWeight: 600, color, fontVariantNumeric: 'tabular-nums' }}>{value}</div>
      <div style={{ fontSize: 11, color: colors.textTertiary, marginTop: 2 }}>{label}</div>
    </div>
  );
}
