import { SectionResult, CheckResult as CheckResultType, getScoreColor } from '../types';
import { colors, shadows, radii } from '../styles/theme';
import CheckResultRow from './CheckResult';
import ScoreGauge from './ScoreGauge';
import { ArrowLeft } from 'lucide-react';

interface Props {
  section: SectionResult;
  onBack: () => void;
}

export default function SectionDetail({ section, onBack }: Props) {
  // Group checks by subsection
  const grouped = new Map<string, CheckResultType[]>();
  for (const check of section.checks || []) {
    const sub = check.subsection || 'General';
    if (!grouped.has(sub)) grouped.set(sub, []);
    grouped.get(sub)!.push(check);
  }

  const scored = section.checks?.filter(c => c.status !== 'not_evaluated' && c.status !== 'info') || [];
  const passCount = scored.filter(c => c.score === 100).length;
  const partialCount = scored.filter(c => c.score === 50).length;
  const failCount = scored.filter(c => c.score === 0).length;

  return (
    <div style={{ padding: 32, maxWidth: 960, margin: '0 auto' }}>
      {/* Back button */}
      <button onClick={onBack} style={{
        display: 'flex', alignItems: 'center', gap: 6, padding: '6px 0',
        background: 'none', border: 'none', fontSize: 13, color: colors.textSecondary,
        cursor: 'pointer', marginBottom: 24,
      }}>
        <ArrowLeft size={14} /> Back to Dashboard
      </button>

      {/* Section header */}
      <div style={{
        background: colors.white, borderRadius: radii.lg, padding: 28,
        boxShadow: shadows.sm, border: `1px solid ${colors.border}`,
        display: 'flex', alignItems: 'center', gap: 32, marginBottom: 28,
      }}>
        <ScoreGauge score={section.score ?? 0} size="md" showLabel />
        <div style={{ flex: 1 }}>
          <h1 style={{ fontSize: 22, fontWeight: 700, color: colors.textPrimary, margin: '0 0 6px' }}>
            {section.name}
          </h1>
          <p style={{ fontSize: 14, color: colors.textSecondary, margin: '0 0 12px' }}>
            {section.description}
          </p>
          <div style={{ display: 'flex', gap: 16 }}>
            <MiniStat count={passCount} label="Pass" color="#059669" />
            <MiniStat count={partialCount} label="Partial" color="#D97706" />
            <MiniStat count={failCount} label="Fail" color="#DC2626" />
            <MiniStat count={(section.checks?.length ?? 0) - scored.length} label="N/A" color={colors.textTertiary} />
          </div>
        </div>
      </div>

      {/* Checks grouped by subsection */}
      {Array.from(grouped.entries()).map(([subsection, checks]) => (
        <div key={subsection} style={{ marginBottom: 28 }}>
          <h2 style={{
            fontSize: 15, fontWeight: 600, color: colors.textPrimary,
            margin: '0 0 12px', paddingBottom: 8,
            borderBottom: `2px solid ${colors.border}`,
          }}>
            {subsection}
          </h2>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {checks.map(check => (
              <CheckResultRow key={check.check_id} check={check} />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function MiniStat({ count, label, color }: { count: number; label: string; color: string }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <div style={{
        width: 8, height: 8, borderRadius: '50%', background: color,
      }} />
      <span style={{ fontSize: 13, color: colors.textSecondary }}>
        <strong style={{ color }}>{count}</strong> {label}
      </span>
    </div>
  );
}
