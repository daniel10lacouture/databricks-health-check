import { HealthCheckResults, getScoreColor, getScoreLabel, SECTION_META } from '../types';
import { colors, shadows, radii } from '../styles/theme';
import ScoreGauge from './ScoreGauge';
import RecommendationCard from './RecommendationCard';
import { getIcon } from './Layout';
import { Download, AlertTriangle, CheckCircle, Minus } from 'lucide-react';

interface Props {
  results: HealthCheckResults;
  onSectionClick: (id: string) => void;
  onExport: () => void;
}

export default function Dashboard({ results, onSectionClick, onExport }: Props) {
  const activeSections = results.sections.filter(s => s.active);
  const passCount = activeSections.filter(s => (s.score ?? 0) >= 90).length;
  const warnCount = activeSections.filter(s => (s.score ?? 0) >= 50 && (s.score ?? 0) < 90).length;
  const failCount = activeSections.filter(s => (s.score ?? 0) < 50).length;

  return (
    <div style={{ padding: 32, maxWidth: 1200, margin: '0 auto' }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 32 }}>
        <div>
          <h1 style={{ fontSize: 24, fontWeight: 700, color: colors.textPrimary, margin: 0 }}>Account Health Dashboard</h1>
          <p style={{ fontSize: 14, color: colors.textSecondary, marginTop: 4 }}>
            {results.run_timestamp ? new Date(results.run_timestamp).toLocaleString() : ''}
          </p>
        </div>
        <button onClick={onExport} style={{
          display: 'flex', alignItems: 'center', gap: 8, padding: '8px 16px',
          borderRadius: radii.md, border: `1px solid ${colors.border}`,
          background: colors.white, fontSize: 13, fontWeight: 500, color: colors.textPrimary,
          cursor: 'pointer',
        }}>
          <Download size={14} /> Export Report
        </button>
      </div>

      {/* Overall Score Card */}
      <div style={{
        background: colors.white, borderRadius: radii.lg, padding: 32,
        boxShadow: shadows.md, border: `1px solid ${colors.border}`,
        display: 'flex', alignItems: 'center', gap: 48, marginBottom: 32,
      }}>
        <ScoreGauge score={results.overall_score} size="lg" showLabel />
        <div style={{ flex: 1 }}>
          <h2 style={{ fontSize: 18, fontWeight: 600, color: colors.textPrimary, margin: '0 0 12px' }}>
            Overall: {getScoreLabel(results.overall_score)}
          </h2>
          <div style={{ display: 'flex', gap: 24 }}>
            <Stat label="Sections Analyzed" value={`${activeSections.length}`} color={colors.textPrimary} />
            <Stat label="Excellent" value={`${passCount}`} color="#059669" />
            <Stat label="Needs Work" value={`${warnCount}`} color="#D97706" />
            <Stat label="Critical" value={`${failCount}`} color="#DC2626" />
          </div>
        </div>
      </div>

      {/* Section Grid */}
      <h3 style={{ fontSize: 16, fontWeight: 600, color: colors.textPrimary, margin: '0 0 16px' }}>Sections</h3>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: 16, marginBottom: 40 }}>
        {results.sections.map(section => {
          const meta = SECTION_META[section.section_id];
          const isActive = section.active;
          const score = section.score ?? 0;
          const issues = section.checks?.filter(c => c.status === 'fail' || c.status === 'partial').length ?? 0;

          return (
            <div
              key={section.section_id}
              onClick={() => isActive && onSectionClick(section.section_id)}
              style={{
                background: colors.white, borderRadius: radii.md, padding: 20,
                border: `1px solid ${colors.border}`, cursor: isActive ? 'pointer' : 'default',
                opacity: isActive ? 1 : 0.5, transition: 'all 0.2s ease',
                boxShadow: shadows.sm,
              }}
              onMouseEnter={e => { if (isActive) { e.currentTarget.style.transform = 'translateY(-2px)'; e.currentTarget.style.boxShadow = shadows.md; } }}
              onMouseLeave={e => { e.currentTarget.style.transform = 'none'; e.currentTarget.style.boxShadow = shadows.sm; }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <div style={{
                    width: 36, height: 36, borderRadius: 8,
                    background: isActive ? `${getScoreColor(score)}15` : colors.surface,
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    color: isActive ? getScoreColor(score) : colors.textTertiary,
                  }}>
                    {getIcon(meta?.icon || section.icon, 18)}
                  </div>
                  <div>
                    <div style={{ fontSize: 14, fontWeight: 600, color: colors.textPrimary }}>{meta?.name || section.name}</div>
                    <div style={{ fontSize: 11, color: colors.textTertiary, textTransform: 'uppercase', letterSpacing: 0.5 }}>
                      {section.section_type}
                    </div>
                  </div>
                </div>
                {isActive ? (
                  <div style={{ fontSize: 28, fontWeight: 700, color: getScoreColor(score), fontVariantNumeric: 'tabular-nums' }}>
                    {Math.round(score)}
                  </div>
                ) : (
                  <div style={{ fontSize: 12, color: colors.textTertiary }}>N/A</div>
                )}
              </div>

              {isActive && (
                <>
                  {/* Score bar */}
                  <div style={{ marginTop: 16, height: 4, background: colors.surface, borderRadius: 2, overflow: 'hidden' }}>
                    <div style={{
                      height: '100%', width: `${score}%`, background: getScoreColor(score),
                      borderRadius: 2, transition: 'width 0.5s ease',
                    }} />
                  </div>

                  {/* Issues count */}
                  {issues > 0 && (
                    <div style={{ marginTop: 10, display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: '#D97706' }}>
                      <AlertTriangle size={12} /> {issues} check{issues > 1 ? 's' : ''} need attention
                    </div>
                  )}
                  {issues === 0 && (
                    <div style={{ marginTop: 10, display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: '#059669' }}>
                      <CheckCircle size={12} /> All checks passing
                    </div>
                  )}
                </>
              )}
              {!isActive && (
                <div style={{ marginTop: 12, display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: colors.textTertiary }}>
                  <Minus size={12} /> Not Evaluated — No Significant Usage
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Top Recommendations */}
      {results.top_recommendations && results.top_recommendations.length > 0 && (
        <>
          <h3 style={{ fontSize: 16, fontWeight: 600, color: colors.textPrimary, margin: '0 0 16px' }}>
            Top Recommendations
          </h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {results.top_recommendations.slice(0, 5).map((rec, i) => (
              <RecommendationCard key={i} recommendation={rec} />
            ))}
          </div>
        </>
      )}
    </div>
  );
}

function Stat({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <div>
      <div style={{ fontSize: 24, fontWeight: 700, color, fontVariantNumeric: 'tabular-nums' }}>{value}</div>
      <div style={{ fontSize: 12, color: colors.textSecondary }}>{label}</div>
    </div>
  );
}
