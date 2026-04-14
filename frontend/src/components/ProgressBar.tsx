import { colors } from '../styles/theme';

interface ProgressBarProps {
  sectionIndex: number;
  totalSections: number;
  currentSection: string;
  status: string;
}

export default function ProgressBar({ sectionIndex, totalSections, currentSection, status }: ProgressBarProps) {
  const pct = totalSections > 0 ? ((sectionIndex) / totalSections) * 100 : 0;

  return (
    <div style={{
      position: 'sticky',
      top: 0,
      zIndex: 100,
      background: colors.white,
      borderBottom: `1px solid ${colors.border}`,
      padding: '12px 24px',
    }}>
      {/* Thin progress bar */}
      <div style={{
        height: 4,
        background: colors.borderLight,
        borderRadius: 2,
        overflow: 'hidden',
        marginBottom: 8,
      }}>
        <div style={{
          height: '100%',
          width: `${pct}%`,
          background: `linear-gradient(90deg, ${colors.primary}, ${colors.darkSecondary})`,
          borderRadius: 2,
          transition: 'width 0.6s ease-out',
        }} />
      </div>
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        fontSize: 13,
        color: colors.textSecondary,
      }}>
        <span>
          {status === 'running' ? (
            <>
              <span style={{ fontWeight: 600, color: colors.textPrimary }}>
                Section {sectionIndex} of {totalSections}
              </span>
              {': '}
              <span style={{ color: colors.darkSecondary }}>{currentSection}</span>
            </>
          ) : (
            <span style={{ fontWeight: 600, color: colors.excellent }}>Health check complete</span>
          )}
        </span>
        <span style={{ fontFeatureSettings: '"tnum"' }}>
          {Math.round(pct)}%
        </span>
      </div>
    </div>
  );
}
