import { ReactNode } from 'react';
import { AppView, HealthCheckResults, SECTION_ORDER, SECTION_META, getScoreColor } from '../types';
import { colors, shadows } from '../styles/theme';
import ScoreGauge from './ScoreGauge';
import * as Icons from 'lucide-react';

interface Props {
  view: AppView;
  results: HealthCheckResults | null;
  selectedSection: string | null;
  onSectionClick: (id: string) => void;
  onBackToDashboard: () => void;
  children: ReactNode;
}

function getIcon(name: string, size = 18) {
  const iconMap: Record<string, any> = {
    'database': Icons.Database, 'bar-chart-3': Icons.BarChart3, 'cpu': Icons.Cpu,
    'dollar-sign': Icons.DollarSign, 'shield': Icons.Shield, 'lock': Icons.Lock,
    'brain': Icons.Brain, 'layout-grid': Icons.LayoutGrid, 'server': Icons.Server,
    'share-2': Icons.Share2, 'download': Icons.Download, 'settings': Icons.Settings,
    'git-branch': Icons.GitBranch, 'circle': Icons.Circle, 'layers': Icons.Layers,
    'app-window': Icons.AppWindow,
  };
  const Icon = iconMap[name] || Icons.Circle;
  return <Icon size={size} />;
}

export default function Layout({ view, results, selectedSection, onSectionClick, onBackToDashboard, children }: Props) {
  const showSidebar = view === 'dashboard' || view === 'section';

  return (
    <div style={{ display: 'flex', height: '100vh', fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}>
      {/* Sidebar */}
      {showSidebar && (
        <nav style={{
          width: 280, minWidth: 280, background: colors.dark, color: colors.textInverse,
          display: 'flex', flexDirection: 'column', overflow: 'hidden',
        }}>
          {/* Logo area */}
          <div style={{ padding: '20px 20px 16px', borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer' }} onClick={onBackToDashboard}>
              <div style={{
                width: 32, height: 32, borderRadius: 8, background: colors.primary,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}>
                <Icons.Activity size={18} color="white" />
              </div>
              <div>
                <div style={{ fontSize: 14, fontWeight: 600 }}>Health Check</div>
                <div style={{ fontSize: 11, opacity: 0.6 }}>Account Diagnostics</div>
              </div>
            </div>
            {results && results.overall_score != null && (
              <div style={{ marginTop: 16, display: 'flex', alignItems: 'center', gap: 12 }}>
                <ScoreGauge score={results.overall_score} size="sm" />
                <div>
                  <div style={{ fontSize: 12, opacity: 0.6 }}>Overall Score</div>
                  <div style={{ fontSize: 18, fontWeight: 600, color: getScoreColor(results.overall_score) }}>
                    {Math.round(results.overall_score)}
                  </div>
                </div>
              </div>
            )}
          </div>

          {/* Section list */}
          <div style={{ flex: 1, overflowY: 'auto', padding: '8px 0' }}>
            {SECTION_ORDER.map((key) => {
              const meta = SECTION_META[key];
              const section = results?.sections.find(s => s.section_id === key);
              const isSelected = selectedSection === key;
              const score = section?.active ? section?.score : null;

              return (
                <div
                  key={key}
                  onClick={() => onSectionClick(key)}
                  style={{
                    padding: '10px 20px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 12,
                    background: isSelected ? 'rgba(255,255,255,0.1)' : 'transparent',
                    borderLeft: isSelected ? `3px solid ${colors.primary}` : '3px solid transparent',
                    transition: 'all 0.15s ease',
                  }}
                  onMouseEnter={e => { if (!isSelected) e.currentTarget.style.background = 'rgba(255,255,255,0.05)' }}
                  onMouseLeave={e => { if (!isSelected) e.currentTarget.style.background = 'transparent' }}
                >
                  <div style={{ opacity: 0.7 }}>{getIcon(meta.icon, 16)}</div>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: 13, fontWeight: 500, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {meta.name}
                    </div>
                  </div>
                  {score != null ? (
                    <div style={{
                      fontSize: 12, fontWeight: 600, padding: '2px 8px', borderRadius: 10,
                      background: getScoreColor(score), color: 'white',
                    }}>
                      {Math.round(score)}
                    </div>
                  ) : section && !section.active ? (
                    <div style={{ fontSize: 10, opacity: 0.4 }}>N/A</div>
                  ) : null}
                </div>
              );
            })}
          </div>

          {/* Footer */}
          <div style={{ padding: 16, borderTop: '1px solid rgba(255,255,255,0.1)', fontSize: 11, opacity: 0.4 }}>
            {results?.run_timestamp ? `Last run: ${new Date(results.run_timestamp).toLocaleString()}` : 'Databricks Account Health Check'}
          </div>
        </nav>
      )}

      {/* Main content */}
      <main style={{ flex: 1, overflow: 'auto', background: view === 'setup' ? colors.white : colors.surface }}>
        {children}
      </main>
    </div>
  );
}

export { getIcon };
