import { useState, useCallback, useRef, useEffect } from 'react';
import { AppView, SectionResult, Recommendation, HealthCheckResults, SECTION_ORDER, SECTION_META } from './types';
import Layout from './components/Layout';
import SetupScreen from './components/SetupScreen';
import Dashboard from './components/Dashboard';
import SectionDetail from './components/SectionDetail';
import ProgressBar from './components/ProgressBar';
import { colors } from './styles/theme';

interface RunState {
  running: boolean;
  sectionIndex: number;
  totalSections: number;
  currentSection: string;
  completedSections: Map<string, any>;
}

export default function App() {
  const [view, setView] = useState<AppView>('setup');
  const [selectedSection, setSelectedSection] = useState<string | null>(null);
  const [results, setResults] = useState<HealthCheckResults | null>(null);
  const [runState, setRunState] = useState<RunState>({
    running: false, sectionIndex: 0, totalSections: 13,
    currentSection: '', completedSections: new Map(),
  });
  const [error, setError] = useState<string | null>(null);
  const eventSourceRef = useRef<EventSource | null>(null);

  const startHealthCheck = useCallback(async (warehouseId: string, includeTableAnalysis: boolean) => {
    setError(null);
    setView('running');
    setRunState({ running: true, sectionIndex: 0, totalSections: 13, currentSection: '', completedSections: new Map() });

    try {
      const res = await fetch('/api/health-check/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ warehouse_id: warehouseId, include_table_analysis: includeTableAnalysis }),
      });
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.error || 'Failed to start health check');
      }
    } catch (e: any) {
      setError(e.message);
      setView('setup');
      return;
    }

    // Connect to SSE stream
    const es = new EventSource('/api/health-check/stream');
    eventSourceRef.current = es;

    es.onmessage = (event) => {
      const data = JSON.parse(event.data);

      if (data.type === 'progress') {
        setRunState(prev => ({
          ...prev,
          sectionIndex: data.section_index || prev.sectionIndex,
          totalSections: data.total_sections || prev.totalSections,
          currentSection: data.section_name || prev.currentSection,
        }));
      } else if (data.type === 'section_complete') {
        setRunState(prev => {
          const updated = new Map(prev.completedSections);
          updated.set(data.section, {
            section_id: data.section,
            section_name: data.section_name,
            score: data.score,
            active: data.active,
            issues_count: data.issues_count,
            checks: data.checks || [],
          });
          return { ...prev, completedSections: updated, sectionIndex: data.section_index || prev.sectionIndex };
        });
      } else if (data.type === 'complete') {
        es.close();
        // Fetch full results
        fetch('/api/health-check/results')
          .then(r => r.json())
          .then(fullResults => {
            // Map backend format to frontend HealthCheckResults
            const mapped: HealthCheckResults = {
              overall_score: fullResults.overall?.overall_score ?? 0,
              score_label: fullResults.overall?.label ?? 'Critical',
              sections: (fullResults.sections || []).map((s: any) => ({
                section_id: s.section_id,
                section_key: s.section_id,
                name: s.section_name,
                description: SECTION_META[s.section_id]?.description || '',
                icon: s.icon || SECTION_META[s.section_id]?.icon || 'circle',
                section_type: s.section_type || 'core',
                active: s.active,
                score: s.score,
                checks: s.checks || [],
                subsections: s.subsections || [],
              })),
              top_recommendations: fullResults.top_recommendations || [],
              run_timestamp: fullResults.timestamp || new Date().toISOString(),
              duration_seconds: 0,
            };
            setResults(mapped);
            setRunState(prev => ({ ...prev, running: false }));
            setView('dashboard');
          });
      } else if (data.type === 'error') {
        es.close();
        setError(data.message);
        setRunState(prev => ({ ...prev, running: false }));
        setView('setup');
      } else if (data.type === 'done') {
        es.close();
      }
    };

    es.onerror = () => {
      es.close();
      setRunState(prev => ({ ...prev, running: false }));
      // Try fetching results anyway
      fetch('/api/health-check/results')
        .then(r => { if (r.ok) return r.json(); throw new Error('No results'); })
        .then(fullResults => {
          const mapped: HealthCheckResults = {
            overall_score: fullResults.overall?.overall_score ?? 0,
            score_label: fullResults.overall?.label ?? 'Critical',
            sections: (fullResults.sections || []).map((s: any) => ({
              section_id: s.section_id, section_key: s.section_id,
              name: s.section_name,
              description: SECTION_META[s.section_id]?.description || '',
              icon: s.icon || SECTION_META[s.section_id]?.icon || 'circle',
              section_type: s.section_type || 'core',
              active: s.active, score: s.score,
              checks: s.checks || [], subsections: s.subsections || [],
            })),
            top_recommendations: fullResults.top_recommendations || [],
            run_timestamp: fullResults.timestamp || new Date().toISOString(),
            duration_seconds: 0,
          };
          setResults(mapped);
          setView('dashboard');
        })
        .catch(() => {
          setError('Connection lost. Please try again.');
          setView('setup');
        });
    };
  }, []);

  const handleSectionClick = (sectionId: string) => {
    setSelectedSection(sectionId);
    setView('section');
  };

  const handleBackToDashboard = () => {
    setSelectedSection(null);
    setView('dashboard');
  };

  const handleExport = () => {
    window.open('/api/health-check/export', '_blank');
  };

  return (
    <Layout
      view={view}
      results={results}
      selectedSection={selectedSection}
      onSectionClick={handleSectionClick}
      onBackToDashboard={handleBackToDashboard}
    >
      {view === 'setup' && (
        <SetupScreen onStart={startHealthCheck} error={error} />
      )}
      {view === 'running' && (
        <div style={{ padding: 32 }}>
          <ProgressBar
            sectionIndex={runState.sectionIndex}
            totalSections={runState.totalSections}
            currentSection={runState.currentSection}
            status="running"
          />
          <div style={{ marginTop: 48, display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: 16 }}>
            {SECTION_ORDER.map((key) => {
              const meta = SECTION_META[key];
              const completed = runState.completedSections.get(key);
              return (
                <div key={key} style={{
                  padding: 20, borderRadius: 8, border: `1px solid ${colors.border}`,
                  background: completed ? colors.white : colors.surface,
                  opacity: completed ? 1 : 0.5,
                  transition: 'all 0.3s ease',
                  transform: completed ? 'scale(1)' : 'scale(0.98)',
                }}>
                  <div style={{ fontSize: 14, fontWeight: 600, color: colors.textPrimary }}>{meta.name}</div>
                  {completed ? (
                    <div style={{
                      marginTop: 8, fontSize: 24, fontWeight: 600,
                      color: completed.active ? (completed.score >= 90 ? '#059669' : completed.score >= 70 ? '#2563EB' : completed.score >= 50 ? '#D97706' : '#DC2626') : colors.textTertiary,
                    }}>
                      {completed.active ? `${Math.round(completed.score)}` : 'N/A'}
                    </div>
                  ) : (
                    <div style={{ marginTop: 8, height: 24, background: colors.surface, borderRadius: 4 }} />
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}
      {view === 'dashboard' && results && (
        <Dashboard results={results} onSectionClick={handleSectionClick} onExport={handleExport} />
      )}
      {view === 'section' && results && selectedSection && (
        <SectionDetail
          section={results.sections.find(s => s.section_id === selectedSection)!}
          onBack={handleBackToDashboard}
        />
      )}
    </Layout>
  );
}
