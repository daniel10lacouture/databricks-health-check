import { useState, useEffect } from 'react';
import { colors, shadows, radii } from '../styles/theme';
import { Activity, Zap, Shield, DollarSign } from 'lucide-react';

interface Warehouse {
  id: string;
  name: string;
  state: string;
  cluster_size: string;
  warehouse_type: string;
  enable_serverless_compute: boolean;
}

interface Props {
  onStart: (warehouseId: string, includeTableAnalysis: boolean) => void;
  error: string | null;
}

export default function SetupScreen({ onStart, error }: Props) {
  const [warehouses, setWarehouses] = useState<Warehouse[]>([]);
  const [selectedWarehouse, setSelectedWarehouse] = useState('');
  const [includeTableAnalysis, setIncludeTableAnalysis] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('/api/warehouses')
      .then(r => r.json())
      .then(data => {
        setWarehouses(Array.isArray(data) ? data : []);
        if (Array.isArray(data) && data.length > 0) {
          setSelectedWarehouse(data[0].id);
        }
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  return (
    <div style={{
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      minHeight: '100vh', padding: 32, background: `linear-gradient(135deg, ${colors.surface} 0%, ${colors.white} 100%)`,
    }}>
      <div style={{ maxWidth: 520, width: '100%' }}>
        {/* Header */}
        <div style={{ textAlign: 'center', marginBottom: 40 }}>
          <div style={{
            width: 64, height: 64, borderRadius: 16, background: colors.primary,
            display: 'inline-flex', alignItems: 'center', justifyContent: 'center', marginBottom: 20,
          }}>
            <Activity size={32} color="white" />
          </div>
          <h1 style={{ fontSize: 28, fontWeight: 700, color: colors.textPrimary, margin: '0 0 8px' }}>
            Account Health Check
          </h1>
          <p style={{ fontSize: 15, color: colors.textSecondary, lineHeight: 1.5 }}>
            Analyze your Databricks account configuration, usage patterns, and best practice adherence across 171 checks in 13 categories.
          </p>
        </div>

        {/* Feature pills */}
        <div style={{ display: 'flex', gap: 12, justifyContent: 'center', marginBottom: 32, flexWrap: 'wrap' }}>
          {[
            { icon: <Zap size={14} />, label: '171 Checks' },
            { icon: <Shield size={14} />, label: '13 Categories' },
            { icon: <DollarSign size={14} />, label: 'Cost Insights' },
          ].map((f, i) => (
            <div key={i} style={{
              display: 'flex', alignItems: 'center', gap: 6, padding: '6px 14px',
              borderRadius: 20, background: colors.white, border: `1px solid ${colors.border}`,
              fontSize: 13, color: colors.textSecondary,
            }}>
              {f.icon} {f.label}
            </div>
          ))}
        </div>

        {/* Card */}
        <div style={{
          background: colors.white, borderRadius: radii.lg, padding: 32,
          boxShadow: shadows.md, border: `1px solid ${colors.border}`,
        }}>
          {/* Warehouse selector */}
          <label style={{ display: 'block', fontSize: 13, fontWeight: 600, color: colors.textPrimary, marginBottom: 8 }}>
            SQL Warehouse
          </label>
          <select
            value={selectedWarehouse}
            onChange={e => setSelectedWarehouse(e.target.value)}
            disabled={loading}
            style={{
              width: '100%', padding: '10px 12px', borderRadius: radii.md,
              border: `1px solid ${colors.border}`, fontSize: 14,
              background: colors.white, color: colors.textPrimary,
              cursor: 'pointer', outline: 'none',
            }}
          >
            {loading ? (
              <option>Loading warehouses...</option>
            ) : warehouses.length === 0 ? (
              <option>No warehouses available</option>
            ) : (
              warehouses.map(w => (
                <option key={w.id} value={w.id}>
                  {w.name} ({w.cluster_size}) — {w.enable_serverless_compute ? 'Serverless' : w.warehouse_type}
                  {w.state !== 'RUNNING' ? ` [${w.state}]` : ''}
                </option>
              ))
            )}
          </select>

          {/* Table analysis toggle */}
          <div style={{ marginTop: 20, display: 'flex', alignItems: 'center', gap: 10 }}>
            <input
              type="checkbox" id="tableAnalysis"
              checked={includeTableAnalysis}
              onChange={e => setIncludeTableAnalysis(e.target.checked)}
              style={{ width: 16, height: 16, cursor: 'pointer' }}
            />
            <label htmlFor="tableAnalysis" style={{ fontSize: 13, color: colors.textSecondary, cursor: 'pointer' }}>
              Include table-level analysis <span style={{ opacity: 0.6 }}>(slower, recommended for first run)</span>
            </label>
          </div>

          {/* Error message */}
          {error && (
            <div style={{
              marginTop: 16, padding: '10px 14px', borderRadius: radii.md,
              background: '#FEF2F2', border: '1px solid #FECACA', color: '#DC2626', fontSize: 13,
            }}>
              {error}
            </div>
          )}

          {/* Run button */}
          <button
            onClick={() => selectedWarehouse && onStart(selectedWarehouse, includeTableAnalysis)}
            disabled={!selectedWarehouse || loading}
            style={{
              width: '100%', marginTop: 24, padding: '14px 24px',
              borderRadius: radii.md, border: 'none', fontSize: 15, fontWeight: 600,
              background: selectedWarehouse && !loading ? colors.primary : colors.border,
              color: selectedWarehouse && !loading ? 'white' : colors.textTertiary,
              cursor: selectedWarehouse && !loading ? 'pointer' : 'not-allowed',
              transition: 'all 0.2s ease',
            }}
            onMouseEnter={e => { if (selectedWarehouse) e.currentTarget.style.background = '#E02E1A' }}
            onMouseLeave={e => { if (selectedWarehouse) e.currentTarget.style.background = colors.primary }}
          >
            Run Health Check
          </button>

          <p style={{ marginTop: 12, fontSize: 12, color: colors.textTertiary, textAlign: 'center' }}>
            Typical run: 2–5 minutes
          </p>
        </div>
      </div>
    </div>
  );
}
