import { useEffect, useState } from 'react';
import { getScoreColor, getScoreLabel } from '../types';

interface ScoreGaugeProps {
  score: number;
  size?: 'sm' | 'md' | 'lg';
  showLabel?: boolean;
  animate?: boolean;
}

const SIZES = {
  sm: { width: 64, stroke: 5, fontSize: 16, labelSize: 9 },
  md: { width: 100, stroke: 7, fontSize: 28, labelSize: 12 },
  lg: { width: 180, stroke: 10, fontSize: 48, labelSize: 16 },
};

export default function ScoreGauge({ score, size = 'md', showLabel = true, animate = true }: ScoreGaugeProps) {
  const [displayed, setDisplayed] = useState(animate ? 0 : score);
  const cfg = SIZES[size];
  const radius = (cfg.width - cfg.stroke) / 2;
  const circumference = 2 * Math.PI * radius;
  const arc = 0.75; // 270 degree arc
  const arcLen = circumference * arc;
  const offset = arcLen - (displayed / 100) * arcLen;
  const color = getScoreColor(displayed);
  const label = getScoreLabel(displayed);

  useEffect(() => {
    if (!animate) { setDisplayed(score); return; }
    let frame: number;
    const start = performance.now();
    const duration = 800;
    const from = 0;
    const tick = (now: number) => {
      const elapsed = now - start;
      const progress = Math.min(elapsed / duration, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      setDisplayed(Math.round(from + (score - from) * eased));
      if (progress < 1) frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  }, [score, animate]);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 4 }}>
      <svg width={cfg.width} height={cfg.width} viewBox={`0 0 ${cfg.width} ${cfg.width}`}>
        {/* Background arc */}
        <circle
          cx={cfg.width / 2} cy={cfg.width / 2} r={radius}
          fill="none" stroke="#E5E7EB" strokeWidth={cfg.stroke}
          strokeDasharray={`${arcLen} ${circumference}`}
          strokeDashoffset={0}
          strokeLinecap="round"
          transform={`rotate(135 ${cfg.width / 2} ${cfg.width / 2})`}
        />
        {/* Score arc */}
        <circle
          cx={cfg.width / 2} cy={cfg.width / 2} r={radius}
          fill="none" stroke={color} strokeWidth={cfg.stroke}
          strokeDasharray={`${arcLen} ${circumference}`}
          strokeDashoffset={offset}
          strokeLinecap="round"
          transform={`rotate(135 ${cfg.width / 2} ${cfg.width / 2})`}
          style={{ transition: 'stroke-dashoffset 0.8s ease-out, stroke 0.3s ease' }}
        />
        {/* Score number */}
        <text
          x={cfg.width / 2} y={cfg.width / 2 + cfg.fontSize * 0.1}
          textAnchor="middle" dominantBaseline="middle"
          fill={color}
          style={{ fontSize: cfg.fontSize, fontWeight: 600, fontFamily: 'Inter, sans-serif', fontFeatureSettings: '"tnum"' }}
        >
          {displayed}
        </text>
      </svg>
      {showLabel && (
        <span style={{
          fontSize: cfg.labelSize,
          fontWeight: 600,
          color,
          textTransform: 'uppercase',
          letterSpacing: '0.05em',
        }}>
          {label}
        </span>
      )}
    </div>
  );
}
