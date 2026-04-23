/* oxlint-disable landbruget/no-inline-styles */
import { resolveGradeMeta } from './pesticideOgMeta';

interface PesticideOgImageProps {
  grade: string | null;
  addr: string;
  fields: string;
  pfas: string;
  dist: string;
}

function StatBlock({ value, label }: { value: string; label: string }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column' }}>
      <div style={{ fontSize: '40px', fontWeight: 700, color: '#1c1917' }}>
        {value}
      </div>
      <div style={{ fontSize: '18px', color: '#78716c' }}>{label}</div>
    </div>
  );
}

export function PesticideOgImage({
  grade,
  addr,
  fields,
  pfas,
  dist,
}: PesticideOgImageProps) {
  const { color, meta, hasGrade } = resolveGradeMeta(grade);

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'center',
        width: '100%',
        height: '100%',
        backgroundColor: '#fafaf9',
        padding: '60px 80px',
        fontFamily: 'system-ui, sans-serif',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: '40px' }}>
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            justifyContent: 'center',
            width: '320px',
            height: '160px',
            borderRadius: '24px',
            backgroundColor: '#f5f5f4',
            border: `3px solid ${color}`,
            padding: '20px 24px',
          }}
        >
          <div
            style={{
              fontSize: '18px',
              color,
              marginBottom: '10px',
              textTransform: 'uppercase' as const,
              letterSpacing: '0.06em',
              fontWeight: 700,
            }}
          >
            Pesticideksponering
          </div>
          <div
            style={{
              fontSize: hasGrade ? '34px' : '38px',
              fontWeight: 700,
              color: '#1c1917',
              lineHeight: 1.15,
              marginBottom: '8px',
            }}
          >
            {meta.label}
          </div>
          <div style={{ fontSize: '16px', color: '#57534e', lineHeight: 1.3 }}>
            {meta.description}
          </div>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', flex: 1 }}>
          <div
            style={{
              fontSize: '20px',
              color: '#78716c',
              marginBottom: '8px',
              textTransform: 'uppercase' as const,
              letterSpacing: '0.05em',
            }}
          >
            Pesticidkortet
          </div>
          <div
            style={{
              fontSize: '36px',
              fontWeight: 700,
              color: '#1c1917',
              lineHeight: 1.2,
            }}
          >
            {addr}
          </div>
        </div>
      </div>

      <div
        style={{
          display: 'flex',
          gap: '48px',
          marginTop: '48px',
          borderTop: '1px solid #e7e5e4',
          paddingTop: '32px',
        }}
      >
        <StatBlock value={fields} label="marker i 1 km" />
        <StatBlock value={pfas} label="med PFAS" />
        <StatBlock value={`${dist}m`} label="nærmeste mark" />
      </div>
    </div>
  );
}
