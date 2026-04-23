/* oxlint-disable landbruget/no-inline-styles */
import { ImageResponse } from 'next/og';
import type { NextRequest } from 'next/server';
import {
  GRADE_DEFINITIONS,
  getGradeHexColor,
  isPesticideGrade,
} from '@/lib/pesticide-score';
import type { PesticideGrade } from '@/components/pesticidkort/types';

export const runtime = 'edge';

const DEFAULT_ACCENT_COLOR = '#57534e';
const DEFAULT_GRADE_META = {
  label: 'Pesticideksponering',
  description: 'Modelleret estimat baseret på offentlige data',
};

export function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const rawGrade = searchParams.get('grade');
  const grade: PesticideGrade | null =
    rawGrade && isPesticideGrade(rawGrade) ? rawGrade : null;
  const addr = searchParams.get('addr') ?? 'Din adresse';
  const fields = searchParams.get('fields') ?? '0';
  const pfas = searchParams.get('pfas') ?? '0';
  const dist = searchParams.get('dist') ?? '0';
  const color = grade ? getGradeHexColor(grade) : DEFAULT_ACCENT_COLOR;
  const gradeMeta = grade ? GRADE_DEFINITIONS[grade] : DEFAULT_GRADE_META;

  return new ImageResponse(
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
              color: color,
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
              fontSize: grade ? '34px' : '38px',
              fontWeight: 700,
              color: '#1c1917',
              lineHeight: 1.15,
              marginBottom: '8px',
            }}
          >
            {gradeMeta.label}
          </div>
          <div
            style={{
              fontSize: '16px',
              color: '#57534e',
              lineHeight: 1.3,
            }}
          >
            {gradeMeta.description}
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
        <div style={{ display: 'flex', flexDirection: 'column' }}>
          <div style={{ fontSize: '40px', fontWeight: 700, color: '#1c1917' }}>
            {fields}
          </div>
          <div style={{ fontSize: '18px', color: '#78716c' }}>
            marker i 1 km
          </div>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column' }}>
          <div style={{ fontSize: '40px', fontWeight: 700, color: '#1c1917' }}>
            {pfas}
          </div>
          <div style={{ fontSize: '18px', color: '#78716c' }}>med PFAS</div>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column' }}>
          <div style={{ fontSize: '40px', fontWeight: 700, color: '#1c1917' }}>
            {dist}m
          </div>
          <div style={{ fontSize: '18px', color: '#78716c' }}>
            nærmeste mark
          </div>
        </div>
      </div>
    </div>,
    { width: 1200, height: 630 }
  );
}
