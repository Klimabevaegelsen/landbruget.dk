/* oxlint-disable landbruget/no-inline-styles */
import { ImageResponse } from 'next/og';
import type { NextRequest } from 'next/server';

export const runtime = 'edge';

const GRADE_COLORS: Record<string, string> = {
  A: '#2d9a6e',
  B: '#5aa85e',
  C: '#c9a030',
  D: '#c06a28',
  E: '#b83a2a',
};

export async function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const grade = searchParams.get('grade') ?? 'C';
  const addr = searchParams.get('addr') ?? 'Din adresse';
  const fields = searchParams.get('fields') ?? '0';
  const pfas = searchParams.get('pfas') ?? '0';
  const dist = searchParams.get('dist') ?? '0';
  const color = GRADE_COLORS[grade] ?? GRADE_COLORS.C;

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
            alignItems: 'center',
            justifyContent: 'center',
            width: '160px',
            height: '160px',
            borderRadius: '24px',
            backgroundColor: color,
            color: '#fff',
            fontSize: '96px',
            fontWeight: 700,
            lineHeight: 1,
          }}
        >
          {grade}
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
