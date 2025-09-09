import { NextResponse } from 'next/server';

const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!;

export async function GET() {
  try {
    // Call the Supabase Edge Function
    const response = await fetch(
      `${SUPABASE_URL}/functions/v1/homepage-statistics`,
      {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
          'Content-Type': 'application/json',
        },
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      console.error('Supabase function error:', errorText);

      // Return fallback data based on our database analysis
      return NextResponse.json({
        total_data_points: 29104178,
        total_companies: 46126,
        last_updated: new Date().toISOString(),
        formatted: {
          data_points: '29.104.178',
          companies: '46.126',
        },
        fallback: true,
      });
    }

    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('API route error:', error);

    // Return fallback data based on our database analysis
    return NextResponse.json({
      total_data_points: 29104178,
      total_companies: 46126,
      last_updated: new Date().toISOString(),
      formatted: {
        data_points: '29.104.178',
        companies: '46.126',
      },
      fallback: true,
    });
  }
}
