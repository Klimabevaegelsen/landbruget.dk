import { GoogleGenerativeAI } from '@google/generative-ai';
import { NextRequest, NextResponse } from 'next/server';
import { catalog, VISUALIZATION_RULES } from '@/lib/render/catalog';

const genAI = new GoogleGenerativeAI(process.env.GOOGLE_API_KEY || '');

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { question, sql, explanation, results } = body as {
      question: string;
      sql: string;
      explanation: string;
      results: Record<string, unknown>[];
    };

    if (!results || results.length === 0) {
      return NextResponse.json(
        { error: 'No results to visualize' },
        { status: 400 }
      );
    }

    // Build data summary for the AI (don't send all rows)
    const columnNames = Object.keys(results[0]);
    const columnTypes = Object.entries(results[0]).map(([name, value]) => ({
      name,
      type: typeof value,
    }));
    const sampleRows = results.slice(0, 10);
    const rowCount = results.length;

    // Generate system prompt from catalog
    const systemPrompt = catalog.prompt({
      customRules: VISUALIZATION_RULES,
    });

    // Build user prompt
    const userPrompt = `Generate a json-render UI spec to visualize these SQL query results.

QUESTION: "${question}"
SQL: ${sql}
EXPLANATION: ${explanation}

RESULT SUMMARY:
- Row count: ${rowCount}
- Columns: ${columnTypes.map((c) => `${c.name} (${c.type})`).join(', ')}

SAMPLE DATA (first ${sampleRows.length} rows):
${JSON.stringify(sampleRows, null, 2)}

IMPORTANT:
- Use "$DATA" as the value for any data/items arrays in chart, table, or map components
- Column names must exactly match: ${columnNames.join(', ')}
- Output ONLY valid JSON matching the spec format. No markdown, no code fences.

Remember: Output /root first, then /elements patches so the UI fills in progressively.`;

    const model = genAI.getGenerativeModel({
      model: 'gemini-flash-latest',
      systemInstruction: systemPrompt,
      generationConfig: {
        temperature: 0.3,
        responseMimeType: 'application/json',
      },
    });

    const result = await model.generateContent(userPrompt);

    const responseText = result.response.text();

    // Parse the spec
    let spec;
    try {
      spec = JSON.parse(responseText);
    } catch {
      // Try to extract JSON from the response
      const jsonMatch = responseText.match(/\{[\s\S]*\}/);
      if (jsonMatch) {
        spec = JSON.parse(jsonMatch[0]);
      } else {
        throw new Error('Failed to parse visualization spec');
      }
    }

    // Validate basic spec structure
    if (!spec.root || !spec.elements) {
      throw new Error('Invalid spec: missing root or elements');
    }

    return NextResponse.json({ spec });
  } catch (err) {
    console.error('Visualization generation failed:', err);
    return NextResponse.json(
      {
        error: 'Failed to generate visualization',
        details: err instanceof Error ? err.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}
