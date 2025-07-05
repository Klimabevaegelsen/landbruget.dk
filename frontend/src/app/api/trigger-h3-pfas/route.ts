import { NextRequest, NextResponse } from 'next/server';

interface PipelineConfig {
  years: string;
  modes: string;
  h3_resolutions: string;
  memory_limit: string;
  thread_count: string;
  chunk_size: string;
}

export async function POST(request: NextRequest) {
  try {
    const config: PipelineConfig = await request.json();

    // GitHub repository details
    const owner = 'Klimabevaegelsen';
    const repo = 'landbruget.dk';
    const workflow_id = 'h3-pfas-analysis.yml';

    // GitHub API endpoint for workflow dispatch
    const url = `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflow_id}/dispatches`;

    // Prepare the workflow inputs
    const workflowInputs = {
      years: config.years,
      modes: config.modes,
      h3_resolutions: config.h3_resolutions,
      memory_limit: config.memory_limit,
      thread_count: config.thread_count,
      chunk_size: config.chunk_size,
    };

    // Make the GitHub API call
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Accept': 'application/vnd.github.v3+json',
        'Authorization': `Bearer ${process.env.GITHUB_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        ref: 'main', // Branch to run the workflow on
        inputs: workflowInputs,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error('GitHub API Error:', response.status, errorText);
      return NextResponse.json(
        { error: 'Failed to trigger workflow', details: errorText },
        { status: response.status }
      );
    }

    return NextResponse.json({
      success: true,
      message: 'H3 PFAS pipeline triggered successfully',
      config: workflowInputs,
    });

  } catch (error) {
    console.error('Error triggering H3 PFAS pipeline:', error);
    return NextResponse.json(
      { error: 'Internal server error', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
} 