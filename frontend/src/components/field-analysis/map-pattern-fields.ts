import { MapInstance } from './map-constants';

export async function createPartialCoveragePattern(
  map: MapInstance,
  color: string = '#374151'
): Promise<void> {
  try {
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');
    canvas.width = 32;
    canvas.height = 32;

    if (!ctx) return;

    ctx.clearRect(0, 0, 32, 32);
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.globalAlpha = 0.9;
    ctx.beginPath();

    for (let i = -32; i <= 64; i += 6) {
      ctx.moveTo(i, 0);
      ctx.lineTo(i + 32, 32);
    }
    ctx.stroke();

    const bitmap = await createImageBitmap(canvas);
    if (!map.hasImage('partial-coverage-pattern')) {
      map.addImage('partial-coverage-pattern', bitmap);
    }
  } catch (error) {
    console.warn('Failed to create partial coverage pattern:', error);
  }
}
