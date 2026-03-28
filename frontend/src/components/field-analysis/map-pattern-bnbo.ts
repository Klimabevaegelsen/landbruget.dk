import { MapInstance } from './map-constants';

function createCanvas(
  size: number
): [HTMLCanvasElement, CanvasRenderingContext2D | null] {
  const canvas = document.createElement('canvas');
  canvas.width = size;
  canvas.height = size;
  return [canvas, canvas.getContext('2d')];
}

function drawCompletedPattern(ctx: CanvasRenderingContext2D) {
  ctx.fillStyle = '#10B981';
  ctx.fillRect(0, 0, 32, 32);
  ctx.strokeStyle = 'rgba(255, 255, 255, 0.3)';
  ctx.lineWidth = 2;
  ctx.beginPath();
  for (let i = -32; i <= 64; i += 8) {
    ctx.moveTo(i, 0);
    ctx.lineTo(i + 32, 32);
  }
  ctx.stroke();
}

function drawActionPattern(ctx: CanvasRenderingContext2D) {
  ctx.fillStyle = '#EAB308';
  ctx.fillRect(0, 0, 32, 32);
  ctx.strokeStyle = 'rgba(255, 255, 255, 0.4)';
  ctx.lineWidth = 2;
  ctx.beginPath();
  for (let i = -32; i <= 64; i += 8) {
    ctx.moveTo(i, 0);
    ctx.lineTo(i + 32, 32);
    ctx.moveTo(i + 32, 0);
    ctx.lineTo(i, 32);
  }
  ctx.stroke();
}

export async function createBNBOPatterns(map: MapInstance): Promise<void> {
  try {
    const [completedCanvas, completedCtx] = createCanvas(32);
    const [actionCanvas, actionCtx] = createCanvas(32);

    if (completedCtx) drawCompletedPattern(completedCtx);
    if (actionCtx) drawActionPattern(actionCtx);

    const [completedBitmap, actionBitmap] = await Promise.all([
      completedCtx ? createImageBitmap(completedCanvas) : Promise.resolve(null),
      actionCtx ? createImageBitmap(actionCanvas) : Promise.resolve(null),
    ]);

    if (completedBitmap && !map.hasImage('bnbo-completed-pattern')) {
      map.addImage('bnbo-completed-pattern', completedBitmap);
    }
    if (actionBitmap && !map.hasImage('bnbo-action-pattern')) {
      map.addImage('bnbo-action-pattern', actionBitmap);
    }
  } catch (error) {
    console.warn('Failed to create BNBO patterns:', error);
  }
}
