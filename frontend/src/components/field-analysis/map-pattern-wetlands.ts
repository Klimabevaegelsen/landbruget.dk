import { MapInstance } from './map-constants';

export async function createWetlandsPattern(map: MapInstance): Promise<void> {
  try {
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');
    canvas.width = 24;
    canvas.height = 16;

    if (!ctx) return;

    ctx.fillStyle = '#3B82F6';
    ctx.fillRect(0, 0, 24, 16);

    ctx.strokeStyle = 'rgba(255, 255, 255, 0.5)';
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.moveTo(0, 4);
    ctx.quadraticCurveTo(6, 2, 12, 4);
    ctx.quadraticCurveTo(18, 6, 24, 4);
    ctx.moveTo(0, 8);
    ctx.quadraticCurveTo(6, 6, 12, 8);
    ctx.quadraticCurveTo(18, 10, 24, 8);
    ctx.moveTo(0, 12);
    ctx.quadraticCurveTo(6, 10, 12, 12);
    ctx.quadraticCurveTo(18, 14, 24, 12);
    ctx.stroke();

    const imageBitmap = await createImageBitmap(canvas);
    if (!map.hasImage('wetlands-pattern')) {
      map.addImage('wetlands-pattern', imageBitmap);
    }

    if (map.getLayer('wetlands-fill')) {
      map.setPaintProperty('wetlands-fill', 'fill-pattern', 'wetlands-pattern');
      map.setPaintProperty('wetlands-fill', 'fill-opacity', 0.4);
    }
  } catch (error) {
    console.warn('Failed to create wetlands pattern:', error);
  }
}
