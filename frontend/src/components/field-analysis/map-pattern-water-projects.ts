import { MapInstance } from './map-constants';

export async function createWaterProjectsPattern(
  map: MapInstance
): Promise<void> {
  try {
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');
    canvas.width = 20;
    canvas.height = 20;

    if (!ctx) return;

    ctx.fillStyle = '#14B8A6';
    ctx.fillRect(0, 0, 20, 20);

    ctx.fillStyle = 'rgba(255, 255, 255, 0.6)';
    for (let x = 3; x < 20; x += 6) {
      for (let y = 3; y < 20; y += 6) {
        ctx.beginPath();
        ctx.arc(x, y, 2, 0, 2 * Math.PI);
        ctx.fill();
      }
    }

    const imageBitmap = await createImageBitmap(canvas);
    if (!map.hasImage('water-projects-pattern')) {
      map.addImage('water-projects-pattern', imageBitmap);
    }

    if (map.getLayer('water-projects-fill')) {
      map.setPaintProperty(
        'water-projects-fill',
        'fill-pattern',
        'water-projects-pattern'
      );
      map.setPaintProperty('water-projects-fill', 'fill-opacity', 0.5);
    }
  } catch (error) {
    console.warn('Failed to create water projects pattern:', error);
  }
}
