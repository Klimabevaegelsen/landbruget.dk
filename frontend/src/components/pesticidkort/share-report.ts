import { toast } from 'sonner';
import type { PesticideReport } from '@/components/pesticidkort/types';

export function handleShare({ lat, lng, address, year }: PesticideReport) {
  const params = new URLSearchParams({
    lat: lat.toFixed(5),
    lng: lng.toFixed(5),
    addr: address,
    y: String(year),
  });
  navigator.clipboard.writeText(
    `${window.location.origin}/pesticidkort?${params}`
  );
  toast.success('Link kopieret', {
    description: 'Del linket, så andre kan tjekke deres egen adresse.',
  });
}
