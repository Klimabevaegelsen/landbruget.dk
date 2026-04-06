import Link from 'next/link';
import { SubsectionHeader } from '@/components/methodology/article-layout';

export function PerspectivesGroundwater() {
  return (
    <>
      <SubsectionHeader
        id="groundwater"
        number="4.1"
        title="Grundvand og drikkevand"
      />
      <p>
        Da 100 % af det danske drikkevand er grundvandsbaseret, er
        pesticidrester (som findes i 51 % af overvågningsboringerne) et massivt
        problem. Data på markniveau gør det muligt at overlejre sprøjtede
        arealer med det nationale grundvandsovervågningsprogram (GRUMO) og de
        boringsnære beskyttelsesområder (BNBO) for at identificere risikozoner.
      </p>
      <p>
        <Link
          href="/pesticidanalyse/grundvand"
          data-testid="grundvand-methodology-link"
          className="text-primary font-medium underline-offset-4 hover:underline"
        >
          Læs den fulde metode for grundvandsanalysen &rarr;
        </Link>
      </p>
    </>
  );
}
