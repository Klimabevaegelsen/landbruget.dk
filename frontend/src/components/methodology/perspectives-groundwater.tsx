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
        Da 100&nbsp;% af det danske drikkevand er grundvandsbaseret, er
        pesticidrester (som findes i 51&nbsp;% af overv&aring;gningsboringerne)
        et massivt problem. Data p&aring; markniveau g&oslash;r det muligt at
        overlejre spr&oslash;jtede arealer med det nationale
        grundvandsoverv&aring;gningsprogram (GRUMO) og de boringsn&aelig;re
        beskyttelsesomr&aring;der (BNBO) for at identificere risikozoner.
      </p>
      <p>
        <Link
          href="/pesticidanalyse/grundvand"
          data-testid="grundvand-methodology-link"
          className="text-primary font-medium underline-offset-4 hover:underline"
        >
          L&aelig;s den fulde metode for grundvandsanalysen &rarr;
        </Link>
      </p>
    </>
  );
}
