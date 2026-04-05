import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionPfasReferences() {
  return (
    <section data-testid="section-pfas-references">
      <SectionHeader id="pfas-references" number="6" title="Referencer" />

      <ol className="list-decimal space-y-2 pl-6 text-sm">
        <li>
          GEUS (2024). Jupiter-databasen &ndash; nationale grundvandskemidata.{' '}
          <em>
            De Nationale Geologiske Unders&oslash;gelser for Danmark og
            Gr&oslash;nland.
          </em>
        </li>
        <li>
          Milj&oslash;styrelsen (2024). Grundvandskortl&aelig;gning (GRUKOS)
          &ndash; indvindingsoplande for almene vandforsyninger.
        </li>
        <li>
          Milj&oslash;styrelsen (2024). Bek&aelig;mpelsesmiddeldatabasen (BMD)
          &ndash; registrerede pesticidprodukter og aktivstoffer.
        </li>
        <li>
          Perkola, N. et al. (2023). Trifluoroacetic acid (TFA) in the
          groundwater: occurrence, sources, and environmental relevance.{' '}
          <em>Science of the Total Environment</em>, 874, 162370.
        </li>
        <li>
          Baken, K. A. et al. (2023). Per- and polyfluoroalkyl substances in
          Danish groundwater: widespread contamination from both legacy and
          emerging sources. <em>Environmental Science &amp; Technology</em>,
          57(32), 11736&ndash;11747.
        </li>
        <li>
          European Food Safety Authority (2020). Risk assessment of
          trifluoroacetic acid (TFA) as a metabolite of pesticides.{' '}
          <em>EFSA Journal</em>, 18(10), e06250.
        </li>
      </ol>
    </section>
  );
}
