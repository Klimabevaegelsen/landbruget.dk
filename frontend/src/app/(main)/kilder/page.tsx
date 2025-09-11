import { Container } from '@/components/layout/container';
import { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Kilder - Landbruget.dk',
  description:
    'Oversigt over datakilder og myndigheder der leverer data til Landbruget.dk.',
};

export default function KilderPage() {
  return (
    <div className="bg-background min-h-screen">
      <Container className="py-16 lg:py-24">
        <div className="mx-auto max-w-6xl">
          <article className="space-y-8">
            {/* Main Title */}
            <header className="space-y-6">
              <h1 className="text-primary text-5xl leading-tight font-black tracking-tight">
                Kilder
              </h1>
              <p className="text-muted-foreground text-xl leading-relaxed">
                Landbruget.dk indsamler og formidler data fra en række danske
                myndigheder og institutioner. Herunder finder du en oversigt
                over alle vores datakilder og de typer af data, vi modtager fra
                hver kilde.
              </p>
            </header>

            <hr className="border-primary/20 my-12" />

            {/* Data Sources Tables */}
            <section className="space-y-12">
              <h2 className="text-primary border-primary/20 border-b pb-4 text-3xl leading-tight font-bold">
                Datakilder og myndigheder
              </h2>
              <div className="overflow-x-auto">
                <table className="border-border w-full border-collapse border">
                  <thead>
                    <tr className="bg-primary/10">
                      <th className="text-primary border-border border px-4 py-3 text-left font-bold">
                        Myndighed eller institution
                      </th>
                      <th className="text-primary border-border border px-4 py-3 text-left font-bold">
                        Kilde
                      </th>
                      <th className="text-primary border-border border px-4 py-3 text-left font-bold">
                        Data
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Danmarks Meteorologiske Institut
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        API
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Vejr
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td
                        className="border-border text-foreground border px-4 py-3 font-medium"
                        rowSpan={2}
                      >
                        Styrelsen for Dataforsyning og Infrastruktur
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Datafordeleren (API)
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        <ul className="list-inside list-disc space-y-1">
                          <li>Administrative Geografiske Inddelinger</li>
                          <li>Matrikeloplysninger</li>
                        </ul>
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        BBR Bygninger
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Bygningsdata og -geometri
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Danmarks Statistik
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Danmarks Statistik (API)
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Landbrugsproduktion
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Naturstyrelsen
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        WFS API
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Miljø og klimaprojekter
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td
                        className="border-border text-foreground border px-4 py-3 font-medium"
                        rowSpan={2}
                      >
                        Styrelsen for Grøn Arealømlægning og Vandmiljø
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Aktindsigt
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Gødningsregnskab
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        WFS API
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        <ul className="list-inside list-disc space-y-1">
                          <li>Lavbundsjorde</li>
                          <li>Miljø og klimaprojekter</li>
                          <li>Tilskud</li>
                        </ul>
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Klimadatastyrelsen
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        API
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        <ul className="list-inside list-disc space-y-1">
                          <li>Skraafoto</li>
                          <li>Danmarks Højdemodel</li>
                        </ul>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>

              {/* Second Table */}
              <div className="mt-12 overflow-x-auto">
                <table className="border-border w-full border-collapse border">
                  <thead>
                    <tr className="bg-primary/10">
                      <th className="text-primary border-border border px-4 py-3 text-left font-bold">
                        Myndighed eller institution
                      </th>
                      <th className="text-primary border-border border px-4 py-3 text-left font-bold">
                        Kilde
                      </th>
                      <th className="text-primary border-border border px-4 py-3 text-left font-bold">
                        Data
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Beredskabsstyrelsen
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Aktindsigter
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        <ul className="list-inside list-disc space-y-1">
                          <li>Staldbrande</li>
                          <li>Gyllelæk</li>
                          <li>Dyretransporter i færdselsuheld</li>
                        </ul>
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td
                        className="border-border text-foreground border px-4 py-3 font-medium"
                        rowSpan={2}
                      >
                        Erhvervsstyrelsen
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        CVR-registret (API)
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        <ul className="list-inside list-disc space-y-1">
                          <li>Ledelse</li>
                          <li>Finansielle oplysninger</li>
                          <li>Medarbejderantal</li>
                          <li>Øvrige kerneoplysninger</li>
                        </ul>
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Aktindsigt
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Tilskud
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td
                        className="border-border text-foreground border px-4 py-3 font-medium"
                        rowSpan={2}
                      >
                        Energistyrelsen
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Emoweb (API)
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Energimærker
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Aktindsigt
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Biogasanlæg
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td
                        className="border-border text-foreground border px-4 py-3 font-medium"
                        rowSpan={4}
                      >
                        Miljøstyrelsen
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Digital Miljøadministration
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Miljøgodkendelser
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Bekæmpelsesmiddel-databasen
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Pesticidoplysninger
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Aktindsigt
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Pesticidforbrug
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        WFS API
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        <ul className="list-inside list-disc space-y-1">
                          <li>Markoplysninger & markblokke</li>
                          <li>Boringsnære beskyttelsesområder</li>
                          <li>Miljø og klimaprojekter</li>
                        </ul>
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td
                        className="border-border text-foreground border px-4 py-3 font-medium"
                        rowSpan={2}
                      >
                        Fødevarestyrelsen
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Det Centrale Husdyrbrugsregister (API)
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        <ul className="list-inside list-disc space-y-1">
                          <li>Antibiotikaforbrug</li>
                          <li>Dyretransporter</li>
                          <li>Veterinære hændelser</li>
                          <li>Øvrige kerneoplysninger om bedrifter</li>
                        </ul>
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Aktindsigt
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        <ul className="list-inside list-disc space-y-1">
                          <li>Internationale dyretransport</li>
                          <li>Tilskud</li>
                        </ul>
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Landbrug & Fødevarer
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        SPF-Sund hjemmeside
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Veterinær status
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Aarhus Universitet, DCE
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Rapport
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Kvælstofmodellering
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Københavns Universitet, IFRO
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Rapport
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Emissionsfaktorer for klimaregnskab
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        SEGES & Økologisk Landsforening
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Aktindsigt
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Klimaregnskabsmodellering
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Vejdirektoratet
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        ArcGIS (API)
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Markblokke
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Styrelsen for International Rekruttering og Integration
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Aktindsigt
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Førstegangsvisumanansøgninger
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Arbejdstilsynet
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Hjemmeside
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Arbejdspladsstilsyn
                      </td>
                    </tr>
                    <tr className="hover:bg-muted/50">
                      <td className="border-border text-foreground border px-4 py-3">
                        Arbejdsmarkedets Erhvervssikring
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Aktindsigt
                      </td>
                      <td className="border-border text-foreground border px-4 py-3">
                        Arbejdsskader
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </section>

            {/* Additional Information */}
            <section className="space-y-8">
              <h2 className="text-primary border-primary/20 mt-16 border-b pb-4 text-3xl leading-tight font-bold">
                Om vores dataindsamling
              </h2>
              <p className="text-foreground text-lg leading-relaxed">
                Alle data på Landbruget.dk stammer fra offentligt tilgængelige
                kilder. Vi indsamler og behandler data fra disse myndigheder og
                institutioner for at skabe et samlet overblik over den danske
                landbrugssektor. Dataene opdateres løbende efter de enkelte
                kilders publiceringsrytme.
              </p>

              <div className="border-conventional/20 bg-conventional/10 rounded-lg border p-6">
                <h3 className="text-primary-darker mb-3 text-xl font-bold">
                  Vigtigt at vide
                </h3>
                <p className="text-foreground leading-relaxed">
                  <strong>Uafhængig databehandling:</strong> Landbruget.dk er et
                  uafhængigt initiativ. De ovenstående myndigheder og
                  institutioner har ikke godkendt, endorseret eller på anden
                  måde sanktioneret vores brug, behandling eller præsentation af
                  deres data.
                </p>
              </div>

              <div className="border-muted bg-muted/50 rounded-lg border p-6">
                <h3 className="text-primary-darker mb-3 text-xl font-bold">
                  Tak til vores datakilder
                </h3>
                <p className="text-foreground leading-relaxed">
                  Vi vil gerne udtrykke vores dybeste taknemmelighed til alle de
                  myndigheder, institutioner og organisationer, der gør deres
                  data offentligt tilgængelige. Deres åbenhed og transparens
                  muliggør projekter som Landbruget.dk og bidrager til en mere
                  informeret offentlig debat om dansk landbrug. Vi anerkender
                  det store arbejde, der ligger bag indsamling, kvalitetssikring
                  og offentliggørelse af disse værdifulde datasæt.
                </p>
              </div>

              <p className="text-foreground text-lg leading-relaxed">
                For mere information om vores metoder og principper, se vores{' '}
                <a
                  href="/om-os"
                  className="text-primary underline hover:no-underline"
                >
                  Om os
                </a>{' '}
                side.
              </p>
            </section>
          </article>
        </div>
      </Container>
    </div>
  );
}
