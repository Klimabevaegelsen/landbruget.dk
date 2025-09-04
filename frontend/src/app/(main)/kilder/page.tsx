import { Container } from '@/components/layout/container';
import { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Kilder - Landbruget.dk',
  description:
    'Oversigt over datakilder og myndigheder der leverer data til Landbruget.dk.',
};

export default function KilderPage() {
  return (
    <div className="min-h-screen bg-white">
      <Container className="py-16 lg:py-24">
        <div className="mx-auto max-w-6xl">
          <article className="space-y-8">
            {/* Main Title */}
            <header className="space-y-6">
              <h1 className="text-primary text-5xl leading-tight font-black tracking-tight">
                Kilder
              </h1>
              <p className="text-xl leading-relaxed text-gray-700">
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

              {/* First Table */}
              <div className="overflow-x-auto">
                <table className="w-full border-collapse border border-gray-300">
                  <thead>
                    <tr className="bg-primary/10">
                      <th className="text-primary border border-gray-300 px-4 py-3 text-left font-bold">
                        Myndighed eller institution
                      </th>
                      <th className="text-primary border border-gray-300 px-4 py-3 text-left font-bold">
                        Kilde
                      </th>
                      <th className="text-primary border border-gray-300 px-4 py-3 text-left font-bold">
                        Data
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Danmarks Meteorologiske Institut
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        API
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Vejr
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td
                        className="border border-gray-300 px-4 py-3 font-medium text-gray-700"
                        rowSpan={2}
                      >
                        Styrelsen for Dataforsyning og Infrastruktur
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Datafordeleren (API)
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        <ul className="list-inside list-disc space-y-1">
                          <li>Administrative Geografiske Inddelinger</li>
                          <li>Matrikeloplysninger</li>
                        </ul>
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        BBR Bygninger
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Bygningsdata og -geometri
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Danmarks Statistik
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Danmarks Statistik (API)
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Landbrugsproduktion
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Naturstyrelsen
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        WFS API
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Miljø og klimaprojekter
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td
                        className="border border-gray-300 px-4 py-3 font-medium text-gray-700"
                        rowSpan={2}
                      >
                        Styrelsen for Grøn Arealømlægning og Vandmiljø
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Aktindsigt
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Gødningsregnskab
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        WFS API
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        <ul className="list-inside list-disc space-y-1">
                          <li>Lavbundsjorde</li>
                          <li>Miljø og klimaprojekter</li>
                          <li>Tilskud</li>
                        </ul>
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Klimadatastyrelsen
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        API
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
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
                <table className="w-full border-collapse border border-gray-300">
                  <thead>
                    <tr className="bg-primary/10">
                      <th className="text-primary border border-gray-300 px-4 py-3 text-left font-bold">
                        Myndighed eller institution
                      </th>
                      <th className="text-primary border border-gray-300 px-4 py-3 text-left font-bold">
                        Kilde
                      </th>
                      <th className="text-primary border border-gray-300 px-4 py-3 text-left font-bold">
                        Data
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Beredskabsstyrelsen
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Aktindsigter
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        <ul className="list-inside list-disc space-y-1">
                          <li>Staldbrande</li>
                          <li>Gyllelæk</li>
                          <li>Dyretransporter i færdselsuheld</li>
                        </ul>
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td
                        className="border border-gray-300 px-4 py-3 font-medium text-gray-700"
                        rowSpan={2}
                      >
                        Erhvervsstyrelsen
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        CVR-registret (API)
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        <ul className="list-inside list-disc space-y-1">
                          <li>Ledelse</li>
                          <li>Finansielle oplysninger</li>
                          <li>Medarbejderantal</li>
                          <li>Øvrige kerneoplysninger</li>
                        </ul>
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Aktindsigt
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Tilskud
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td
                        className="border border-gray-300 px-4 py-3 font-medium text-gray-700"
                        rowSpan={2}
                      >
                        Energistyrelsen
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Emoweb (API)
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Energimærker
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Aktindsigt
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Biogasanlæg
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td
                        className="border border-gray-300 px-4 py-3 font-medium text-gray-700"
                        rowSpan={3}
                      >
                        Miljøstyrelsen
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Digital Miljøadministration
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Miljøgodkendelser
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Bekæmpelsesmiddel-databasen
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Pesticidoplysninger
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Aktindsigt
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Pesticidforbrug
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td
                        className="border border-gray-300 px-4 py-3 font-medium text-gray-700"
                        rowSpan={2}
                      >
                        Miljøstyrelsen (fortsat)
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        WFS API
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        <ul className="list-inside list-disc space-y-1">
                          <li>Markoplysninger & markblokke</li>
                          <li>Boringsnære beskyttelsesområder</li>
                          <li>Miljø og klimaprojekter</li>
                        </ul>
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Det Centrale Husdyrbrugsregister (API)
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        <ul className="list-inside list-disc space-y-1">
                          <li>Antibiotikaforbrug</li>
                          <li>Dyretransporter</li>
                          <li>Veterinære hændelser</li>
                          <li>Øvrige kerneoplysninger om bedrifter</li>
                        </ul>
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Fødevarestyrelsen
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Aktindsigt
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        <ul className="list-inside list-disc space-y-1">
                          <li>Internationale dyretransport</li>
                          <li>Tilskud</li>
                        </ul>
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Landbrug & Fødevarer
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        SPF-Sund hjemmeside
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Veterinær status
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Aarhus Universitet, DCE
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Rapport
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Kvælstofmodellering
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Københavns Universitet, IFRO
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Rapport
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Emissionsfaktorer for klimaregnskab
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        SEGES & Økologisk Landsforening
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Aktindsigt
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Klimaregnskabsmodellering
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Vejdirektoratet
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        ArcGIS (API)
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Markblokke
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Styrelsen for International Rekruttering og Integration
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Aktindsigt
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Førstegangsvisumanansøgninger
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Arbejdstilsynet
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Hjemmeside
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Arbejdspladsstilsyn
                      </td>
                    </tr>
                    <tr className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Arbejdsmarkedets Erhvervssikring
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Aktindsigt
                      </td>
                      <td className="border border-gray-300 px-4 py-3 text-gray-700">
                        Arbejdsskader
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </section>

            {/* Additional Information */}
            <section className="space-y-6">
              <h2 className="text-primary border-primary/20 mt-16 border-b pb-4 text-3xl leading-tight font-bold">
                Om vores dataindsamling
              </h2>
              <p className="text-lg leading-relaxed text-gray-700">
                Alle data på Landbruget.dk stammer fra offentligt tilgængelige
                kilder. Vi indsamler og behandler data fra disse myndigheder og
                institutioner for at skabe et samlet overblik over den danske
                landbrugssektor. Dataene opdateres løbende efter de enkelte
                kilders publiceringsrytme.
              </p>
              <p className="text-lg leading-relaxed text-gray-700">
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
