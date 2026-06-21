import { Container } from '@/components/layout/container';
import { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Om os - Landbruget.dk',
  description:
    'Lær mere om Landbruget.dk – et almennyttigt, open-source og non-profit initiativ drevet af den almennyttige forening Bureau 10.',
};

export default function AboutPage() {
  return (
    <div className="bg-background min-h-screen">
      <Container className="py-16 lg:py-24">
        <div className="mx-auto max-w-4xl">
          <article className="space-y-8">
            {/* Main Title */}
            <header className="space-y-6">
              <h1 className="text-primary text-5xl leading-tight font-black tracking-tight">
                Om os
              </h1>
              <p className="text-muted-foreground text-xl leading-relaxed">
                Velkommen til Landbruget.dk. Vi er et almennyttigt,{' '}
                <a
                  href="https://github.com/klimabevaegelsen/landbruget.dk/"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="underline hover:no-underline"
                >
                  open-source
                </a>{' '}
                og non-profit initiativ drevet af den publicistiske forening
                Bureau 10. Vores formål er at indsamle og formidle offentligt
                tilgængelige data om den danske landbrugssektor.
              </p>
            </header>

            <hr className="border-primary/20 my-12" />

            {/* Section 1 */}
            <section className="space-y-6">
              <h2 className="text-primary border-primary/20 border-b pb-4 text-3xl leading-tight font-bold">
                Hvorfor gør vi det?
              </h2>
              <p className="text-muted-foreground text-lg leading-relaxed">
                Landbruget spiller en stor rolle i Danmark – for økonomien,
                landskabet, miljøet, sundheden og klimaet. Beslutninger i og
                omkring sektoren har vidtrækkende konsekvenser, og en åben og
                informeret offentlig debat forudsætter adgang til fakta. I dag
                er mange relevante data spredt, svært tilgængelige eller gemt i
                formater, der gør dem vanskelige at anvende for den almindelige
                borger, journalist, forsker eller landmanden selv.
              </p>
            </section>

            {/* Section 2 */}
            <section className="space-y-8">
              <h2 className="text-primary border-primary/20 mt-16 border-b pb-4 text-3xl leading-tight font-bold">
                Vores formål er at:
              </h2>

              <div className="space-y-8">
                <div className="space-y-4">
                  <h3 className="text-primary-darker text-2xl leading-tight font-bold">
                    Fremme offentlig oplysning
                  </h3>
                  <p className="text-muted-foreground text-lg leading-relaxed">
                    Vi vil gøre det lettere for alle at forstå
                    landbrugssektorens komplekse sammenhænge og dens påvirkning
                    på samfundet, herunder miljø, sundhed og klima.
                  </p>
                </div>

                <div className="space-y-4">
                  <h3 className="text-primary-darker text-2xl leading-tight font-bold">
                    Understøtte en informeret offentlig debat
                  </h3>
                  <p className="text-muted-foreground text-lg leading-relaxed">
                    Ved at gøre data tilgængelige og brugervenlige vil vi styrke
                    grundlaget for en faktabaseret debat om landbrugets forhold
                    og fremtid.
                  </p>
                </div>

                <div className="space-y-4">
                  <h3 className="text-primary-darker text-2xl leading-tight font-bold">
                    Styrke transparens og læring af historien
                  </h3>
                  <p className="text-muted-foreground text-lg leading-relaxed">
                    Vi mener, at åbenhed om data – herunder produktionsforhold,
                    miljøpåvirkning, økonomiske støtteordninger og ledelses- og
                    ejerforhold – er et gode i sig selv. For at forstå nutiden
                    er det relevant også at kunne se tidligere ledelses- og
                    ejerforhold; derfor inkluderer vi også historiske
                    oplysninger. Formålet er at oplyse, ikke at placere skyld.
                  </p>
                </div>
              </div>
            </section>

            {/* Section 3 */}
            <section className="space-y-6">
              <h2 className="text-primary border-primary/20 mt-16 border-b pb-4 text-3xl leading-tight font-bold">
                Hvordan arbejder vi?
              </h2>

              <div className="space-y-6">
                <p className="text-muted-foreground text-lg leading-relaxed">
                  Landbruget.dk er baseret på{' '}
                  <a
                    href="https://github.com/klimabevaegelsen/landbruget.dk/"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="underline hover:no-underline"
                  >
                    open-source
                  </a>{' '}
                  principper. Det betyder, at vores metoder og software er åbne
                  for alle at inspicere, foreslå ændringer til og videreudvikle
                  på.
                </p>

                <p className="text-muted-foreground text-lg leading-relaxed">
                  Vi bestræber os på, efter bedste evne, at formidle data
                  neutralt og basere arbejdet på den bedste tilgængelige viden.
                  De data, vi indsamler, er primært dem, der i forvejen er
                  offentligt tilgængelige via officielle kilder. Hvor vi kan,
                  stiller vi data frit til rådighed under en Creative
                  Commons-licens. For data, hvor dette ikke er tilfældet,
                  bevares den ophavsret og de vilkår, som de oprindeligt er
                  offentliggjort under.
                </p>

                <p className="text-muted-foreground text-lg leading-relaxed">
                  Selvom vi lægger stor vægt på datakvalitet og nøjagtighed,
                  stilles platformen og dens data til rådighed{' '}
                  <strong className="text-foreground font-bold">
                    &apos;som de er og forefindes&apos;
                  </strong>
                  . Vi kan derfor ikke garantere, at alle informationer til
                  enhver tid er udtømmende, fejlfrie eller opdaterede.
                </p>

                <p className="text-muted-foreground text-lg leading-relaxed">
                  For at sikre platformens integritet og kvalitet bliver alle
                  forslag til ændringer i koden gennemgået og verificeret af
                  projektets kernebidragydere i Bureau 10, før de eventuelt
                  integreres i den offentlige version.
                </p>

                <p className="text-muted-foreground text-lg leading-relaxed">
                  Som en almennyttig forening uden kommerciel interesse i
                  projektet opfordrer vi til samarbejde og modtager gerne
                  bidrag, der kan styrke platformen – fx forslag til nye
                  datakilder, forbedrede metoder eller anden viden, herunder
                  hvis du mener at have fundet fejl eller unøjagtigheder. Vi
                  vurderer alle henvendelser seriøst, men kan på grund af
                  projektets ressourcer ikke garantere, at alle forslag kan
                  implementeres.
                </p>

                <p className="text-muted-foreground text-lg leading-relaxed">
                  Vi håber, at Landbruget.dk kan blive et værdifuldt redskab for
                  alle med interesse i dansk landbrug og dets rolle i samfundet.
                </p>
              </div>
            </section>

            {/* Contact Section */}
            <section className="space-y-6">
              <h2 className="text-primary border-primary/20 mt-16 border-b pb-4 text-3xl leading-tight font-bold">
                Kontakt os
              </h2>
              <div className="space-y-4">
                <p className="text-muted-foreground text-lg leading-relaxed">
                  Har du spørgsmål, forslag eller har du fundet fejl i vores
                  data? Vi hører gerne fra dig!
                </p>
                <div className="flex flex-col gap-4 sm:flex-row sm:items-center">
                  <a
                    href="mailto:info@landbruget.dk"
                    className="touch-target bg-primary text-primary-foreground hover:bg-primary/90 focus:ring-primary inline-flex min-h-[44px] items-center justify-center rounded-lg px-6 py-3 text-base font-medium transition-colors focus:ring-2 focus:ring-offset-2 focus:outline-none"
                  >
                    Skriv til os: info@landbruget.dk
                  </a>
                  <p className="text-muted-foreground text-sm">
                    Vi bestræber os på at svare inden for få dage.
                  </p>
                </div>
              </div>
            </section>
          </article>
        </div>
      </Container>
    </div>
  );
}
