import { Container } from '@/components/layout/container';
import { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Om os - Landbruget.dk',
  description:
    'Lær mere om Landbruget.dk - et almennyttigt, open-source og non-profit initiativ drevet af Klimabevægelsen.',
};

export default function AboutPage() {
  return (
    <div className="min-h-screen bg-white">
      <Container className="py-16 lg:py-24">
        <div className="mx-auto max-w-4xl">
          <article className="space-y-8">
            {/* Main Title */}
            <header className="space-y-6">
              <h1 className="text-primary text-5xl leading-tight font-black tracking-tight">
                Om os
              </h1>
              <p className="text-xl leading-relaxed text-gray-700">
                Velkommen til Landbruget.dk. Vi er et almennyttigt,{' '}
                <a
                  href="https://github.com/klimabevaegelsen/landbruget.dk/"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="underline hover:no-underline"
                >
                  open-source
                </a>{' '}
                og non-profit initiativ drevet af Klimabevægelsen. Vores mission
                er at indsamle og formidle offentligt tilgængelige data om den
                danske landbrugssektor.
              </p>
            </header>

            <hr className="border-primary/20 my-12" />

            {/* Section 1 */}
            <section className="space-y-6">
              <h2 className="text-primary border-primary/20 border-b pb-4 text-3xl leading-tight font-bold">
                Hvorfor gør vi det?
              </h2>
              <p className="text-lg leading-relaxed text-gray-700">
                Landbruget spiller en afgørende rolle i Danmark – for vores
                økonomi, vores landskab, vores miljø, vores sundhed og vores
                klima. Beslutninger truffet i og omkring sektoren har
                vidtrækkende konsekvenser. Vi mener, at en åben og informeret
                offentlig debat er essentiel for at sikre en bæredygtig
                udvikling. Desværre er mange relevante data i dag spredt, svært
                tilgængelige eller gemt i formater, der gør dem vanskelige at
                anvende for den almindelige borger, journalist, forsker eller
                endda landmanden selv.
              </p>
            </section>

            {/* Section 2 */}
            <section className="space-y-8">
              <h2 className="text-primary border-primary/20 mt-16 border-b pb-4 text-3xl leading-tight font-bold">
                Vores mål er at:
              </h2>

              <div className="space-y-8">
                <div className="space-y-4">
                  <h3 className="text-primary-darker text-2xl leading-tight font-bold">
                    Fremme offentlig oplysning og inspirere
                  </h3>
                  <p className="text-lg leading-relaxed text-gray-700">
                    Vi vil gøre det lettere for alle at forstå
                    landbrugssektorens komplekse sammenhænge og dens påvirkning
                    på samfundet, herunder miljø, sundhed og klima. Samtidig
                    ønsker vi at synliggøre de landmænd og virksomheder, der går
                    forrest med bæredygtige løsninger og viser vejen mod en
                    grønnere praksis.
                  </p>
                </div>

                <div className="space-y-4">
                  <h3 className="text-primary-darker text-2xl leading-tight font-bold">
                    Understøtte en informeret og konstruktiv demokratisk debat
                  </h3>
                  <p className="text-lg leading-relaxed text-gray-700">
                    Ved at gøre data tilgængelige og brugervenlige ønsker vi at
                    styrke grundlaget for en dialog om landbrugets fremtid og
                    den nødvendige grønne omstilling, båret af både udfordringer
                    og succesfulde eksempler.
                  </p>
                </div>

                <div className="space-y-4">
                  <h3 className="text-primary-darker text-2xl leading-tight font-bold">
                    Styrke transparens, ansvarlighed og læring af historien
                  </h3>
                  <div className="space-y-4">
                    <p className="text-lg leading-relaxed text-gray-700">
                      Vi tror på, at åbenhed om data – herunder
                      produktionsforhold, miljøpåvirkning, økonomiske
                      støtteordninger og ledelsesforhold – er afgørende for at
                      drive positive forandringer. For at forstå nutiden og
                      forme fremtiden er det vigtigt at lære af fortiden. Derfor
                      inkluderer vi også oplysninger om tidligere ledelses- og
                      ejerforhold.
                    </p>
                    <p className="text-lg leading-relaxed text-gray-700">
                      Det handler ikke kun om at placere ansvar for beslutninger
                      med langsigtede negative konsekvenser, som f.eks.
                      pesticidanvendelse der påvirker grundvandet. Lige så
                      vigtigt er det at kunne identificere og fremhæve de
                      positive, langsigtede initiativer, f.eks. de landmænd der
                      tidligt plantede træer, investerede i naturpleje eller
                      omlagde til driftsformer, der først årtier senere viser
                      deres fulde positive værdi for miljø og biodiversitet.
                      Denne historiske indsigt er afgørende for både at
                      anerkende fremsynede handlinger og for at sikre en
                      retfærdig forståelse af, hvordan vi er nået til, hvor vi
                      er i dag.
                    </p>
                  </div>
                </div>

                <div className="space-y-4">
                  <h3 className="text-primary-darker text-2xl leading-tight font-bold">
                    Understøtte den grønne omstilling
                  </h3>
                  <p className="text-lg leading-relaxed text-gray-700">
                    Særligt i lyset af Den Grønne Trepartsaftale er der et stort
                    behov for et solidt datagrundlag. Landbruget.dk skal levere
                    data, der kan hjælpe med at monitorere og evaluere
                    implementeringen af aftalens mål, fremhæve fremskridt og
                    accelerere transformationen mod en mere bæredygtig
                    arealanvendelse og fødevareproduktion.
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
                <p className="text-lg leading-relaxed text-gray-700">
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

                <p className="text-lg leading-relaxed text-gray-700">
                  Vi bestræber os på, med bedste intention og efter bedste evne,
                  at formidle data neutralt og basere vores arbejde på den
                  bedste tilgængelige videnskab. De data, vi indsamler, er
                  primært dem, der i forvejen er offentligt tilgængelige via
                  officielle kilder. Hvor vi har mulighed for det, stiller vi
                  disse data frit til rådighed under en Creative Commons licens.
                  For data, hvor dette ikke er tilfældet, bibeholder de den
                  ophavsret og de vilkår, som de oprindeligt er offentliggjort
                  under, af de offentlige myndigheder.
                </p>

                <p className="text-lg leading-relaxed text-gray-700">
                  Selvom vi lægger stor vægt på datakvalitet og nøjagtighed, og
                  løbende arbejder på at forbedre disse, er det vigtigt at
                  understrege, at platformen og de data, den indeholder, stilles
                  til rådighed{' '}
                  <strong className="font-bold text-gray-900">
                    &apos;som de er og forefindes&apos;
                  </strong>
                  . Vi kan derfor ikke give garantier for, at alle informationer
                  til enhver tid er fuldstændigt udtømmende, fejlfrie eller
                  opdaterede, især i et felt der konstant udvikler sig.
                </p>

                <p className="text-lg leading-relaxed text-gray-700">
                  For at sikre platformens integritet, stabilitet og kvalitet,
                  vil alle forslag til ændringer i koden blive gennemgået og
                  verificeret af projektets kernebidragydere fra
                  Klimabevægelsen, før de eventuelt integreres i den offentlige
                  version. Landbrugssektoren og dens samfundsmæssige
                  påvirkninger er dog et komplekst felt, hvor viden og
                  forståelse konstant udvikler sig. Derfor anerkender vi, at vi
                  alle bliver klogere over tid.
                </p>

                <p className="text-lg leading-relaxed text-gray-700">
                  Da vi er en almennyttig forening drevet af ønsket om at
                  bidrage positivt til samfundet og uden kommerciel interesse i
                  dette projekt, opfordrer vi aktivt til samarbejde og modtager
                  med stor taknemmelighed bidrag, der kan styrke platformen og
                  dens formål. Dette kan være forslag til nye relevante
                  datakilder, forbedrede metoder, eller anden viden. Det gælder
                  også, hvis du mener at have fundet fejl, unøjagtigheder eller
                  har forslag til forbedringer af eksisterende indhold.
                </p>

                <p className="text-lg leading-relaxed text-gray-700">
                  Sådanne henvendelser betragter vi som værdifuld hjælp i vores
                  bestræbelser på løbende at forbedre platformens kvalitet og
                  nøjagtighed, hvilket understøtter idéen om kollektiv
                  forbedring gennem åbenhed. Alle bidrag og henvendelser vil
                  blive taget seriøst i betragtning og vurderet af
                  projektteamet, ligesom vi gør med forslag til kodeændringer.
                  På grund af projektets natur og ressourcer kan vi dog ikke
                  garantere, at alle indsendte forslag eller fejlrettelser kan
                  implementeres, eller hvornår det i givet fald vil ske, men vi
                  værdsætter al den feedback, vi modtager.
                </p>

                <p className="text-lg leading-relaxed text-gray-700">
                  Vi håber, at Landbruget.dk kan blive et værdifuldt redskab for
                  alle med interesse i dansk landbrug og dets rolle i en
                  bæredygtig fremtid.
                </p>
              </div>
            </section>
          </article>
        </div>
      </Container>
    </div>
  );
}
