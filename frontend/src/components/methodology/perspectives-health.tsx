import { SubsectionHeader } from '@/components/methodology/article-layout';

export function PerspectivesHealth() {
  return (
    <>
      <SubsectionHeader
        id="health"
        number="4.2"
        title="Sundhed og nærhedsanalyse"
      />
      <p>
        En systematisk gennemgang af 151 studier (1988&ndash;2019) viser, at 71
        % finder en signifikant sammenhæng mellem beboeres nærhed til
        pesticidbehandlede marker og negative sundhedseffekter [18]. De hyppigst
        dokumenterede udfald er neurodegenerative sygdomme (f.eks. Parkinsons),
        børnekræft (hjernetumorer), udviklingsforstyrrelser og reproduktive
        komplikationer [19, 20].
      </p>
      <p>
        Eksponeringsgradienten er veldokumenteret: Koncentrationen af
        pesticid-rester (metabolitter) i urin falder i takt med afstanden til
        behandlede arealer [21]. De fleste epidemiologiske studier anvender
        afstandsanalyser (bufferzoner) på 200&ndash;500 meter omkring boliger
        som indikator for eksponering [18].
      </p>
      <p>
        Kombinationen af vores data og Bygnings- og Boligregistret (BBR) [14]
        gør det muligt at beregne den præcise afstand mellem marker med
        pesticidanvendelse og beboelse, skoler, daginstitutioner og hospitaler.
        Afstandene beregnes præcist i meter via geografiske koordinater (
        <code>ST_Distance</code>).
      </p>
      <p>
        Selvom dette er et overordnet pejlemærke og ikke en præcis
        eksponeringsmodel (da faktisk afdrift afhænger af bl.a. vind, vejr og
        sprøjteteknik), giver det et hidtil uset udgangspunkt for epidemiologisk
        forskning i Danmark.
      </p>
    </>
  );
}
