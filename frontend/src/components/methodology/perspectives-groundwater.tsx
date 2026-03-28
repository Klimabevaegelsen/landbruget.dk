import { SubsectionHeader } from '@/components/methodology/article-layout';

export function PerspectivesGroundwater() {
  return (
    <>
      <SubsectionHeader
        id="groundwater"
        number="4.1"
        title="Grundvandsbeskyttelse og drikkevandssikkerhed"
      />
      <p>
        Danmark er et af de eneste lande i Europa, hvor 100 % af drikkevandet
        stammer fra grundvand. Pesticider og deres nedbrydningsprodukter er
        påvist i 51 % af overvågningsboringer, med koncentrationer over
        grænseværdien (0,1 &mu;g/L) i 15 % [15]. I yngre, overfladenært
        grundvand er tallene endnu højere: fund i 72 % af boringerne og
        overskridelser i 39 %.
      </p>
      <p>
        Pesticiddata på markniveau muliggør en direkte geografisk kobling mellem
        anvendelsessteder og det nationale grundvandsovervågningsprogram
        (GRUMO), der årligt analyserer for ca. 53 pesticider [16]. Ved at
        overlejre vores markkort med GRUMO-boringernes placering kan man
        undersøge, om områder med intensiv pesticidanvendelse korrelerer med
        forhøjede fund i det underliggende grundvand.
      </p>
      <p>
        En særlig dansk anvendelse er de boringsnære beskyttelsesområder (BNBO).
        Cirka 20.000 hektar BNBO er udpeget, heraf ca. 9.500 ha landbrugsjord.
        Et generelt sprøjteforbud i BNBO trådte i kraft 1. juli 2024 [17]. Data
        på markniveau gør det muligt at identificere præcist, hvilke marker med
        dokumenteret pesticidanvendelse der ligger i eller grænser op til
        BNBO-zonerne.
      </p>
    </>
  );
}
