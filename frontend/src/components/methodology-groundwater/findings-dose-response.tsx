import { SubsectionHeader } from '@/components/methodology/article-layout';

export function FindingsDoseResponse() {
  return (
    <>
      <SubsectionHeader
        id="findings-dose-response"
        number="5.2"
        title="Dosis-respons"
      />
      <p>
        En kvartilanalyse af anvendelsesintensitet viser en klar
        dosis-respons-gradient: oplande med højest pesticidanvendelse (Q4) har
        ca. fire gange højere detektionssandsynlighed end oplande med lavest
        anvendelse (Q1).
      </p>
      <div className="border-border bg-card my-6 rounded border p-5">
        <p className="text-foreground mb-3 text-sm font-semibold">
          Detektionsrate: højeste vs. laveste kvartil
        </p>
        <div className="space-y-2 text-sm">
          <div className="flex items-baseline justify-between">
            <span className="text-muted-foreground">Bentazon</span>
            <span className="text-foreground font-medium">4,4× højere</span>
          </div>
          <div className="flex items-baseline justify-between">
            <span className="text-muted-foreground">1,2,4-Triazol</span>
            <span className="text-foreground font-medium">3,8× højere</span>
          </div>
          <div className="flex items-baseline justify-between">
            <span className="text-muted-foreground">
              4-Chlor-2-methylphenol
            </span>
            <span className="text-foreground font-medium">3,7× højere</span>
          </div>
        </div>
      </div>
      <p>
        Disse tre stoffer overlever alle fire valideringslag (se afsnit 4) og
        viser konsistente dosis-respons-mønstre.
      </p>
    </>
  );
}
