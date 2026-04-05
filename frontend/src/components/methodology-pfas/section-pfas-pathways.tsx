import { SectionHeader } from '@/components/methodology/article-layout';

export function SectionPfasPathways() {
  return (
    <section data-testid="section-pfas-pathways">
      <SectionHeader
        id="pfas-pathways"
        number="4"
        title="Fire PFAS-veje til grundvandet"
      />

      <p className="mb-4">
        Landbruget bidrager til PFAS i grundvand via mindst fire mekanismer:
      </p>

      <ol className="list-decimal space-y-3 pl-6">
        <li>
          <strong>TFA fra fluorpesticider</strong> &ndash; Fluorholdige
          aktivstoffer (fluopyram, diflufenican, tau-fluvalinat m.fl.) nedbrydes
          i milj&oslash;et til trifluoreddikesyre. 35 registrerede aktivstoffer
          i Danmark kan danne TFA. Denne vej forklarer den universelle
          TFA-tilstedev&aelig;relse.
        </li>
        <li>
          <strong>Biosolids (spildevandsslam)</strong> &ndash; PFAS-holdigt slam
          udspredt p&aring; landbrugsjord kan udvaske PFOS, PFOA og andre
          langk&aelig;dede PFAS til grundvandet over tid.
        </li>
        <li>
          <strong>Pesticidformuleringer</strong> &ndash; Visse
          pesticidformuleringer anvender fluorholdige overfladeaktive stoffer
          (surfactants), som kan introducere traditionelle PFAS uafh&aelig;ngigt
          af selve aktivstoffet.
        </li>
        <li>
          <strong>Atmosf&aelig;risk deposition</strong> &ndash; Nedbrydning af
          fluorholdige k&oslash;lemidler (HFC&apos;er) producerer TFA i
          atmosf&aelig;ren, som afs&aelig;ttes via nedb&oslash;r. Denne vej
          bidrager til baggrundsniveauet af TFA og er uafh&aelig;ngig af lokal
          pesticidanvendelse.
        </li>
      </ol>

      <p className="mt-4">
        De fire veje har meget forskellige tidslinjer: TFA fra pesticider kan
        n&aring; grundvandet inden for f&aring; &aring;r, mens PFOS fra
        biosolids kan tage &aring;rtier. Den atmosf&aelig;riske deposition er
        kontinuerlig og allestedsn&aelig;rv&aelig;rende.
      </p>
    </section>
  );
}
