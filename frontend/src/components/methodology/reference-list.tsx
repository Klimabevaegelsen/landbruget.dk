import { type ReactNode } from 'react';

const REFS: ReactNode[] = [
  <>
    Maggi, F., Tang, F. H. M., la Cecilia, D. &amp; McBratney, A. (2019).
    &ldquo;PEST-CHEMGRIDS, global gridded maps of the top 20 crop-specific
    pesticide application rates.&rdquo; <em>Scientific Data</em> 6, 170.
  </>,
  <>
    Maggi, F. et al. (2025). &ldquo;Gridded maps of pesticide application rates
    for the European Union at 250-m resolution.&rdquo; <em>Scientific Data</em>{' '}
    12, 725.
  </>,
  <>
    Martin, E. et al. (2023). &ldquo;Spatialisation of crop-specific pesticide
    use from national sales data to the parcel level.&rdquo;{' '}
    <em>Journal of Cleaner Production</em>.
  </>,
  <>
    Galimberti, F. et al. (2025). &ldquo;From parcels to people: a
    high-resolution pesticide risk indicator for France.&rdquo;{' '}
    <em>Scientific Reports</em>.
  </>,
  <>
    Habran, L. et al. (2022). &ldquo;Mapping pesticide exposure at 100-m
    resolution in Wallonia.&rdquo; <em>Environmental Pollution</em>.
  </>,
  <>
    Udias, A. et al. (2023). &ldquo;Estimation of pesticide emissions for the
    European Union at NUTS-3 resolution.&rdquo; <em>Scientific Data</em>.
  </>,
  <>
    Nause, M. et al. (2021). &ldquo;SYNOPS-GIS: a spatially explicit pesticide
    risk assessment at the field level.&rdquo; <em>Pest Management Science</em>.
  </>,
  <>
    Kudsk, P. &amp; J&oslash;rgensen, L. N. (2018). &ldquo;Pesticide Load
    &ndash; a new Danish pesticide risk indicator with multiple
    applications.&rdquo; <em>Land Use Policy</em> 70, 384&ndash;393.
  </>,
  <>
    J&oslash;rgensen, L. N. &amp; Kudsk, P. (2019). &ldquo;Twenty years of
    pesticide use patterns in winter wheat in Denmark.&rdquo;{' '}
    <em>Crop Protection</em>.
  </>,
  <>
    d&apos;Andrimont, R. et al. (2021). &ldquo;Improving the monitoring of
    pesticide use in the European Union.&rdquo;{' '}
    <em>Nature Ecology &amp; Evolution</em>.
  </>,
  <>
    Landbrugsstyrelsen. &ldquo;F&aelig;llesskema &ndash; Markblokke og
    afgr&oslash;dedata.&rdquo; Udgivet &aring;rligt.
  </>,
  <>
    Milj&oslash;styrelsen. &ldquo;Bek&aelig;mpelsesmiddeldatabasen (BMD).&rdquo;
    bmd.mst.dk.
  </>,
  <>
    Milj&oslash;styrelsen. &ldquo;Pesticidindberetninger &ndash; aggregeret
    virksomhedsdata.&rdquo; Landbrug&aring;r 2020/21&ndash;2024/25.
  </>,
  <>
    Geodatastyrelsen. &ldquo;Bygnings- og Boligregistret (BBR).&rdquo;
    dataforsyningen.dk.
  </>,
  <>
    GEUS. &ldquo;Grundvandsoverv&aring;gning (GRUMO) &ndash; pesticider i dansk
    grundvand.&rdquo; &Aring;rlig statusrapport.
  </>,
  <>
    Hansen, B. et al. (2022). &ldquo;National Assessment of Long-Term
    Groundwater Response to Pesticide Regulation.&rdquo;{' '}
    <em>Environmental Science &amp; Technology</em>.
  </>,
  <>
    Milj&oslash;- og Ligestillingsministeriet (2024). &ldquo;Nyt forbud
    beskytter s&aring;rbare omr&aring;der omkring drikkevandsboringer mod
    spr&oslash;jtemidler.&rdquo; regeringen.dk.
  </>,
  <>
    Bresson, M. et al. (2020). &ldquo;Assessment of residential exposures to
    agricultural pesticides: A scoping review.&rdquo; <em>PLOS ONE</em> 15(4),
    e0232258.
  </>,
  <>
    Brouwer, M. et al. (2022). &ldquo;Residential proximity to crops and
    agricultural pesticide use and cause-specific mortality.&rdquo;{' '}
    <em>Science of the Total Environment</em> 814, 152722.
  </>,
  <>
    Baldi, I. et al. (2021). &ldquo;Residential proximity to agricultural fields
    and neurological and mental health outcomes.&rdquo;{' '}
    <em>Environmental Health Perspectives</em>.
  </>,
  <>
    Lu, C. et al. (2012). &ldquo;Organophosphate Pesticide Exposure and
    Residential Proximity to Nearby Fields: Evidence for the Drift
    Pathway.&rdquo;{' '}
    <em>Journal of Exposure Science &amp; Environmental Epidemiology</em>.
  </>,
  <>
    EEA (2024). &ldquo;Pesticides in rivers, lakes and groundwater in
    Europe.&rdquo; European Environment Agency indicator report.
  </>,
  <>
    Hooper, M. J. et al. (2023). &ldquo;Combined impact of pesticides and other
    stressors on aquatic biodiversity.&rdquo;{' '}
    <em>Frontiers in Environmental Science</em>.
  </>,
  <>
    Fernandez, N. et al. (2024). &ldquo;Threats to and management of Natura 2000
    protected areas relative to agricultural practices.&rdquo;{' '}
    <em>Conservation Biology</em>.
  </>,
  <>
    Nicholson, C. et al. (2022). &ldquo;Putting pesticides on the map for
    pollinator research and conservation.&rdquo; <em>Scientific Data</em> 9,
    570.
  </>,
  <>
    Guignet, D. et al. (2016). &ldquo;The Property Value Impacts of Groundwater
    Contamination: Agricultural Runoff and Private Wells.&rdquo; US EPA Working
    Paper 2015-05.
  </>,
];

export function ReferenceList() {
  return (
    <ol className="text-foreground/65 [&_li:target]:bg-primary/[0.06] mt-6 space-y-4 pl-0 text-[14px] leading-relaxed [&_li]:scroll-mt-24 [&_li:target]:-mx-3 [&_li:target]:rounded [&_li:target]:px-3 [&_li:target]:py-1 [&_li:target]:transition-colors [&_li:target]:duration-700">
      {REFS.map((ref, i) => (
        <li key={i} id={`ref-${i + 1}`} className="flex gap-3">
          <span className="text-primary/40 mt-px shrink-0 text-[12px] font-semibold tabular-nums">
            [{i + 1}]
          </span>
          <span>{ref}</span>
        </li>
      ))}
    </ol>
  );
}
