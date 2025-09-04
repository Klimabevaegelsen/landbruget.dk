'use client';

import IndividualRankingTable from './IndividualRankingTable';

// Define all 23 rankings
const RANKINGS = [
  // Agricultural Area Rankings (4)
  {
    id: 'largest_land_area',
    title: 'Størst landbrugsareal',
    category: 'field',
    description: 'Virksomheder med det største samlede landbrugsareal i 2024',
  },
  {
    id: 'largest_organic_area',
    title: 'Størst økologisk areal',
    category: 'field',
    description:
      'Virksomheder med det største økologiske landbrugsareal i 2024',
  },
  {
    id: 'highest_organic_percentage',
    title: 'Højest økologisk andel',
    category: 'field',
    description:
      'Virksomheder med den højeste andel økologisk landbrug (min. 50 ha) i 2024',
  },
  {
    id: 'most_fields',
    title: 'Flest marker',
    category: 'field',
    description:
      'Virksomheder med det største antal individuelle marker i 2024',
  },

  // Environment Rankings (8)
  {
    id: 'highest_pesticide_burden',
    title: 'Højest pesticidbelastning',
    category: 'environment',
    description:
      'Virksomheder med den højeste samlede pesticidbelastning i 2024',
  },
  {
    id: 'most_pfas_usage',
    title: 'Højest PFAS-forbrug',
    category: 'environment',
    description:
      'Virksomheder med det højeste forbrug af PFAS-holdige pesticider i 2024',
  },
  {
    id: 'most_glyphosate_usage',
    title: 'Højest glyphosatforbrug',
    category: 'environment',
    description: 'Virksomheder med det højeste glyphosatforbrug i 2024',
  },
  {
    id: 'most_diquat_usage',
    title: 'Højest diquatforbrug',
    category: 'environment',
    description: 'Virksomheder med det højeste diquatforbrug i 2024',
  },
  {
    id: 'most_bnbo_not_dealt_with',
    title: 'Mest BNBO-areal ikke håndteret',
    category: 'environment',
    description:
      'Virksomheder med mest boringsnært beskyttelsesområde-areal der kræver handling i 2024',
  },
  {
    id: 'most_bnbo_dealt_with',
    title: 'Mest BNBO-areal håndteret',
    category: 'environment',
    description:
      'Virksomheder med mest boringsnært beskyttelsesområde-areal der er håndteret i 2024',
  },
  {
    id: 'most_wetland_not_restored',
    title: 'Mest lavbundsjorde ikke genoprettet',
    category: 'environment',
    description:
      'Virksomheder med mest lavbundsjorde-areal der har behov for genopretning i 2024',
  },
  {
    id: 'most_wetland_restored',
    title: 'Mest lavbundsjorde genoprettet',
    category: 'environment',
    description:
      'Virksomheder med mest lavbundsjorde-areal der er helt eller delvist genoprettet i 2024',
  },

  // Animal Rankings (5)
  {
    id: 'largest_pig_production',
    title: 'Størst svineproduktionskapacitet',
    category: 'animal',
    description:
      'Produktionssteder med den største svineproduktionskapacitet i 2024',
  },
  {
    id: 'largest_cattle_production',
    title: 'Størst kvægproduktionskapacitet',
    category: 'animal',
    description:
      'Produktionssteder med den største kvægproduktionskapacitet i 2024',
  },
  {
    id: 'highest_antibiotic_usage',
    title: 'Højest antibiotikaforbrug',
    category: 'animal',
    description: 'Virksomheder med det højeste antibiotikaforbrug i 2024',
  },
  {
    id: 'most_production_sites',
    title: 'Flest produktionssteder',
    category: 'animal',
    description: 'Virksomheder med flest dyreproduktionssteder i 2024',
  },
  {
    id: 'most_transported_pigs',
    title: 'Flest transporterede svin',
    category: 'animal',
    description: 'Virksomheder med flest transporterede svin i 2024',
  },

  // Worker Rankings (3)
  {
    id: 'most_employees_worker',
    title: 'Flest ansatte (arbejdsmarkedsdata)',
    category: 'worker',
    description:
      'Virksomheder med flest ansatte ifølge arbejdsmarkedsdata 2024',
  },
  {
    id: 'most_foreign_workers',
    title: 'Flest arbejdstilladelser',
    category: 'worker',
    description: 'Virksomheder med flest aktive arbejdstilladelser i 2024',
  },
  {
    id: 'most_work_injuries',
    title: 'Flest arbejdsulykker',
    category: 'worker',
    description: 'Virksomheder med flest rapporterede arbejdsulykker i 2024',
  },

  // Financial Rankings (moved to end)
  {
    id: 'highest_profit',
    title: 'Højest overskud',
    category: 'financial',
    description: 'Virksomheder med det højeste nettoresultat i 2024',
  },
  {
    id: 'largest_assets',
    title: 'Størst aktiver',
    category: 'financial',
    description: 'Virksomheder med de største samlede aktiver i 2024',
  },
  {
    id: 'most_employees_financial',
    title: 'Flest ansatte',
    category: 'financial',
    description: 'Virksomheder med flest ansatte ifølge regnskabsdata 2024',
  },
];

export default function AllRankings() {
  return (
    <div className="w-full space-y-8">
      {/* Rankings Grid */}
      <div className="grid grid-cols-1 gap-6">
        {RANKINGS.map((ranking) => (
          <IndividualRankingTable
            key={ranking.id}
            rankingId={ranking.id}
            title={ranking.title}
            category={ranking.category}
            description={ranking.description}
            initialLimit={20}
          />
        ))}
      </div>

      {/* Footer */}
      <div className="border-t border-gray-200 pt-8 text-center">
        <p className="text-xs text-gray-500">
          Data opdateret: {new Date().toLocaleDateString('da-DK')} • 23
          ranglister baseret på officielle data fra 2023-2024
        </p>
      </div>
    </div>
  );
}
