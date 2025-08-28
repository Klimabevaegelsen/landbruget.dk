'use client';

import IndividualRankingTable from './IndividualRankingTable';

// Define all 23 rankings
const RANKINGS = [
  // Financial Rankings (3)
  {
    id: 'highest_profit',
    title: 'Højest Overskud',
    category: 'financial',
    description: 'Virksomheder med det højeste nettoresultat i 2023',
  },
  {
    id: 'largest_assets',
    title: 'Størst Aktiver',
    category: 'financial',
    description: 'Virksomheder med de største samlede aktiver i 2023',
  },
  {
    id: 'most_employees_financial',
    title: 'Flest Ansatte',
    category: 'financial',
    description: 'Virksomheder med flest ansatte ifølge regnskabsdata 2023',
  },

  // Agricultural Area Rankings (4)
  {
    id: 'largest_land_area',
    title: 'Størst Landbrugsareal',
    category: 'field',
    description: 'Virksomheder med det største samlede landbrugsareal i 2024',
  },
  {
    id: 'largest_organic_area',
    title: 'Størst Økologisk Areal',
    category: 'field',
    description:
      'Virksomheder med det største økologiske landbrugsareal i 2024',
  },
  {
    id: 'highest_organic_percentage',
    title: 'Højest Økologisk Andel',
    category: 'field',
    description:
      'Virksomheder med den højeste andel økologisk landbrug (min. 50 ha)',
  },
  {
    id: 'most_fields',
    title: 'Flest Marker',
    category: 'field',
    description:
      'Virksomheder med det største antal individuelle marker i 2024',
  },

  // Environment Rankings (8)
  {
    id: 'highest_pesticide_burden',
    title: 'Højest Pesticidbelastning',
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
    title: 'Højest Glyphosatforbrug',
    category: 'environment',
    description: 'Virksomheder med det højeste glyphosatforbrug i 2024',
  },
  {
    id: 'most_diquat_usage',
    title: 'Højest Diquatforbrug',
    category: 'environment',
    description: 'Virksomheder med det højeste diquatforbrug i 2024',
  },
  {
    id: 'most_bnbo_not_dealt_with',
    title: 'Mest BNBO-areal Ikke Håndteret',
    category: 'environment',
    description:
      'Virksomheder med mest boringsnært beskyttelsesområde-areal der kræver handling i 2024',
  },
  {
    id: 'most_bnbo_dealt_with',
    title: 'Mest BNBO-areal Håndteret',
    category: 'environment',
    description:
      'Virksomheder med mest boringsnært beskyttelsesområde-areal der er håndteret i 2024',
  },
  {
    id: 'most_wetland_not_restored',
    title: 'Mest Lavbundsjorde Ikke Genoprettet',
    category: 'environment',
    description:
      'Virksomheder med mest lavbundsjorde-areal der har behov for genopretning i 2024',
  },
  {
    id: 'most_wetland_restored',
    title: 'Mest Lavbundsjorde Genoprettet',
    category: 'environment',
    description:
      'Virksomheder med mest lavbundsjorde-areal der er helt eller delvist genoprettet i 2024',
  },

  // Animal Rankings (5)
  {
    id: 'largest_pig_production',
    title: 'Størst Svineproduktion',
    category: 'animal',
    description: 'Produktionssteder med den største svineproduktion i 2024',
  },
  {
    id: 'largest_cattle_production',
    title: 'Størst Kvægproduktion',
    category: 'animal',
    description: 'Produktionssteder med den største kvægproduktion i 2024',
  },
  {
    id: 'highest_antibiotic_usage',
    title: 'Højest Antibiotikaforbrug',
    category: 'animal',
    description: 'Virksomheder med det højeste antibiotikaforbrug i 2024',
  },
  {
    id: 'most_production_sites',
    title: 'Flest Produktionssteder',
    category: 'animal',
    description: 'Virksomheder med flest dyreproduktionssteder i 2024',
  },
  {
    id: 'most_transported_pigs',
    title: 'Flest Transporterede Svin',
    category: 'animal',
    description: 'Virksomheder med flest transporterede svin i 2024',
  },

  // Worker Rankings (3)
  {
    id: 'most_employees_worker',
    title: 'Flest Ansatte (Arbejdsmarkedsdata)',
    category: 'worker',
    description:
      'Virksomheder med flest ansatte ifølge arbejdsmarkedsdata 2024',
  },
  {
    id: 'most_foreign_workers',
    title: 'Flest Arbejdstilladelser',
    category: 'worker',
    description: 'Virksomheder med flest aktive arbejdstilladelser i 2024',
  },
  {
    id: 'most_work_injuries',
    title: 'Flest Arbejdsulykker',
    category: 'worker',
    description: 'Virksomheder med flest rapporterede arbejdsulykker i 2024',
  },
];

export default function AllRankings() {
  return (
    <div className="w-full space-y-8">
      {/* Header */}
      <div className="space-y-4 text-center">
        <h2 className="text-3xl font-bold text-gray-900">
          Top 20 Danske Landbrugsvirksomheder
        </h2>
        <p className="mx-auto max-w-3xl text-lg text-gray-600">
          23 ranglister viser de førende virksomheder inden for økonomi,
          landbrugsareal, miljøpåvirkning, husdyrproduktion og beskæftigelse
          baseret på officielle data.
        </p>
      </div>

      {/* Rankings Grid */}
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2 xl:grid-cols-3">
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
