import { Company } from "@/components/company/company";
import { getCompanyById } from "@/services/supabase/company";
import { notFound } from "next/navigation";

type Props = {
  params: Promise<{ id: string }>;
};

export const revalidate = 3600;

export async function generateStaticParams() {
  return [];
}

export default async function CompanyPage({ params }: Props) {
  const { id } = await params;
  const company = await getCompanyById(id);

  if (!company) {
    return notFound();
  }

  company.pageBuilder.push({
    iterationConfig: {
      layout: "collapsible",
      titleField: "site_name",
    },
    sections: [
      {
        _key: "animal-welfare-sites-iteration-item-0_x7k9p",
        title: "Vejle Svinefarm 1",
        layout: "collapsible",
        content: [
          {
            items: [
              {
                label: "Site CHR",
                value: "CHR00401",
              },
              {
                label: "Adresse",
                value: "Testvej 99",
              },
              {
                label: "Ejer CVR",
                value: "99887766",
              },
              {
                label: "Kapacitet",
                value: "2000",
              },
              {
                label: "Aktuel Sygdomsstatus",
                value: "PRRS Mistanke",
              },
            ],
            _key: "site-details-CHR00401_m2n4v",
            _type: "infoCard",
            title: "Site Basis Info (Vejle Svinefarm)",
          },
          {
            kpis: [
              {
                key: "production",
                label: "Produktion (Ækv.)",
                value: "1800",
              },
              {
                key: "antibiotics",
                label: "Antibiotika (DDD)",
                value: "150",
              },
              {
                key: "transport",
                label: "Transporterede Dyr",
                value: "175",
              },
              {
                key: "rank_mun_site_prod",
                label: "Placering Kommune (Site Prod.)",
                value: "1",
              },
              {
                key: "rank_mun_site_antibiotics",
                label: "Placering Kommune (Site Antibiotika)",
                value: "1",
              },
              {
                key: "rank_mun_site_transport",
                label: "Placering Kommune (Site Transport)",
                value: "1",
              },
            ],
            _key: "site-kpis-CHR00401_h5j8w",
            _type: "kpiGroup",
            title: "Site Nøgletal & Placering",
          },
          {
            iterationConfig: {
              layout: "collapsible",
              titleField: "species_name",
            },
            sections: [
              {
                _key: "site-species-iteration-CHR00401-item-0_q3r6t",
                title: "Svin",
                layout: "collapsible",
                content: [
                  {
                    kpis: [],
                    _key: "species-kpis-CHR00401-101_y9u2i",
                    _type: "kpiGroup",
                    title: "Svin Produktion & Placering",
                  },
                  {
                    data: {
                      xAxis: {
                        label: "year",
                        values: [2021, 2022, 2023],
                      },
                      series: [
                        {
                          name: "Polte",
                          data: [0, 50, 0],
                        },
                        {
                          name: "Slagtesvin",
                          data: [5000, 5500, 6000],
                        },
                        {
                          name: "Smågrise",
                          data: [10000, 0, 12000],
                        },
                        {
                          name: "Søer",
                          data: [0, 200, 210],
                        },
                      ],
                      yAxis: {
                        label: "production_volume_equiv",
                      },
                    },
                    _key: "species-age-chart-CHR00401-101_l7k4m",
                    _type: "stackedBarChart",
                    title: "Svin Produktion pr. År (Aldersgruppe)",
                  },
                ],
              },
              {
                _key: "site-species-iteration-CHR00401-item-5_b8n1p",
                title: "Kvæg",
                layout: "collapsible",
                content: [
                  {
                    kpis: [],
                    _key: "species-kpis-CHR00401-102_v6c9x",
                    _type: "kpiGroup",
                    title: "Kvæg Produktion & Placering",
                  },
                  {
                    data: {
                      xAxis: {
                        label: "year",
                        values: [2021, 2022, 2023],
                      },
                      series: [
                        {
                          name: "Kalve",
                          data: [45, 48, 50],
                        },
                        {
                          name: "Malkekøer",
                          data: [78, 86, 91],
                        },
                        {
                          name: "Opdræt",
                          data: [52, 58, 63],
                        },
                        {
                          name: "Tyr",
                          data: [0, 2, 3],
                        },
                      ],
                      yAxis: {
                        label: "production_volume_equiv",
                      },
                    },
                    _key: "species-age-chart-CHR00401-102_z2f5d",
                    _type: "stackedBarChart",
                    title: "Kvæg Produktion pr. År (Aldersgruppe)",
                  },
                ],
              },
            ],
            _key: "site-species-iteration-CHR00401_a4s7g",
            _type: "iteratedSection",
            title: "Detaljer pr. Dyreart på Site",
          },
        ],
      },
      {
        _key: "animal-welfare-sites-iteration-item-0_e3h8j",
        title: "Vejle Svinefarm 2",
        layout: "collapsible",
        content: [
          {
            items: [
              {
                label: "Site CHR",
                value: "CHR00401",
              },
              {
                label: "Adresse",
                value: "Testvej 99",
              },
              {
                label: "Ejer CVR",
                value: "99887766",
              },
              {
                label: "Kapacitet",
                value: "2000",
              },
              {
                label: "Aktuel Sygdomsstatus",
                value: "PRRS Mistanke",
              },
            ],
            _key: "site-details-CHR00401_2_k9m2w",
            _type: "infoCard",
            title: "Site Basis Info (Vejle Svinefarm)",
          },
          {
            kpis: [
              {
                key: "production",
                label: "Produktion (Ækv.)",
                value: "1800",
              },
              {
                key: "antibiotics",
                label: "Antibiotika (DDD)",
                value: "150",
              },
              {
                key: "transport",
                label: "Transporterede Dyr",
                value: "175",
              },
              {
                key: "rank_mun_site_prod",
                label: "Placering Kommune (Site Prod.)",
                value: "1",
              },
              {
                key: "rank_mun_site_antibiotics",
                label: "Placering Kommune (Site Antibiotika)",
                value: "1",
              },
              {
                key: "rank_mun_site_transport",
                label: "Placering Kommune (Site Transport)",
                value: "1",
              },
            ],
            _key: "site-kpis-CHR00401_2_p5r8t",
            _type: "kpiGroup",
            title: "Site Nøgletal & Placering",
          },
          {
            iterationConfig: {
              layout: "collapsible",
              titleField: "species_name",
            },
            sections: [
              {
                _key: "site-species-iteration-CHR00401-item-0_24_y1u4i",
                title: "Svin 2",
                layout: "collapsible",
                content: [
                  {
                    kpis: [],
                    _key: "species-kpis-CHR00401-101_l6c9x",
                    _type: "kpiGroup",
                    title: "Svin Produktion & Placering",
                  },
                  {
                    data: {
                      xAxis: {
                        label: "year",
                        values: [2021, 2022, 2023],
                      },
                      series: [
                        {
                          name: "Polte",
                          data: [0, 50, 0],
                        },
                        {
                          name: "Slagtesvin",
                          data: [5000, 5500, 6000],
                        },
                        {
                          name: "Smågrise",
                          data: [10000, 0, 12000],
                        },
                        {
                          name: "Søer",
                          data: [0, 200, 210],
                        },
                      ],
                      yAxis: {
                        label: "production_volume_equiv",
                      },
                    },
                    _key: "species-age-chart-CHR00401-101_z2f5d",
                    _type: "stackedBarChart",
                    title: "Svin Produktion pr. År (Aldersgruppe)",
                  },
                ],
              },
              {
                _key: "site-species-iteration-CHR00401-item-5_02_a4s7g",
                title: "Kvæg",
                layout: "collapsible",
                content: [
                  {
                    kpis: [],
                    _key: "species-kpis-CHR00401-102_02_e3h8j",
                    _type: "kpiGroup",
                    title: "Kvæg Produktion & Placering",
                  },
                  {
                    data: {
                      xAxis: {
                        label: "year",
                        values: [2021, 2022, 2023],
                      },
                      series: [
                        {
                          name: "Kalve",
                          data: [45, 48, 50],
                        },
                        {
                          name: "Malkekøer",
                          data: [78, 86, 91],
                        },
                        {
                          name: "Opdræt",
                          data: [52, 58, 63],
                        },
                        {
                          name: "Tyr",
                          data: [0, 2, 3],
                        },
                      ],
                      yAxis: {
                        label: "production_volume_equiv",
                      },
                    },
                    _key: "species-age-chart-CHR00401-102_k9m2w",
                    _type: "stackedBarChart",
                    title: "Kvæg Produktion pr. År (Aldersgruppe)",
                  },
                ],
              },
            ],
            _key: "site-species-iteration-CHR00401_22_p5r8t",
            _type: "iteratedSection",
            title: "Detaljer pr. Dyreart på Site",
          },
        ],
      },
    ],
    _key: "animal-welfare-sites-iteration-2_y1u4i",
    _type: "iteratedSection",
    title: "Detaljer pr. Produktionssted (Site)",
  });

  return <Company company={company} />;
}
