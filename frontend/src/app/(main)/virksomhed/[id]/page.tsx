import { Company } from "@/components/company/company";
import { CompanySkeleton } from "@/components/skeleton/templates/company";
import { getCompanyById } from "@/services/supabase/company";
import { notFound } from "next/navigation";
import { Suspense } from "react";

type Props = {
  params: { id: string };
};

export async function CompanyPage({ id }: { id: string }) {
  const company = await getCompanyById(id);

  if (!company) {
    return notFound();
  }

  company.pageBuilder.push({
    events: [
      {
        date: "2023-11-10T08:00:00.000Z",
        description: "Årlig IBR vaccination.",
        event_type: "Vaccination",
      },
      {
        date: "2023-10-15T09:00:00.000Z",
        description: "Hoste observeret i stald B, prøver taget for PRRS.",
        event_type: "Sygdomsudbrud",
      },
      {
        date: "2023-09-05T14:30:00.000Z",
        description: "Assisteret kælvning, ko 345.",
        event_type: "Kælvningshjælp",
      },
      {
        date: "2023-08-10T13:00:00.000Z",
        description: "Behandling af halt so.",
        event_type: "Skadebehandling",
      },
      {
        date: "2023-07-15T10:00:00.000Z",
        description: "Rutinemæssig kontrol af malkekøer, celletal OK.",
        event_type: "Yversundhedskontrol",
      },
      {
        date: "2023-04-20T11:00:00.000Z",
        description: "Behandling af sommermastitis, ko 211.",
        event_type: "Behandling",
      },
      {
        date: "2023-03-01T09:00:00.000Z",
        description: "Årlig vaccination mod Rødsyge.",
        event_type: "Vaccination",
      },
      {
        date: "2023-01-19T23:00:00.000Z",
        description: "Scanning af polte og søer.",
        event_type: "Drægtighedsscanning",
      },
      {
        date: "2023-01-15T09:00:00.000Z",
        description: "Generel sundhedskontrol efter vinter.",
        event_type: "Kontrolbesøg",
      },
      {
        date: "2022-11-01T09:30:00.000Z",
        description: "Planlagt klovbeskæring af hele besætningen.",
        event_type: "Klovbeskæring",
      },
      {
        date: "2022-09-14T22:00:00.000Z",
        description: "Rutinemæssig klovpleje af søer.",
        event_type: "Klovpleje",
      },
      {
        date: "2022-08-18T16:00:00.000Z",
        description: "Behandling af sår på kalv 801.",
        event_type: "Skadebehandling",
      },
      {
        date: "2022-05-20T11:30:00.000Z",
        description: "Rutinemæssig sundhedskontrol, ingen anmærkninger.",
        event_type: "Kontrolbesøg",
      },
      {
        date: "2022-05-02T07:30:00.000Z",
        description: "Kalvevaccination (RSV/PI3).",
        event_type: "Vaccination",
      },
      {
        date: "2022-02-10T13:00:00.000Z",
        description: "Scanning af kvier.",
        event_type: "Drægtighedsscanning",
      },
      {
        date: "2021-10-25T09:00:00.000Z",
        description: "Rutinemæssig kontrol.",
        event_type: "Yversundhedskontrol",
      },
      {
        date: "2021-06-15T15:00:00.000Z",
        description: "Behandling for lungebetændelse hos kalve.",
        event_type: "Behandling",
      },
    ],
    config: {
      groupByColumns: ["event_type"],
      filterColumns: ["event_type"],
    },
    _key: "site-vet-events-CHR00401",
    _type: "timeline",
    title: "Site Veterinære Hændelser",
  });

  return <Company company={company} />;
}

export default async function CompanyPageWrapper(props: Props) {
  const { id } = await props.params;
  return (
    <Suspense fallback={<CompanySkeleton />}>
      <CompanyPage id={id} />
    </Suspense>
  );
}
