# bmd_processed

**Pipeline:** bmd_scraper
**Stage:** silver
**Generated:** 2025-07-05 09:19:45

## Table Overview

- **Row count:** 10,518
- **Columns:** 45

## Columns

| Column | Type | Nullable | Key | Default |
|--------|------|----------|-----|---------|
| produktnavn | VARCHAR | ✓ |  |  |
| registrerings_nr | VARCHAR | ✓ |  |  |
| ufi_kode | VARCHAR | ✓ |  |  |
| eu_registrerings_nr | VARCHAR | ✓ |  |  |
| yderligere_handelsnavne | VARCHAR | ✓ |  |  |
| bekæmpelsesmiddeltype | VARCHAR | ✓ |  |  |
| bruger_pesticid | VARCHAR | ✓ |  |  |
| bruger_biocid | VARCHAR[] | ✓ |  |  |
| produktstatus | VARCHAR | ✓ |  |  |
| godkendelsestype_pesticid | VARCHAR | ✓ |  |  |
| godkendelsestype_biocid | VARCHAR | ✓ |  |  |
| produktgruppe_biocid | VARCHAR | ✓ |  |  |
| produktgruppe_pesticid | VARCHAR | ✓ |  |  |
| formulering | VARCHAR | ✓ |  |  |
| produktformuleringstype | VARCHAR | ✓ |  |  |
| aktivstoftype | VARCHAR | ✓ |  |  |
| aktivstofnavn_e | VARCHAR | ✓ |  |  |
| cas_nr | VARCHAR | ✓ |  |  |
| koncentration_er | VARCHAR | ✓ |  |  |
| enhed_er | VARCHAR | ✓ |  |  |
| frist_for_salg_i_detailled | DATE | ✓ |  |  |
| frist_for_anvendelse_og_besiddelse | DATE | ✓ |  |  |
| godkendelsesindehaver | VARCHAR | ✓ |  |  |
| anvendelse | VARCHAR | ✓ |  |  |
| mindre_anvendelse_nr | VARCHAR[] | ✓ |  |  |
| mindre_anvendelse_godkendelsesindehaver | VARCHAR[] | ✓ |  |  |
| mindre_anvendelse_beskrivelse | VARCHAR | ✓ |  |  |
| godkendelsesdato | DATE | ✓ |  |  |
| udløbsdato | DATE | ✓ |  |  |
| godkendelses_udløbsdato | DATE | ✓ |  |  |
| risikosætninger | VARCHAR | ✓ |  |  |
| farebetegnelse_ild | VARCHAR | ✓ |  |  |
| farebetegnelse_sundhed | VARCHAR | ✓ |  |  |
| farebetegnelse_miljø | VARCHAR | ✓ |  |  |
| ghs_farepiktogrammer | VARCHAR | ✓ |  |  |
| signalord | VARCHAR | ✓ |  |  |
| h_sætninger | VARCHAR[] | ✓ |  |  |
| belastning_miljøeffekt | DOUBLE | ✓ |  |  |
| belastning_miljøadfærd | DOUBLE | ✓ |  |  |
| belastning_sundhed | DOUBLE | ✓ |  |  |
| samlet_belastning | DOUBLE | ✓ |  |  |
| belastning_koncentration | DOUBLE | ✓ |  |  |
| belastningsafgift | DOUBLE | ✓ |  |  |
| belastningsafgiftdato | DATE | ✓ |  |  |
| contains_pfas | BOOLEAN | ✓ |  |  |

## Column Statistics

| Column | Min | Max | Unique | Avg | Null % |
|--------|-----|-----|--------|-----|--------|
| produktnavn | 1+1 Wofasteril SC super | Åffa duepasta | 9113 |  | 0.0% |
| registrerings_nr | 1-10 | 998-1 | 10724 |  | 0.0% |
| ufi_kode | 92GM-N31S-FK2M-2NHT | 94A0-204C-0007-4HM7 | 2 |  | 100.0% |
| eu_registrerings_nr | DK-0012902-0001 1-1 | er EU-0020540-0012 1-8 | 198 |  | 97.0% |
| yderligere_handelsnavne | ALDOVET KOK, EFFIKOK, KOXYFOAM, LERASEPT COC, KOXYSURF, KOXY 250, KRESOKOK, Disinfect Coc, ALDOVET KOK SPEZIAL, STALOKOK, GERMICIDAN KOK FORTE, FaWo Coc Plus, AGAKOK 2.5, AGASURF 2.5 | yladecor Holzschutzlasur BPR Kastanie | 244 |  | 97.3% |
| bekæmpelsesmiddeltype | Biocid | Pesticid | 2 |  | 0.0% |
| bruger_pesticid | Ikke-professionel | Ukendt | 3 |  | 0.0% |
| bruger_biocid | [Industriel] | [Trænet professionel] | 20 |  | 48.5% |
| produktstatus | Produkt afmeldt | Produktet er lovligt | 5 |  | 0.0% |
| godkendelsestype_pesticid | Almindelig | Parallelimport | 5 |  | 77.1% |
| godkendelsestype_biocid | Aktivstof med produkt | Sammenfaldende produkter under godkendelse (NA-BBP) | 17 |  | 42.2% |
| produktgruppe_biocid | Afskrækningsmidler mod myg | Utøj hos husdyr, herunder stuefugle | 10 |  | 0.0% |
| produktgruppe_pesticid | Acaricider | Vækstreguleringsmidler (inkl. spiringshæmmende midler) | 16 |  | 65.8% |
| formulering | Andet | Væske, herunder aerosolspray og pasta | 11 |  | 0.0% |
| produktformuleringstype | Aerosol | Vandopløseligt pulver | 38 |  | 0.0% |
| aktivstoftype | Kemisk | Ukendt | 3 |  | 0.0% |
| aktivstofnavn_e | (E,E)-8, 10-dodecadien-1-ol | Æggepulver; Saccharomyces cerevisiae (gær) | 893 |  | 0.1% |
| cas_nr | 10004-44-1 | Ikke tildelt - Not allocated | 766 |  | 3.9% |
| koncentration_er | 0 | 9; 9 | 1263 |  | 0.1% |
| enhed_er | % v/v | g/l; g/l; g/l; g/l; g/l | 21 |  | 0.1% |
| frist_for_salg_i_detailled | 2001-09-11 | 2035-08-04 | 516 | 2019-01-24 13:18:04.337349 | 84.2% |
| frist_for_anvendelse_og_besiddelse | 2002-03-15 | 2036-01-31 | 600 | 2019-10-24 05:53:03.614458 | 84.2% |
| godkendelsesindehaver | 2022 ENVIRONMENTAL SCIENCE FR SAS | Østerhøjgaard akvakultur apS | 677 |  | 0.0% |
| anvendelse | "Må kun anvendes erhvervsmæssigt til yverdesinfektion af bakterier og gær hos mælkeproducerende dyr efter malkning. Må kun anvendes indendørs.Må ikke anvendes mod andre skadevoldere og ikke i højere doseringer end de i brugsanvisningen nævnte.Opbevares utilgængeligt for børn.Må ikke opbevares sammen med fødevarer, drikkevarer og foderstoffer.” | ”Må kun anvendes mod fluer og hvepse indendørs.Produktet må ved rumsprøjtning ikke benyttes i rum, der rengøres vedbrug af vand.Må ikke anvendes mod andre skadevoldere og ikke i højere doseringer endde i brugsanvisningen nævnte.Må ikke tømmes i kloakløb.Holdes væk fra varme/gnister/åben ild/varme overflader. Rygningforbudt.Spray ikke mod åben ild eller andre antændelseskilder.Levnedsmidler og foderstoffer må ikke forurenes.Opbevares utilgængeligt for børnMå ikke opbevares sammen med fødevarer, drikkevarer og foderstoffer.” | 3530 |  | 13.8% |
| mindre_anvendelse_nr | [1-117-1] | [937-1-1, ' 937-1-2'] | 197 |  | 98.1% |
| mindre_anvendelse_godkendelsesindehaver | [ADAMA Registrations B.V.] | [UPL Holdings Coöperatief U.A.] | 107 |  | 98.1% |
| mindre_anvendelse_beskrivelse | Bekæmpelse af græsukrudt i hundegræs, strandsvingel og alm rajgræs | til vækstregulering i prydplanter og planteskolekulturer dyrket i potter i åbne og lukkede væksthuse og på friland | 144 |  | 98.1% |
| godkendelsesdato | 1980-01-01 | 2035-05-26 | 2886 | 2004-02-19 03:12:33.798641 | 38.4% |
| udløbsdato | 1983-01-01 | 2039-10-31 | 1165 | 2011-06-24 15:47:21.400682 | 38.6% |
| godkendelses_udløbsdato | 1983-01-01 | 2026-08-15 | 1280 | 2004-10-10 13:01:10.027789 | 52.1% |
| risikosætninger | R10 – Brandfarlig | R67 – Dampe kan give sløvhed og svimmelhed; R36 – Irriterer øjnene; R38 – Irriterer huden; R43 – Kan give overfølsomhed ved kontakt med huden; R50 – Meget giftig for organismer, der lever i vand; R53 – Kan forårsage uønskede langtidsvirkninger i vandmiljøet; R65 – Farlig – kan give lungeskade ved indtagelse | 302 |  | 89.9% |
| farebetegnelse_ild | Brandfarlig (B) | Yderst brandfarlig (Fx) | 3 |  | 97.6% |
| farebetegnelse_sundhed | Giftig (T) | Ætsende (C) | 5 |  | 82.7% |
| farebetegnelse_miljø | Miljøfarlig (N) | Miljøfarlig (N) | 1 |  | 92.1% |
| ghs_farepiktogrammer | GHS02 - Flamme | GHS09 - Miljø (dødt træ og død fisk) | 48 |  | 86.6% |
| signalord | Advarsel | Forsigtig | 3 |  | 83.2% |
| h_sætninger | ['Beholder under tryk – kan sprænge ved opvarmning (H229)', ' Brugsanvisningen skal følges for ikke at bringe menneskers sundhed og miljøet i fare (EUH 401)'] | ['Yderst brandfarlig gas (H220)', ' Beholder under tryk – kan sprænge ved opvarmning (H229)', ' Meget giftig med langvarige virkninger for vandlevende organismer (H410)', ' Gentagen kontakt kan give tør eller revnet hud (EUH 066)', ' Udvikler giftig gas ved kontakt med vand (EUH 029)'] | 678 |  | 80.3% |
| belastning_miljøeffekt | 0.0 | 1206.2239 | 1282 | 0.87 | 0.0% |
| belastning_miljøadfærd | 0.0 | 663.039 | 1161 | 0.40 | 0.0% |
| belastning_sundhed | 0.0 | 1.9215 | 133 | 0.04 | 0.0% |
| samlet_belastning | 0.0 | 1206.82275 | 1498 | 1.31 | 0.0% |
| belastning_koncentration | 0.0 | 1.38 | 510 | 0.12 | 0.0% |
| belastningsafgift | 0.0 | 129148.0 | 508 | 132.48 | 0.0% |
| belastningsafgiftdato |  |  | 0 |  | 100.0% |
| contains_pfas | false | true | 2 |  | 0.1% |
