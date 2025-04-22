# Landbruget.dk

The purpose of this project is to organize information about the Danish agricultural sector and make it universally accessible and useful.

![Backend](https://img.shields.io/badge/backend-Python%203.11-green.svg)
![Frontend](https://img.shields.io/badge/frontend-React%2018-blue.svg)
![License](https://img.shields.io/badge/license-CC--BY-green.svg)
[![slack](https://img.shields.io/badge/slack-landbrugsdata-brightgreen.svg?logo=slack)](https://join.slack.com/t/landbrugsdata/shared_invite/zt-2xap8il6o-0YT4IV9sv9t72XZB5pVirA)

## Untraditional and temporary readme as we get started.

There is shitload of data about the externalities of the agricultural sector, but it's hidden behind PDFs, SOAP APIs, WFS endpoints and subject access requests across many governmental agencies.
The goal is to fetch that data, transform it, combine it, and visualise it so it's super easy for anyone - citizens, journalists, activists, farmers and civil servants to understand what's up and to speed up the much needed transformation of our land.

The founder of this project is a noob fumbling around in Cursor, so the code will probably leave a lot to desire. So if you can code, do give us a hand!

Broad ideas (open for input!):
- Fetch and parse data in Python using Github Actions
- Save it in Google Cloud Storage
- Clean it, transform it, combine it in Github Actions or Cloud Run
- Export it as PMTiles and datasets in Google Cloud Storage and Supabase. All data should be joinable on a field, CVR, unique person, parcel and/or CHR.
- There could be data-heavy blogposts as secondary pages.
- Most views could be expected to come from either screenshots or embeds with specific filters applied

Have fun!

## 📊 Data Sources

Data should have one or more of the following attributes to be useful:
- CVR number (company registration number)
- CHR number (herd registration number)
- geospatial coordinates (point or polygon)
- enhedsnummer (CVR individual identifier)
- bfe number (cadaster number)

### Live Sources
1. **Agricultural Fields (WFS)**
   - Updates: Weekly (Mondays 2 AM UTC)
   - Content: Field boundaries, crop types

### Static Sources
All static sources are updated through manual pull requests:
- Animal Welfare: Inspection reports and focus areas
- Biogas: Production data and methane leakage reports
- Fertilizer: Nitrogen data and climate calculations
- Herd Data: CHR (Central Husbandry Register)
- Pesticides: Usage statistics (2021-2023)
- Pig Movements: International transport (2017-2024)
- Subsidies: Support schemes and project grants
- Visa: Agricultural visa statistics
- Wetlands: Areas and carbon content


## 📝 License
This work is licensed under a Creative Commons Attribution 4.0 International License (CC-BY).

## 🙏 Acknowledgments
- Styrelsen for Grøn Arealomlægning og Vandmiljø
- Miljøstyrelsen
- Landbrugstyrelsen
- Energistyrelsen
- Danmarks Meteorologiske Institut
- Geodatastyrelsen
- Klimadatastyrelsen
- Erhvervsstyrelsen
- Københavns Universitet, IFRO
- Aarhus Universitet, DCE
- SEGES & Økologisk Landsforening
- Naturstyrelsen
- Vejdirektoratet
- Beredskabsstyrelsen
- SIRI
