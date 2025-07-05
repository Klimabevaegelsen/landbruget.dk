# Bmd_Scraper Schema Documentation

**Pipeline:** bmd_scraper
**Stage:** silver
**Generated:** 2025-07-05 09:19:45

This document contains schema information for all tables in this pipeline.

## Available Tables

- [bmd_processed](#bmd-processed)

## bmd_processed

```sql
-- Table: bmd_processed
-- Rows: 10,518
-- Columns: 45
--
-- Schema:
--   produktnavn: VARCHAR NULL
--   registrerings_nr: VARCHAR NULL
--   ufi_kode: VARCHAR NULL
--   eu_registrerings_nr: VARCHAR NULL
--   yderligere_handelsnavne: VARCHAR NULL
--   bekæmpelsesmiddeltype: VARCHAR NULL
--   bruger_pesticid: VARCHAR NULL
--   bruger_biocid: VARCHAR[] NULL
--   produktstatus: VARCHAR NULL
--   godkendelsestype_pesticid: VARCHAR NULL
--   godkendelsestype_biocid: VARCHAR NULL
--   produktgruppe_biocid: VARCHAR NULL
--   produktgruppe_pesticid: VARCHAR NULL
--   formulering: VARCHAR NULL
--   produktformuleringstype: VARCHAR NULL
--   aktivstoftype: VARCHAR NULL
--   aktivstofnavn_e: VARCHAR NULL
--   cas_nr: VARCHAR NULL
--   koncentration_er: VARCHAR NULL
--   enhed_er: VARCHAR NULL
--   frist_for_salg_i_detailled: DATE NULL
--   frist_for_anvendelse_og_besiddelse: DATE NULL
--   godkendelsesindehaver: VARCHAR NULL
--   anvendelse: VARCHAR NULL
--   mindre_anvendelse_nr: VARCHAR[] NULL
--   mindre_anvendelse_godkendelsesindehaver: VARCHAR[] NULL
--   mindre_anvendelse_beskrivelse: VARCHAR NULL
--   godkendelsesdato: DATE NULL
--   udløbsdato: DATE NULL
--   godkendelses_udløbsdato: DATE NULL
--   risikosætninger: VARCHAR NULL
--   farebetegnelse_ild: VARCHAR NULL
--   farebetegnelse_sundhed: VARCHAR NULL
--   farebetegnelse_miljø: VARCHAR NULL
--   ghs_farepiktogrammer: VARCHAR NULL
--   signalord: VARCHAR NULL
--   h_sætninger: VARCHAR[] NULL
--   belastning_miljøeffekt: DOUBLE NULL
--   belastning_miljøadfærd: DOUBLE NULL
--   belastning_sundhed: DOUBLE NULL
--   samlet_belastning: DOUBLE NULL
--   belastning_koncentration: DOUBLE NULL
--   belastningsafgift: DOUBLE NULL
--   belastningsafgiftdato: DATE NULL
--   contains_pfas: BOOLEAN NULL
```
