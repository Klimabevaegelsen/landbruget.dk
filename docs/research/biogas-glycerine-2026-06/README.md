# Biogas / glycerine research — June 2026

Investigative research produced during the `mc/explore-biogas-fertiliser-data`
workspace, preserved here so it survives Conductor workspace cleanup (which
discards the gitignored `.context/` scratch directory).

This branch is a storage/archive branch, not meant to merge into `main`
as-is — see "Before merging" below.

**⚠️ Unverified, work-in-progress research.** Every report in `reports/` was
produced by an AI-assisted research pipeline and has not been fact-checked,
edited, or reviewed by a journalist or editor. Claims, figures, and sourcing
may be incomplete, outdated, or wrong. Do not cite, publish, or act on this
content as-is. Landbruget.dk takes no responsibility for its accuracy. Each
report carries the same disclaimer inline.

**PII check.** The `scripts/` CSV panels (`herd_panel_2015_2023.csv`,
`afsat_panel.csv`, and their derived outputs) key only on CVR number — no
farmer or company officer names. The reports themselves name no private
individuals connected to the investigation; the only personal names present
are standard academic-citation surnames (e.g. "Olesen et al.", "Viana, M. B.")
and one named public official quoted from a public EU workshop (Florence
School of Regulation, cited in the GO/biomethane legal analysis) — all public
figures cited for published work, not investigation subjects.

## Contents

- `reports/` — six research reports (RED III GO legal analysis, Arla biogas
  double counting, glycerine subsidy allocation lever, farm-level manure
  dependency on biogas, EU ETS LNG vs. piped gas) plus the shared
  `sources.json` bibliography for the GO/biomethane legal analysis.
- `scripts/` — the two scripts behind the manure-disposition figures cited in
  `research_report_20260609_danish_farm_biogas_manure_dependency.md`, plus
  their small aggregate outputs:
  - `manure_disposition.py`, `disposition_over_time.py`
  - `disposition_over_time.csv`, `manure_disposition_2023.csv` (national/
    quartile-level aggregates, no CVR-level data)

The CVR-level input panels these scripts read (`herd_panel_2015_2023.csv`,
`afsat_panel.csv`, plus `plant_cvr_resolved.csv`, `biogas_plants_2023.csv` for
`manure_disposition.py`) are **not** archived here — they exceed this repo's
1000KB pre-commit file-size limit and duplicate data already derivable from
the project's own pipelines. Regenerate them from `.context/manure_transfer/`
(if that ephemeral workspace still exists) or from the underlying CHR/
Gødningsregnskab pipeline data if the analysis needs to be re-run.

Some citations in the manure-dependency report (e.g. `INVESTIGATION_BRIEF.md`,
`ownership_findings.md`, `exclusivity_findings.md`) point to other files that
were still in the ephemeral `.context/manure_transfer/` workspace and were
**not** migrated here — only the two scripts/CSVs actually needed to
regenerate the cited figures were pulled in.
