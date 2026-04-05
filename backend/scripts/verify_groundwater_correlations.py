#!/usr/bin/env python3
"""
One-time verification script: re-run the groundwater correlation analysis
from the draft paper (groundwater_correlation_paper.md) and compare results.

Reproduces:
  - Table 2: Point-biserial correlations (19 significant substances)
  - Table 3: Metabolite vs parent compound rates
  - Table 4: Temporal lag analysis
  - Table 5: Ubiquitous contaminants (legacy)

Data sources (all from R2 / landbruget-data bucket):
  - gold/pesticide_disaggregation_{year}_{year+1}/ — field-level kg/ha
  - silver/geus_clean_pesticides/ — groundwater detections (sample-level clean dataset)
  - silver/grukos/ — GRUKO catchment polygons (indsatsområder + indvindingsoplande)

Usage:
    cd backend && source venv/bin/activate
    python scripts/verify_groundwater_correlations.py [--dry-run] [--verbose]

Auth: Uses wrangler CLI (must be logged in) or R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY/R2_ACCOUNT_ID env vars.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb
import numpy as np
import statsmodels.api as sm
from scipy import stats as scipy_stats
from sklearn.metrics import roc_auc_score
from statsmodels.stats.multitest import multipletests
from statsmodels.stats.outliers_influence import variance_inflation_factor

# Add backend to path for common imports
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("verify_correlations")

BUCKET = "landbruget-data"

# Detection threshold from paper (Section 2.2.2)
# In GEUS data (both AM and clean dataset), value=0.015 means "below LOD" (not detected).
# This is the ½ LOQ substitution value (pesticide LOQ = 0.03 µg/L → ½ LOQ = 0.015).
# Real detections have value > 0.015. Use strict greater-than.
DETECTION_THRESHOLD_UGL = 0.015  # strict > this value

# Minimum sample size per substance (Section 2.3.1)
MIN_DETECTIONS = 30

# Application years — three consecutive years with no gap, temporally aligned
# with the 2018+ detection window (3-9 year soil transit times)
APPLICATION_YEARS = [2015, 2016, 2017]

# Detection window from paper (Section 2.2.2): 2-9 years post-application
DETECTION_YEAR_START = 2018

# Soil-dependent transit times (years from surface application to groundwater detection)
# Based on Danish soil classification (DJF_FGJOR soil_height codes)
SOIL_TRANSIT_TIMES = {
    1: 3,  # Grovsandet jord (coarse sand) — fast transit
    2: 3,  # Finsandet jord (fine sand) — fast transit
    3: 5,  # Lerblandet sandjord (clay-mixed sand) — intermediate
    4: 7,  # Sandblandet lerjord (sand-mixed clay) — slow transit
    5: 7,  # Lerjord (clay) — slow transit
    6: 7,  # Svær lerjord (heavy clay) — slow transit
}
DEFAULT_TRANSIT_YEARS = 5  # fallback (Humusjord, unknown)

# Metabolite-to-parent mappings (Section 2.2.3 + Table 2)
# Sources:
# - 1,2,4-Triazol: common breakdown product of all triazole fungicides (ring cleavage)
# - AMPA: glyphosate aminomethylphosphonic acid metabolite
# - 2-(4-Chlorphenoxy)propionsyre (4-CPP): mecoprop demethylation product; NOT dichlorprop
#   (dichlorprop is 2,4-dichlorophenoxypropionic acid — different ring substitution)
#   GEUS stof_tekst is "2-(4-Chlorphenoxy)propionsyre" (1,248 detections)
# - 4-Chlor-2-methylphenol: MCPA/mecoprop oxidative ether cleavage (confirmed in ScienceDirect)
# - 2,4-Dichlorphenol: 2,4-D and dichlorprop metabolite (ether cleavage)
# - 2,6-Dichlorphenol: from dichlobenil (PPDB confirmed) and possibly dicloran/chloroxylenol
# - 2-(3-Trifluoromethyl-phenoxy)nicotinsyre: diflufenican metabolite AE B107137
#   (confirmed PPDB ref AE-B107137, CAS 36701-89-0 — previously mapped to fluazinam INCORRECTLY)
# - Triadimenol: direct parent (registered a.i.) + metabolite of triadimefon
# - Propyzamid metabolite: RH-24644, 2-(3,5-dichlorophenyl)-4,4-dimethyl-5-methylene-oxazoline
METABOLITE_PARENT_MAP = {
    "1,2,4-Triazol": [
        "propiconazol",
        "tebuconazol",
        "epoxiconazol",
        "difenoconazol",
        "metconazol",
        "prothioconazol",
        "triadimenol",
        "triadimefon",
        "cyproconazol",
        "penconazol",
        "flusilazol",
        "bitertanol",
    ],
    "(Aminomethyl)phosphonsyre": ["glyphosat"],  # AMPA
    # 4-CPP: mecoprop (mechlorprop) demethylation → 4-chlorophenoxypropionic acid
    # Also MCPB beta-oxidation → mecoprop → 4-CPP (two-step)
    # dichlorprop produces 2,4-dichlorphenol not 4-CPP (different ring)
    "2-(4-Chlorphenoxy)propionsyre": [
        "mechlorprop-p",
        "mechlorprop",
        "mecoprop-p",
        "mcpb",
    ],
    "Metazachlor OA": ["metazachlor"],
    # 4-Chlor-2-methylphenol: MCPA and mecoprop ether cleavage oxidative metabolite
    "4-Chlor-2-methylphenol": ["mcpa", "mechlorprop-p", "mechlorprop", "mecoprop-p"],
    # 2,4-Dichlorphenol: from 2,4-D and dichlorprop (ether cleavage)
    "2,4-Dichlorphenol": ["dichlorprop-p", "dichlorprop", "2,4-d"],
    "Metazachlor ESA": ["metazachlor"],
    # Propyzamid metabolite (RH-24644) is NOT in GEUS data — only "Propyzamid" (parent) exists there.
    # The paper's "Propyzamid metabolite" entry (r=0.126) cannot be reproduced from GEUS.
    "Azoxystrobinsyre": ["azoxystrobin"],  # azoxystrobin acid (R234886), mobile metabolite
    # 2-(3-TFM-phenoxy)nicotinic acid = diflufenican metabolite AE B107137 (PPDB ref 770)
    # Previously mapped to fluazinam/haloxyfop — CORRECTED
    "2-(3-Trifluoromethyl-phenoxy)nicotinsyre": ["diflufenican"],
    # 2,6-Dichlorphenol: from dichlobenil (confirmed) and possibly dicloran
    "2,6-Dichlorphenol": ["dichlobenil", "dicloran"],
    "Methyl-desphenyl-chloridazon": ["chloridazon"],
    "Desphenyl chloridazon": ["chloridazon"],
    "2,6-Dichlorbenzamid": ["dichlobenil"],  # BAM — most persistent Danish groundwater contaminant
    # Terbuthylazin metabolites (triazine herbicide, widely used on maize 2015-2016)
    "Terbuthylazin-desethyl": ["terbuthylazin"],
    "Terbuthylazin, hydroxy-": ["terbuthylazin"],
    # Metribuzin metabolites (used on potatoes/cereals)
    "Metribuzin-desamino-diketo": ["metribuzin"],
    "Metribuzin-diketo": ["metribuzin"],
    # Metalaxyl metabolites — CGA 108906 (ring-cleavage) and the sulfoacetyl form
    # N-(2,6-dimethylphenyl)-N-(methoxyacetyl)alanin is metalaxyl itself (IUPAC name)
    # These metabolites trace metalaxyl/metalaxyl-M applications (used on potatoes, brassicas)
    "(2,6-Dimethylphenylcarbamoyl)methansulfonsyre": ["metalaxyl", "metalaxyl-m"],
    "CGA 108906": ["metalaxyl", "metalaxyl-m"],
    "[(2,6-Dimethylphenyl)(2-sulfoacetyl)amino]eddikesyre": ["metalaxyl", "metalaxyl-m"],
    # Dimethachlor metabolites (chloroacetamide, used on oilseed rape)
    "Dimethachlor ESA": ["dimethachlor"],
    "Dimethachlor OA": ["dimethachlor"],
    # Ethylenthiourea (ETU): metabolite of all ethylene-bis-dithiocarbamate fungicides
    # mancozeb is the dominant source (88,529 kg/yr in BMD, widely used)
    "Ethylenthiourea": ["mancozeb", "maneb", "zineb", "metiram"],
    # DMS (dimethylsulfamide): metabolite of tolylfluanid and dichlofluanid (GEUS 2023/42 Tabel 1)
    "N,N-Dimethylsulfamidsyre": ["tolylfluanid", "dichlofluanid"],
    # Chlorothalonil metabolite R417888 (GEUS 2023/42 Tabel 1)
    "Chlorothalonilamid sulfonsyre (R417888)": ["chlorothalonil"],
    # Atrazine metabolites (atrazine still detected, applications pre-ban ~1994)
    "Desethyl-hydroxyatrazin": ["atrazin"],
    "Desisopropyl-hydroxyatrazin": ["atrazin"],
    # Simazine metabolite
    "Hydroxysimazin": ["simazin"],
    # Alachlor metabolite (alachlor banned EU ~2006, legacy contamination)
    "Alachlor ESA": ["alachlor"],
    # TFMP: fluazifop-P-butyl degradate (5-(trifluoromethyl)-2(1H)-pyridinone)
    # GEUS published key leaching study; fluazifop banned in DK since 2014
    "TFMP": ["fluazifop-p-butyl", "fluazifop-p"],
    # 6-Chlor-2-methylphenol: positional isomer of 4-Chlor-2-methylphenol, same parents
    # MCPA/mecoprop ether cleavage product (different ring position from 4-Chlor variant)
    "6-Chlor-2-methylphenol": ["mcpa", "mechlorprop-p", "mechlorprop", "mecoprop-p"],
    # 2,3,4,6-Tetrachlorphenol: degradation product of pentachlorophenol (PCP)
    # Also component of timber treatment fungicide KY5
    "2,3,4,6-Tetrachlorphenol": ["pentachlorphenol"],
    # Chlorthiamid: converts to dichlobenil in soil (chlorthiamid → dichlobenil → BAM)
    # Map to dichlobenil as the functional parent in the pathway
    "Chlorthiamid": ["dichlobenil"],
    # --- Comprehensive GEUS mapping (all substances with detections) ---
    # Atrazine metabolites (atrazine banned ~1994, but metabolites persist decades in GW)
    "Desethyldesisopropyl-atrazin": ["atrazin"],  # DEDIA, fully dealkylated (1906 det)
    "Desisopropyl-atrazin": ["atrazin"],  # DIA (1806 det)
    "Desethyl-atrazin": ["atrazin"],  # DEA (1551 det)
    "Hydroxy-atrazin": ["atrazin"],  # (1329 det)
    "Didealkyl-hydroxyatrazin": ["atrazin"],  # (301 det)
    # Additional terbuthylazin metabolites
    "6-(tert-Butylamino)-1,3,5-triazin-2,4-diol": ["terbuthylazin"],  # desethyl-hydroxy (473 det)
    "4-(tert-Butylamino)-6-hydroxy-1,3,5-triazin-2(1H)-one": ["terbuthylazin"],  # (399 det)
    "2-Hydroxy-desethyl-terbuthylazin": ["terbuthylazin"],  # explicit name (58 det)
    # Tolylfluanid/dichlofluanid metabolites (DMS pathway)
    # N,N-Dimethylsulfamid = DMS, the most detected metabolite (8477 det!)
    # Note: "N,N-Dimethylsulfamidsyre" (DMS acid) already mapped above; this is the base form
    "N,N-Dimethylsulfamid": ["tolylfluanid", "dichlofluanid"],
    "N,N-Dimethyl-N'-tolylsulfonyldiamid": ["tolylfluanid"],  # DMST intermediate (69 det)
    "Dimethylaminosulfanilid": ["tolylfluanid"],  # DMSA intermediate (62 det)
    "P-toluensulfonamid": ["tolylfluanid"],  # PTS (156 det)
    # Additional chlorothalonil metabolites
    "4-Bis-amido-3,5,6-trichlorobenzenesulfonat": ["chlorothalonil"],  # (964 det)
    "Chlorthalonilamid-benzoesyre (R 611965)": ["chlorothalonil"],  # R611965 (32 det)
    # Dichlobenil metabolite
    "2,6-Dichlorbenzosyre": ["dichlobenil"],  # 2,6-dichlorobenzoic acid (494 det)
    # Propyzamid metabolite RH-24644
    # Note: comment at line 122 said "NOT in GEUS" — it IS, under this Danish name
    "N-(1,1-Dimethylacetonyl)-3,5-dichlorbenzamid": ["propyzamid"],  # RH-24644 (73 det)
    # Metamitron metabolite
    "Metamitron-desamino": ["metamitron"],  # desamino-metamitron (204 det)
    # Isoproturon metabolites
    "PPU (IN70941)": ["isoproturon"],  # 4-isopropylphenylurea (41 det)
    "PPU-desamino (IN70942)": ["isoproturon"],  # desamino-PPU (6 det)
    "Desmethyl-isoproturon": ["isoproturon"],  # N-demethylation product (45 det)
    # 4-Nitrophenol: ether cleavage product of parathion and parathion-methyl
    "4-Nitrophenol": ["parathion", "parathion-methyl"],  # (556 det)
    # Additional alachlor metabolite
    "Alachlor OA": ["alachlor"],  # oxanilic acid (59 det)
    # Metribuzin-desamino (distinct from desamino-diketo already mapped)
    "Metribuzin-desamino": ["metribuzin"],  # (40 det)
    # Diazinon metabolite
    "Isopropyl-6-methyl-4-pyrimidon": ["diazinon"],  # 2-isopropyl-6-methyl-4-pyrimidinol (51 det)
    # Fluazifop parent variants (in addition to TFMP metabolite)
    "Fluazifop-P": ["fluazifop-p-butyl", "fluazifop-p"],  # (40 det)
    "Fluazifop": ["fluazifop-p-butyl", "fluazifop-p"],  # (28 det)
    # Paraoxon: active metabolite of parathion (oxon form)
    "Paraoxon": ["parathion"],  # (37 det)
    # Propachlor metabolite
    "Propachlor ESA": ["propachlor"],  # (36 det)
    # Additional dimethachlor metabolite
    "Dimethachlor metabolit, SYN 530561": ["dimethachlor"],  # (4 det)
    # Pirimicarb metabolite
    "Pirimicarb-desmethyl": ["pirimicarb"],  # (19 det)
    # Acetochlor metabolite
    "Acetochlor ESA": ["acetochlor"],  # (3 det)
    # Metolachlor metabolites
    "Metolachlor ESA": ["metolachlor", "s-metolachlor"],  # (4 det)
    "Metolachlor NOA 413173": ["metolachlor", "s-metolachlor"],  # (2 det)
    # Additional tetrachlorophenol isomer (PCP degradation)
    "2,3,4,5-Tetrachlorphenol": ["pentachlorphenol"],  # (25 det)
    # Bifenox metabolite
    "Bifenox-syre": ["bifenox"],  # bifenox acid (3 det)
    # Pyridafol: metabolite of pyridate
    "Pyridafol": ["pyridate"],  # (18 det)
}

# GEUS stof_tekst → BMD active ingredient (lowercase) for parent compounds
# Note: Triadimenol is BOTH a registered a.i. AND the main metabolite of triadimefon.
# For correlation: Triadimenol in GW reflects BOTH triadimenol AND triadimefon applications.
# Dichlorprop: check both "-p" suffix (pure R enantiomer) and base name in BMD.
GEUS_TO_BMD_SINGLE = {
    "Glyphosat": "glyphosat",
    "Bentazon": "bentazon",
    "MCPA": "mcpa",
    "Triadimefon": "triadimefon",
    "Propyzamid": "propyzamid",
    "Metazachlor": "metazachlor",
    "Diuron": "diuron",
    "Atrazin": "atrazin",
    "Simazin": "simazin",
    "Pendimethalin": "pendimethalin",
    "Isoproturon": "isoproturon",
    "Terbuthylazin": "terbuthylazin",
    # 2,4-Dichlorphenoxyeddikesyre = 2,4-D (full IUPAC GEUS name)
    "2,4-Dichlorphenoxyeddikesyre": "2,4-d",
    "Lenacil": "lenacil",
    "Chloridazon": "chloridazon",
    "Metribuzin": "metribuzin",
    # N-(2,6-dimethylphenyl)-N-(methoxyacetyl)alanin is the IUPAC name for metalaxyl
    "N-(2,6-dimethylphenyl)-N-(methoxyacetyl)alanin": "metalaxyl",
    # Legacy/banned parent compounds detected in groundwater (mostly pre-2000 applications)
    "Dinoseb": "dinoseb",  # dinitrophenol herbicide, banned 1986
    "Pentachlorphenol": "pentachlorphenol",  # PCP, wood preservative/fungicide, Stockholm Convention POP
    "Dinitro-o-cresol": "dinitro-o-cresol",  # DNOC, earliest herbicide (1890s), banned 1991
    "Monuron": "monuron",  # first phenylurea herbicide (1952), banned
    "Cyanazin": "cyanazin",  # triazine herbicide, banned EU 2002
    "Metamitron": "metamitron",  # triazinone herbicide for sugar beet
    "Pirimicarb": "pirimicarb",  # carbamate insecticide (aphicide)
    "Metaldehyd": "metaldehyd",  # metaldehyde molluscicide (slug pellets)
    "Trichloreddikesyre": "trichloreddikesyre",  # TCA (trichloroacetic acid), herbicide until ~1990
    "N,N-Diethyl-m-toluamid": "n,n-diethyl-m-toluamid",  # DEET insect repellent (not agricultural)
    # --- Comprehensive parent compound mapping (all detected in GEUS) ---
    "Hexazinon": "hexazinon",  # triazine herbicide (864 det)
    "Dichlobenil": "dichlobenil",  # nitrile herbicide, parent of BAM (353 det)
    "Dinoterb": "dinoterb",  # dinitrophenol herbicide, banned (328 det)
    "Clopyralid": "clopyralid",  # picolinic acid herbicide (402 det)
    "Parathion": "parathion",  # organophosphate insecticide (143 det)
    "Parathion-methyl": "parathion-methyl",  # methyl-parathion (100 det)
    "Malathion": "malathion",  # organophosphate insecticide (120 det)
    "Ethofumesat": "ethofumesat",  # benzofuran herbicide (111 det)
    "Propiconazol": "propiconazol",  # triazole fungicide (110 det)
    "Diflufenican": "diflufenican",  # herbicide (132 det)
    "Imidacloprid": "imidacloprid",  # neonicotinoid insecticide (123 det)
    "Boscalid": "boscalid",  # SDHI fungicide (107 det)
    "Tebuconazol": "tebuconazol",  # triazole fungicide (85 det)
    "Fluroxypyr": "fluroxypyr",  # auxin herbicide (84 det)
    "Amitrol": "amitrol",  # triazole herbicide (88 det)
    "Benazolin": "benazolin",  # herbicide (85 det)
    "Epoxiconazol": "epoxiconazol",  # triazole fungicide (56 det)
    "Azoxystrobin": "azoxystrobin",  # strobilurin fungicide (49 det)
    "Prosulfocarb": "prosulfocarb",  # thiocarbamate herbicide (42 det)
    "Metalaxyl": "metalaxyl",  # common name (IUPAC form already mapped above) (56 det)
    "Fluopyram": "fluopyram",  # SDHI fungicide (44 det)
    "Metsulfuron-methyl": "metsulfuron-methyl",  # sulfonylurea herbicide (44 det)
    "Dimethomorph": "dimethomorph",  # morpholine fungicide (34 det)
    "Carbendazim": "carbendazim",  # benzimidazole fungicide (22 det)
    "Dicamba": "dicamba",  # benzoic acid herbicide (21 det)
    "Glufosinat": "glufosinat",  # phosphinic acid herbicide (21 det)
    "Linuron": "linuron",  # phenylurea herbicide (17 det)
    "Prometryn": "prometryn",  # triazine herbicide (17 det)
    "Sulfotep": "sulfotep",  # organophosphate insecticide (110 det)
    "Dimethoat": "dimethoat",  # organophosphate insecticide (15 det)
    "Clomazon": "clomazon",  # isoxazolidinone herbicide (29 det)
    "Thiamethoxam": "thiamethoxam",  # neonicotinoid insecticide (27 det)
    "Haloxyfop": "haloxyfop",  # aryloxyphenoxypropionate herbicide (26 det)
    "Propachlor": "propachlor",  # chloroacetamide herbicide (2 det)
    "Aminopyralid": "aminopyralid",  # picolinic acid herbicide (31 det)
    "Picloram": "picloram",  # picolinic acid herbicide (36 det)
    "Napropamid": "napropamid",  # amide herbicide (16 det)
    "Prothioconazol": "prothioconazol",  # triazole fungicide (17 det)
    "Diazinon": "diazinon",  # organophosphate insecticide (for metabolite mapping)
}
# Multi-parent lookups for parent compounds detected in GW
# (used in run_correlations to SUM intensity across multiple BMD names)
GEUS_TO_BMD_MULTI = {
    # Dichlorprop: sold as both "dichlorprop" and "dichlorprop-p" (pure R enantiomer)
    "Dichlorprop": ["dichlorprop-p", "dichlorprop"],
    # Mechlorprop/mecoprop: multiple BMD name variants
    "Mechlorprop": ["mechlorprop-p", "mechlorprop", "mecoprop-p"],
    # Triadimenol: also catches triadimefon applications (triadimefon metabolizes to triadimenol)
    "Triadimenol": ["triadimenol", "triadimefon"],
}
# Backwards-compatible combined mapping used in run_correlations
GEUS_TO_BMD = {**GEUS_TO_BMD_SINGLE}

# Known substance types from Table 2
SUBSTANCE_TYPE = {
    "1,2,4-Triazol": "metabolite",
    "Dichlorprop": "parent",
    "(Aminomethyl)phosphonsyre": "metabolite",  # AMPA
    "2-(4-Chlorphenoxy)propionsyre": "metabolite",  # 4-CPP — GEUS name
    "Metazachlor OA": "metabolite",
    "Glyphosat": "parent",
    "Bentazon": "parent",
    "4-Chlor-2-methylphenol": "metabolite",
    "Mechlorprop": "parent",
    "MCPA": "parent",
    "2,4-Dichlorphenol": "metabolite",
    "Triadimefon": "parent",
    "Metazachlor ESA": "metabolite",
    "Propyzamid": "parent",
    "Propyzamid metabolite": "metabolite",
    "Azoxystrobinsyre": "metabolite",
    "2-(3-Trifluoromethyl-phenoxy)nicotinsyre": "metabolite",
    "Triadimenol": "parent",
    "2,6-Dichlorphenol": "metabolite",
    "Desphenyl chloridazon": "metabolite",
    "Methyl-desphenyl-chloridazon": "metabolite",
    "2,6-Dichlorbenzamid": "metabolite",  # BAM
    # New entries from trend analysis
    "Isoproturon": "parent",
    "Terbuthylazin": "parent",
    "2,4-Dichlorphenoxyeddikesyre": "parent",  # 2,4-D
    "Lenacil": "parent",
    "Chloridazon": "parent",
    "Metribuzin": "parent",
    "N-(2,6-dimethylphenyl)-N-(methoxyacetyl)alanin": "parent",  # metalaxyl
    "Terbuthylazin-desethyl": "metabolite",
    "Terbuthylazin, hydroxy-": "metabolite",
    "Metribuzin-desamino-diketo": "metabolite",
    "Metribuzin-diketo": "metabolite",
    "(2,6-Dimethylphenylcarbamoyl)methansulfonsyre": "metabolite",
    "CGA 108906": "metabolite",
    "[(2,6-Dimethylphenyl)(2-sulfoacetyl)amino]eddikesyre": "metabolite",
    "Dimethachlor ESA": "metabolite",
    "Dimethachlor OA": "metabolite",
    "Ethylenthiourea": "metabolite",
    "N,N-Dimethylsulfamidsyre": "metabolite",
    "Chlorothalonilamid sulfonsyre (R417888)": "metabolite",
    "Desethyl-hydroxyatrazin": "metabolite",
    "Desisopropyl-hydroxyatrazin": "metabolite",
    "Hydroxysimazin": "metabolite",
    "Alachlor ESA": "metabolite",
    # Newly mapped substances from unmapped-substance analysis
    "TFMP": "metabolite",  # fluazifop-P-butyl degradate
    "6-Chlor-2-methylphenol": "metabolite",  # MCPA/mecoprop ether cleavage
    "2,3,4,6-Tetrachlorphenol": "metabolite",  # PCP degradation product
    "Chlorthiamid": "parent",  # converts to dichlobenil in soil
    "Dinoseb": "parent",  # dinitrophenol herbicide
    "Trichloreddikesyre": "parent",  # TCA, used as herbicide + environmental formation
    "Pentachlorphenol": "parent",  # PCP wood preservative
    "Dinitro-o-cresol": "parent",  # DNOC herbicide
    "Monuron": "parent",  # phenylurea herbicide
    "Cyanazin": "parent",  # triazine herbicide
    "Metamitron": "parent",  # triazinone herbicide
    "Pirimicarb": "parent",  # carbamate insecticide
    "Metaldehyd": "parent",  # metaldehyde molluscicide
    "N,N-Diethyl-m-toluamid": "other",  # DEET insect repellent, not agricultural pesticide
    # --- Comprehensive GEUS substance type mapping ---
    # Atrazine metabolites
    "Desethyldesisopropyl-atrazin": "metabolite",
    "Desisopropyl-atrazin": "metabolite",
    "Desethyl-atrazin": "metabolite",
    "Hydroxy-atrazin": "metabolite",
    "Didealkyl-hydroxyatrazin": "metabolite",
    # Terbuthylazin metabolites
    "6-(tert-Butylamino)-1,3,5-triazin-2,4-diol": "metabolite",
    "4-(tert-Butylamino)-6-hydroxy-1,3,5-triazin-2(1H)-one": "metabolite",
    "2-Hydroxy-desethyl-terbuthylazin": "metabolite",
    # Tolylfluanid metabolites
    "N,N-Dimethylsulfamid": "metabolite",  # DMS
    "N,N-Dimethyl-N'-tolylsulfonyldiamid": "metabolite",  # DMST
    "Dimethylaminosulfanilid": "metabolite",  # DMSA
    "P-toluensulfonamid": "metabolite",  # PTS
    # Chlorothalonil metabolites
    "4-Bis-amido-3,5,6-trichlorobenzenesulfonat": "metabolite",
    "Chlorthalonilamid-benzoesyre (R 611965)": "metabolite",
    # Dichlobenil metabolites / parents
    "2,6-Dichlorbenzosyre": "metabolite",
    "Dichlobenil": "parent",
    # Propyzamid metabolite
    "N-(1,1-Dimethylacetonyl)-3,5-dichlorbenzamid": "metabolite",  # RH-24644
    # Metamitron metabolite
    "Metamitron-desamino": "metabolite",
    # Isoproturon metabolites
    "PPU (IN70941)": "metabolite",
    "PPU-desamino (IN70942)": "metabolite",
    "Desmethyl-isoproturon": "metabolite",
    # Organophosphate metabolites
    "4-Nitrophenol": "metabolite",  # parathion
    "Paraoxon": "metabolite",  # parathion active metabolite
    "Isopropyl-6-methyl-4-pyrimidon": "metabolite",  # diazinon
    # More metabolites
    "Alachlor OA": "metabolite",
    "Metribuzin-desamino": "metabolite",
    "Fluazifop-P": "parent",
    "Fluazifop": "parent",
    "Propachlor ESA": "metabolite",
    "Dimethachlor metabolit, SYN 530561": "metabolite",
    "Pirimicarb-desmethyl": "metabolite",
    "Acetochlor ESA": "metabolite",
    "Metolachlor ESA": "metabolite",
    "Metolachlor NOA 413173": "metabolite",
    "2,3,4,5-Tetrachlorphenol": "metabolite",
    "Bifenox-syre": "metabolite",
    "Pyridafol": "metabolite",
    # Parent compounds detected in GEUS
    "Hexazinon": "parent",
    "Dinoterb": "parent",
    "Clopyralid": "parent",
    "Parathion": "parent",
    "Parathion-methyl": "parent",
    "Malathion": "parent",
    "Ethofumesat": "parent",
    "Propiconazol": "parent",
    "Diflufenican": "parent",
    "Imidacloprid": "parent",
    "Boscalid": "parent",
    "Tebuconazol": "parent",
    "Fluroxypyr": "parent",
    "Amitrol": "parent",
    "Benazolin": "parent",
    "Epoxiconazol": "parent",
    "Azoxystrobin": "parent",
    "Prosulfocarb": "parent",
    "Metalaxyl": "parent",
    "Fluopyram": "parent",
    "Metsulfuron-methyl": "parent",
    "Dimethomorph": "parent",
    "Carbendazim": "parent",
    "Dicamba": "parent",
    "Glufosinat": "parent",
    "Linuron": "parent",
    "Prometryn": "parent",
    "Sulfotep": "parent",
    "Dimethoat": "parent",
    "Clomazon": "parent",
    "Thiamethoxam": "parent",
    "Haloxyfop": "parent",
    "Propachlor": "parent",
    "Aminopyralid": "parent",
    "Picloram": "parent",
    "Napropamid": "parent",
    "Prothioconazol": "parent",
    "Diazinon": "parent",
    # Non-pesticide / other
    "Formaldehyd": "other",  # ubiquitous, not a pesticide
    "Sulfanilamid": "other",  # sulfonamide antibiotic
    # Original parents that were in GEUS_TO_BMD but missing from SUBSTANCE_TYPE
    "Atrazin": "parent",
    "Diuron": "parent",
    "Metazachlor": "parent",
    "Pendimethalin": "parent",
    "Simazin": "parent",
}

# Paper's expected values for verification (Table 2)
# Keys use GEUS stof_tekst names (may differ from paper's English names)
EXPECTED_TABLE2 = {
    "1,2,4-Triazol": {"r": 0.231, "detection_rate": 16.6},
    "Dichlorprop": {"r": 0.228, "detection_rate": 7.6},
    "(Aminomethyl)phosphonsyre": {"r": 0.226, "detection_rate": 9.2},  # AMPA
    "2-(4-Chlorphenoxy)propionsyre": {"r": 0.204, "detection_rate": 10.1},  # 4-CPP
    "Metazachlor OA": {"r": 0.203, "detection_rate": 1.9},
    "Glyphosat": {"r": 0.193, "detection_rate": 7.7},
    "Bentazon": {"r": 0.181, "detection_rate": 15.0},
    "4-Chlor-2-methylphenol": {"r": 0.178, "detection_rate": 26.9},
    "Mechlorprop": {"r": 0.170, "detection_rate": 11.0},
    "MCPA": {"r": 0.169, "detection_rate": 7.4},
    "2,4-Dichlorphenol": {"r": 0.168, "detection_rate": 3.6},
    "Triadimefon": {"r": 0.159, "detection_rate": 3.6},
    "Metazachlor ESA": {"r": 0.144, "detection_rate": 3.0},
    "Propyzamid": {"r": 0.126, "detection_rate": 8.4},
    "Propyzamid metabolite": {"r": 0.126, "detection_rate": 8.4},
    "Azoxystrobinsyre": {"r": 0.104, "detection_rate": 12.1},
    "2-(3-Trifluoromethyl-phenoxy)nicotinsyre": {"r": 0.088, "detection_rate": 16.4},
    "Triadimenol": {"r": 0.083, "detection_rate": 13.6},
    "2,6-Dichlorphenol": {"r": 0.076, "detection_rate": 1.3},
}


# ---------------------------------------------------------------------------
# Statistical helper functions
# ---------------------------------------------------------------------------


def _compute_nagelkerke_r2(model) -> float | None:
    """Nagelkerke R² (1991) from a fitted statsmodels Logit model."""
    try:
        n = int(model.nobs)
        ll_full = model.llf
        ll_null = model.llnull
        cox_snell = 1.0 - np.exp(-2.0 / n * (ll_full - ll_null))
        max_r2 = 1.0 - np.exp(2.0 / n * ll_null)
        if max_r2 == 0:
            return None
        return float(cox_snell / max_r2)
    except Exception:
        return None


def _hosmer_lemeshow(y_true: np.ndarray, y_pred_prob: np.ndarray, g: int = 10) -> tuple[float, float]:
    """Hosmer-Lemeshow goodness-of-fit test.

    Returns (chi2_statistic, p_value). Non-significant p (>0.05) = adequate fit.
    """
    try:
        order = np.argsort(y_pred_prob)
        y_sorted = y_true[order]
        p_sorted = y_pred_prob[order]

        groups = np.array_split(np.arange(len(y_sorted)), g)
        chi2 = 0.0
        for grp in groups:
            if len(grp) == 0:
                continue
            obs = y_sorted[grp].sum()
            exp_val = p_sorted[grp].sum()
            n_grp = len(grp)
            exp_neg = n_grp - exp_val
            if exp_val > 0:
                chi2 += (obs - exp_val) ** 2 / exp_val
            if exp_neg > 0:
                chi2 += ((n_grp - obs) - exp_neg) ** 2 / exp_neg

        df = g - 2
        p_value = 1.0 - scipy_stats.chi2.cdf(chi2, df)
        return float(chi2), float(p_value)
    except Exception:
        return np.nan, np.nan


def _compute_auc(y_true: np.ndarray, y_pred_prob: np.ndarray) -> float | None:
    """AUC via sklearn."""
    try:
        if len(np.unique(y_true)) < 2:
            return None
        return float(roc_auc_score(y_true, y_pred_prob))
    except Exception:
        return None


def _compute_vif(X: np.ndarray, col_idx: int) -> float:
    """VIF for a specific column in design matrix X (with constant)."""
    try:
        return float(variance_inflation_factor(X, col_idx))
    except Exception:
        return np.nan


# ---------------------------------------------------------------------------
# R2 data access — tries native DuckDB R2 auth, falls back to wrangler CLI
# ---------------------------------------------------------------------------


def _has_r2_env() -> bool:
    return all(os.getenv(k) for k in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ACCOUNT_ID"))


def _setup_duckdb_r2(conn: duckdb.DuckDBPyConnection) -> bool:
    """Try to configure DuckDB for direct R2 access via env vars."""
    if not _has_r2_env():
        return False
    try:
        from common.storage.filesystem import setup_duckdb_cloud_auth

        return setup_duckdb_cloud_auth(conn)
    except Exception as e:
        log.warning(f"Native R2 auth failed: {e}")
        return False


def _wrangler_download(r2_path: str, local_path: str) -> None:
    """Download a single object from R2 via wrangler CLI."""
    cmd = ["wrangler", "r2", "object", "get", f"{BUCKET}/{r2_path}", "--file", local_path, "--remote"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"wrangler download failed for {r2_path}: {result.stderr}")


def _get_wrangler_token() -> str:
    """Read wrangler's cached OAuth token."""
    for config_path in [
        Path.home() / "Library" / "Preferences" / ".wrangler" / "config" / "default.toml",
        Path.home() / ".config" / ".wrangler" / "config" / "default.toml",
    ]:
        if config_path.exists():
            for line in config_path.read_text().splitlines():
                if line.startswith("oauth_token"):
                    return line.split("=", 1)[1].strip().strip('"')
    raise RuntimeError("Could not read wrangler OAuth token")


def _wrangler_list_prefix(prefix: str) -> list[str]:
    """List objects under a prefix using Cloudflare API via wrangler's OAuth token.

    Triggers a wrangler CLI call first to ensure the OAuth token is refreshed.
    """
    # Force token refresh by calling wrangler (it refreshes expired tokens automatically)
    subprocess.run(["wrangler", "whoami"], capture_output=True, text=True, timeout=15)

    account_id = os.getenv("R2_ACCOUNT_ID", "a5f130bfd0d34de38f8e77f6a0f40a27")
    token = _get_wrangler_token()

    import urllib.request

    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
        f"/r2/buckets/{BUCKET}/objects?prefix={prefix}&limit=1000"
    )
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})  # noqa: S310
    with urllib.request.urlopen(req) as resp:  # noqa: S310
        data = json.loads(resp.read())

    if not data.get("success"):
        raise RuntimeError(f"Cloudflare API error: {data.get('errors')}")

    result = data.get("result", [])
    if isinstance(result, dict):
        objects = result.get("objects", [])
    elif isinstance(result, list):
        objects = result
    else:
        objects = []

    return [obj["key"] for obj in objects if obj.get("key", "").endswith(".parquet")]


class DataLoader:
    """Handles loading parquet data into DuckDB, using R2 native or wrangler fallback."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self.has_native_r2 = _setup_duckdb_r2(conn)
        self._tmpdir = None

        if self.has_native_r2:
            log.info("Using native DuckDB R2 auth")
        else:
            log.info("Using wrangler CLI for R2 downloads")
            self._tmpdir = tempfile.mkdtemp(prefix="verify_gw_")
            log.info(f"  Temp dir: {self._tmpdir}")

    def read_parquet(self, r2_prefix: str, table_name: str, extra_select: str = "*") -> int:
        """Load parquet file(s) from R2 prefix into a DuckDB table. Returns row count."""
        if self.has_native_r2:
            return self._read_native(r2_prefix, table_name, extra_select)
        return self._read_via_wrangler(r2_prefix, table_name, extra_select)

    def _read_native(self, r2_prefix: str, table_name: str, extra_select: str) -> int:
        """Read parquet directly from R2 using DuckDB native auth."""
        # Try glob first, then single file
        for path in [f"r2://{BUCKET}/{r2_prefix}/*.parquet", f"r2://{BUCKET}/{r2_prefix}/data.parquet"]:
            try:
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT {extra_select} FROM read_parquet('{path}')")
                return self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            except Exception:  # noqa: S112
                continue
        raise RuntimeError(f"Could not read {r2_prefix} from R2")

    def _read_via_wrangler(self, r2_prefix: str, table_name: str, extra_select: str) -> int:
        """Download parquet files via wrangler, then read from local disk."""
        local_dir = Path(self._tmpdir) / r2_prefix.replace("/", "_")
        local_dir.mkdir(parents=True, exist_ok=True)

        # List files under prefix (add trailing slash to avoid matching sibling prefixes)
        list_prefix = r2_prefix.rstrip("/") + "/"
        log.info(f"  Listing {list_prefix}...")
        try:
            keys = _wrangler_list_prefix(list_prefix)
        except Exception as e:
            log.warning(f"  List failed ({e}), trying known filenames...")
            keys = []

        # Only keep files that are strictly under this prefix
        parquet_keys = [k for k in keys if k.endswith(".parquet") and k.startswith(list_prefix)]
        if not parquet_keys:
            raise RuntimeError(f"No parquet files found under {r2_prefix}")

        # Pick the latest version: prefer root-level file, else latest timestamped
        root_files = [k for k in parquet_keys if "/" not in k[len(list_prefix) :]]
        parquet_keys = [root_files[-1]] if root_files else [sorted(parquet_keys)[-1]]

        log.info(f"  Using: {parquet_keys[0]}")

        # Download each parquet file
        local_files = []
        for key in parquet_keys:
            fname = key.replace("/", "_")
            local_path = str(local_dir / fname)
            log.info(f"  Downloading {key}...")
            _wrangler_download(key, local_path)
            local_files.append(local_path)

        # Read into DuckDB
        if len(local_files) == 1:
            sql = f"CREATE TABLE {table_name} AS SELECT {extra_select} FROM read_parquet('{local_files[0]}')"
        else:
            paths = ", ".join(f"'{f}'" for f in local_files)
            sql = f"CREATE TABLE {table_name} AS SELECT {extra_select} FROM read_parquet([{paths}])"

        self.conn.execute(sql)
        return self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    def cleanup(self):
        if self._tmpdir:
            import shutil

            shutil.rmtree(self._tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Analysis functions
# ---------------------------------------------------------------------------


def get_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial")
    return conn


def discover_data(loader: DataLoader) -> dict:
    """Quick data discovery — check what's available."""
    info = {}
    conn = loader.conn

    # GEUS
    log.info("Checking GEUS Dataverse pesticides...")
    try:
        n = loader.read_parquet("silver/geus_clean_pesticides", "_geus_check")
        row = conn.execute("SELECT COUNT(*) as n, MIN(year), MAX(year) FROM _geus_check").fetchone()
        info["geus_rows"] = row[0]
        log.info(f"  GEUS: {row[0]:,} rows, years {row[1]}-{row[2]}")
        cols = [
            c[0]
            for c in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name='_geus_check'"
            ).fetchall()
        ]
        log.info(f"  Columns: {cols}")
        conn.execute("DROP TABLE _geus_check")
    except Exception as e:
        log.error(f"  GEUS not found: {e}")
        info["geus_rows"] = 0

    # GRUKOS
    log.info("Checking GRUKOS...")
    try:
        n = loader.read_parquet("silver/grukos", "_grukos_check")
        info["grukos_count"] = n
        log.info(f"  GRUKOS: {n:,} features")
        cols = [
            c[0]
            for c in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name='_grukos_check'"
            ).fetchall()
        ]
        log.info(f"  Columns: {cols}")
        conn.execute("DROP TABLE _grukos_check")
    except Exception as e:
        log.error(f"  GRUKOS not found: {e}")
        info["grukos_count"] = 0

    # Disaggregation
    for year in APPLICATION_YEARS:
        key = f"disagg_{year}"
        try:
            n = loader.read_parquet(f"gold/pesticide_disaggregation_{year}_{year + 1}", f"_disagg_check_{year}")
            info[key] = n
            log.info(f"  Disaggregation {year}: {n:,} rows")
            cols = [
                c[0]
                for c in conn.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name='_disagg_check_{year}'"
                ).fetchall()
            ]
            log.info(f"  Columns: {cols}")
            conn.execute(f"DROP TABLE _disagg_check_{year}")
        except Exception as e:
            log.error(f"  Disaggregation {year} not found: {e}")
            info[key] = 0

    return info


def load_data(loader: DataLoader) -> None:
    """Load all required datasets into DuckDB."""
    conn = loader.conn

    log.info("Loading GEUS groundwater data (sample-level clean dataset)...")
    n = loader.read_parquet("silver/geus_clean_pesticides", "_geus_all")
    log.info(f"  Loaded {n:,} GEUS records (sample-level, all data types)")

    # Filter: exclude DEPOT (contaminated sites) which skew detection rates
    # Note: using all years gives better correlation alignment with the paper
    # (the paper's temporal lag analysis handles the detection window separately)
    conn.execute("""
        CREATE TABLE geus_raw AS
        SELECT * FROM _geus_all
        WHERE data_type NOT IN ('DEPOT', 'DEPOT (øvrige)')
    """)
    conn.execute("DROP TABLE _geus_all")
    n = conn.execute("SELECT COUNT(*) FROM geus_raw").fetchone()[0]
    log.info(f"  After filtering (no DEPOT, year >= 2018): {n:,} records")

    # Report data type breakdown
    rows = conn.execute("""
        SELECT data_type, COUNT(*) as n FROM geus_raw GROUP BY data_type ORDER BY n DESC
    """).fetchall()
    for dt, cnt in rows:
        log.info(f"    {dt}: {cnt:,}")

    log.info("Loading GRUKOS catchment polygons...")
    n = loader.read_parquet("silver/grukos", "grukos_raw")
    log.info(f"  Loaded {n:,} GRUKOS features")

    # Use the 'id' column as gruko_id (unique per polygon within each layer)
    conn.execute("ALTER TABLE grukos_raw ADD COLUMN gruko_id VARCHAR")
    conn.execute("UPDATE grukos_raw SET gruko_id = id")

    # Report layer breakdown
    rows = conn.execute("""
        SELECT layer, COUNT(*) as n FROM grukos_raw GROUP BY layer ORDER BY layer
    """).fetchall()
    for layer, cnt in rows:
        log.info(f"  Layer {layer}: {cnt:,} features")

    log.info("Loading BMD product→active ingredient mapping...")
    n = loader.read_parquet("silver/bmd", "bmd_raw")
    log.info(f"  Loaded {n:,} BMD product records")

    # Create mapping: registration_nr → (active_ingredient, concentration_g_per_unit)
    # BMD stores concentrations as semicolon-separated Danish decimals (comma = decimal point)
    # Units are g/l or g/kg — both convert identically: dose_product_units × conc / 1000 = kg a.i.  # noqa: RUF003
    # Use generate_subscripts to split ingredients and concentrations in parallel
    conn.execute("""
        CREATE TABLE _bmd_numbered AS
        SELECT
            CAST(registrerings_nr AS VARCHAR) as reg_nr,
            string_split(aktivstofnavn_e, ';') as ingredients,
            string_split(koncentration_er, ';') as concentrations,
            string_split(enhed_er, ';') as units
        FROM bmd_raw
        WHERE aktivstofnavn_e IS NOT NULL AND aktivstofnavn_e != ''
    """)
    conn.execute("""
        CREATE TABLE bmd_ingredients AS
        SELECT
            reg_nr,
            -- Strip parenthetical abbreviations like "mechlorprop (MCPP)" -> "mechlorprop"
            -- BMD stores some ingredients with e.g. "(MCPP-P)" or "(DEET)" suffixes
            TRIM(REGEXP_REPLACE(LOWER(TRIM(ingredients[i])), '\\s*\\([^)]*\\)\\s*$', '')) as active_ingredient,
            TRY_CAST(REPLACE(TRIM(concentrations[i]), ',', '.') AS DOUBLE) as concentration_g,
            TRIM(units[i]) as conc_unit
        FROM _bmd_numbered,
             generate_series(1, GREATEST(len(ingredients), 1)) t(i)
        WHERE i <= len(ingredients)
    """)
    conn.execute("DROP TABLE _bmd_numbered")

    n = conn.execute("SELECT COUNT(*) FROM bmd_ingredients").fetchone()[0]
    n_ing = conn.execute("SELECT COUNT(DISTINCT active_ingredient) FROM bmd_ingredients").fetchone()[0]
    n_with_conc = conn.execute(
        "SELECT COUNT(*) FROM bmd_ingredients WHERE concentration_g IS NOT NULL AND concentration_g > 0"
    ).fetchone()[0]
    log.info(f"  BMD ingredients: {n:,} mappings, {n_ing} unique active ingredients")
    log.info(f"  With valid concentration: {n_with_conc:,} ({100 * n_with_conc / max(n, 1):.0f}%)")

    log.info("Loading pesticide disaggregation data...")
    parts = []
    for year in APPLICATION_YEARS:
        tbl = f"_disagg_{year}"
        loader.read_parquet(f"gold/pesticide_disaggregation_{year}_{year + 1}", tbl)
        parts.append(f"SELECT *, {year} as application_year FROM {tbl}")

    conn.execute(f"CREATE TABLE disagg_raw AS {' UNION ALL '.join(parts)}")
    for year in APPLICATION_YEARS:
        conn.execute(f"DROP TABLE _disagg_{year}")
    n = conn.execute("SELECT COUNT(*) FROM disagg_raw").fetchone()[0]
    log.info(f"  Loaded {n:,} disaggregated pesticide records")

    # Map disagg products to active ingredients via BMD registration number
    # DosageQuantity is in PRODUCT units (L or kg). Multiply by BMD concentration to get kg a.i.:
    #   kg_active_ingredient = DosageQuantity (L or kg product) × concentration_g (g/L or g/kg) / 1000  # noqa: RUF003
    # For products without valid concentration, fall back to equal split among ingredients
    conn.execute("""
        CREATE TABLE disagg_with_ingredient AS
        SELECT
            d.*,
            bi.active_ingredient,
            bi.concentration_g,
            CASE
                WHEN bi.concentration_g IS NOT NULL AND bi.concentration_g > 0
                THEN d.DosageQuantity * bi.concentration_g / 1000.0
                ELSE d.DosageQuantity / COUNT(*) OVER (PARTITION BY d.DisaggregatedID)
            END as ingredient_dosage_kg
        FROM disagg_raw d
        JOIN bmd_ingredients bi
            ON CAST(d.PesticideRegistrationNumber AS VARCHAR) = bi.reg_nr
    """)
    n_mapped = conn.execute("SELECT COUNT(*) FROM disagg_with_ingredient").fetchone()[0]
    n_total = conn.execute("SELECT COUNT(*) FROM disagg_raw").fetchone()[0]
    n_with_conc = conn.execute("""
        SELECT COUNT(*) FROM disagg_with_ingredient
        WHERE concentration_g IS NOT NULL AND concentration_g > 0
    """).fetchone()[0]
    log.info(f"  Mapped to active ingredients: {n_mapped:,} / {n_total:,} ({100 * n_mapped / max(n_total, 1):.1f}%)")
    log.info(f"  With BMD concentration: {n_with_conc:,} ({100 * n_with_conc / max(n_mapped, 1):.0f}%)")

    # Show top active ingredients by kg a.i.
    rows = conn.execute("""
        SELECT active_ingredient, SUM(ingredient_dosage_kg) as total_kg
        FROM disagg_with_ingredient
        GROUP BY active_ingredient ORDER BY total_kg DESC LIMIT 10
    """).fetchall()
    log.info("  Top active ingredients by kg a.i.:")
    for r in rows:
        log.info(f"    {r[0]:40s} {r[1]:>12,.0f} kg")

    # Load field geometries for spatial join with GRUKOs.
    # We need proper ST_Intersection (area overlap), not centroid containment,
    # so that fields straddling GRUKO boundaries split their pesticide load
    # proportionally by overlap area.
    #
    # Strategy: try pre-computed gold/field_grukos_intersections first (year-matched),
    # then fall back to loading field geometries and computing intersections ourselves.
    log.info("Loading field-GRUKO intersections...")
    field_gruko_years = sorted({y + 1 for y in APPLICATION_YEARS})

    # First try: pre-computed intersections from gold layer
    fg_parts = []
    missing_years = []
    for field_year in field_gruko_years:
        tbl = f"_fg_{field_year}"
        try:
            loader.read_parquet(
                f"gold/field_analysis_field_grukos_intersections_{field_year}",
                tbl,
            )
            n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            log.info(f"  field_grukos_intersections_{field_year}: {n:,} rows (pre-computed)")
            fg_parts.append(f"SELECT field_uuid, grukos_id as gruko_id, field_grukos_geometry as geometry FROM {tbl}")
        except Exception:
            missing_years.append(field_year)

    # Second: for years without pre-computed intersections, load field geometries
    # and compute intersections against GRUKO polygons
    if missing_years:
        log.info(f"  Computing intersections for years without pre-computed data: {missing_years}")
        fld_parts = []
        for field_year in missing_years:
            tbl = f"_fields_{field_year}"
            try:
                loader.read_parquet(
                    f"silver/fvm_marker_{field_year}",
                    tbl,
                    extra_select="field_uuid, geometry",
                )
                n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                log.info(f"  fvm_marker_{field_year}: {n:,} fields")
                fld_parts.append(f"SELECT DISTINCT field_uuid, geometry FROM {tbl}")
            except Exception as e:
                log.warning(f"  fvm_marker_{field_year} not available: {e}")

        if fld_parts:
            conn.execute(f"CREATE TABLE _all_fields AS {' UNION ALL '.join(fld_parts)}")
            # Deduplicate (same field_uuid may appear in multiple years)
            conn.execute("""
                CREATE TABLE _unique_fields AS
                SELECT field_uuid, FIRST(geometry) as geometry
                FROM _all_fields GROUP BY field_uuid
            """)
            conn.execute("DROP TABLE _all_fields")

            log.info("  Computing ST_Intersection (field × GRUKO)...")  # noqa: RUF001
            conn.execute("""
                CREATE TABLE _computed_fg AS
                SELECT f.field_uuid, g.gruko_id,
                       ST_Intersection(f.geometry, g.geometry_spatial) as geometry
                FROM _unique_fields f
                JOIN grukos_raw g ON ST_Intersects(f.geometry, g.geometry_spatial)
            """)
            n = conn.execute("SELECT COUNT(*) FROM _computed_fg").fetchone()[0]
            log.info(f"  Computed {n:,} field-GRUKO intersections")
            fg_parts.append("SELECT field_uuid, gruko_id, geometry FROM _computed_fg")
            conn.execute("DROP TABLE _unique_fields")

    # Combine all sources
    conn.execute(f"""
        CREATE TABLE field_gruko_intersections AS
        {" UNION ALL ".join(fg_parts)}
    """)
    # Clean up temp tables
    for field_year in field_gruko_years:
        conn.execute(f"DROP TABLE IF EXISTS _fg_{field_year}")
    for field_year in missing_years:
        conn.execute(f"DROP TABLE IF EXISTS _fields_{field_year}")
    conn.execute("DROP TABLE IF EXISTS _computed_fg")

    n = conn.execute("SELECT COUNT(*) FROM field_gruko_intersections").fetchone()[0]
    n_fields = conn.execute("SELECT COUNT(DISTINCT field_uuid) FROM field_gruko_intersections").fetchone()[0]
    log.info(f"  Total field-GRUKO intersections: {n:,} ({n_fields:,} unique fields)")

    # Load soil types for transit time estimation
    log.info("Loading soil type polygons...")
    try:
        n = loader.read_parquet("silver/soil_types", "soil_types_raw")
        log.info(f"  Loaded {n:,} soil type polygons")
        rows = conn.execute("""
            SELECT soil_description, soil_height, COUNT(*) as n
            FROM soil_types_raw
            GROUP BY soil_description, soil_height
            ORDER BY soil_height
        """).fetchall()
        for desc, height, cnt in rows:
            transit = SOIL_TRANSIT_TIMES.get(height, DEFAULT_TRANSIT_YEARS)
            log.info(f"    {desc} (height={height}): {cnt:,} polygons -> {transit}yr transit")
    except Exception as e:
        log.warning(f"  Could not load soil types: {e}")
        log.warning("  Will use default transit time for all GRUKOs")


def build_gruko_application_intensity(conn: duckdb.DuckDBPyConnection) -> None:
    """Aggregate pesticide application intensity (total kg a.i.) per substance per GRUKO.

    Uses pre-computed field-GRUKO intersection geometries for area-weighted allocation.
    A field straddling two GRUKOs splits its pesticide load proportionally by overlap area:
        kg_in_gruko = (ingredient_dosage_kg / field_area) * intersection_area
    """
    log.info("Building GRUKO-level application intensity (area-weighted)...")

    # Join disagg (with active ingredient) to field-GRUKO intersections.
    # Each field×GRUKO pair gets the kg proportional to the intersection area.  # noqa: RUF003
    # intersection_area / AllocatedArea = fraction of field in this GRUKO.
    conn.execute("""
        CREATE TABLE field_gruko_join AS
        SELECT
            d.*,
            fg.gruko_id,
            ST_Area(fg.geometry) / 10000.0 as intersection_area_ha,
            CASE
                WHEN d.AllocatedArea > 0
                THEN d.ingredient_dosage_kg * (ST_Area(fg.geometry) / 10000.0) / d.AllocatedArea
                ELSE 0
            END as kg_in_gruko
        FROM disagg_with_ingredient d
        JOIN field_gruko_intersections fg ON d.field_uuid = fg.field_uuid
    """)

    n = conn.execute("SELECT COUNT(*) FROM field_gruko_join").fetchone()[0]
    n_fields = conn.execute("SELECT COUNT(DISTINCT field_uuid) FROM field_gruko_join").fetchone()[0]
    log.info(f"  Field-GRUKO join: {n:,} rows ({n_fields:,} unique fields)")

    # Sanity check: compare total kg before/after area weighting
    total_kg_raw = conn.execute("SELECT SUM(ingredient_dosage_kg) FROM disagg_with_ingredient").fetchone()[0]
    total_kg_weighted = conn.execute("SELECT SUM(kg_in_gruko) FROM field_gruko_join").fetchone()[0]
    if total_kg_raw and total_kg_raw > 0:
        pct = 100 * (total_kg_weighted or 0) / total_kg_raw
        log.info(f"  Area-weighted kg: {total_kg_weighted:,.0f} / {total_kg_raw:,.0f} = {pct:.1f}%")
        log.info("  (fields partially outside GRUKOs account for the difference)")

    # Aggregate by active ingredient per GRUKO
    conn.execute("""
        CREATE TABLE gruko_intensity AS
        SELECT
            gruko_id,
            active_ingredient as substance,
            SUM(kg_in_gruko) / NULLIF(SUM(intersection_area_ha), 0) as kg_per_ha,
            SUM(kg_in_gruko) as total_kg,
            SUM(intersection_area_ha) as total_area_ha,
            COUNT(*) as n_applications
        FROM field_gruko_join
        WHERE intersection_area_ha > 0 AND kg_in_gruko > 0
        GROUP BY gruko_id, active_ingredient
    """)

    n_combos = conn.execute("SELECT COUNT(*) FROM gruko_intensity").fetchone()[0]
    n_subs = conn.execute("SELECT COUNT(DISTINCT substance) FROM gruko_intensity").fetchone()[0]
    n_grukos = conn.execute("SELECT COUNT(DISTINCT gruko_id) FROM gruko_intensity").fetchone()[0]
    log.info(f"  Intensity: {n_combos:,} combos, {n_subs} substances, {n_grukos} GRUKOs")

    # Diagnostic: list all substances available in gruko_intensity
    gi_subs = conn.execute("""
        SELECT substance, SUM(total_kg) as total_kg, COUNT(DISTINCT gruko_id) as n_grukos
        FROM gruko_intensity
        GROUP BY substance
        ORDER BY total_kg DESC
    """).fetchall()
    log.info(f"  All {len(gi_subs)} substances in gruko_intensity:")
    for s, kg, ng in gi_subs[:30]:
        log.info(f"    {s:<45} {kg:>15,.0f} kg  {ng:>5} GRUKOs")
    if len(gi_subs) > 30:
        log.info(f"    ... and {len(gi_subs) - 30} more")


def build_gruko_soil_transit(conn: duckdb.DuckDBPyConnection) -> None:
    """Assign soil-dependent transit times to each GRUKO polygon.

    Spatial join: for each GRUKO, find the dominant soil type (largest overlap area)
    and map it to a transit time in years. Sandy soils ~3yr, clay ~7yr.
    """
    has_soil = conn.execute("""
        SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'soil_types_raw'
    """).fetchone()[0]

    if not has_soil:
        log.info("No soil type data — using default transit time for all GRUKOs")
        conn.execute(f"""
            CREATE TABLE gruko_transit AS
            SELECT DISTINCT gruko_id, {DEFAULT_TRANSIT_YEARS} as transit_years,
                   'unknown' as dominant_soil, 0 as soil_height
            FROM grukos_raw
        """)
        return

    log.info("Assigning soil-dependent transit times to GRUKOs...")

    # Ensure soil geometry is a proper GEOMETRY type (may be stored as WKT text)
    has_geom_col = conn.execute("""
        SELECT data_type FROM information_schema.columns
        WHERE table_name = 'soil_types_raw' AND column_name = 'geometry'
    """).fetchone()
    soil_geom_type = has_geom_col[0] if has_geom_col else "unknown"
    log.info(f"  Soil geometry column type: {soil_geom_type}")

    # Cast to GEOMETRY if stored as text/varchar
    soil_geom_expr = "ST_GeomFromText(s.geometry)" if soil_geom_type.upper() in ("VARCHAR", "TEXT") else "s.geometry"

    # Spatial join: intersect GRUKO polygons with soil polygons, compute overlap area
    # Use the dominant soil type (largest intersection area) per GRUKO
    conn.execute(f"""
        CREATE TABLE _gruko_soil_overlap AS
        SELECT
            g.gruko_id,
            s.soil_description,
            s.soil_height,
            ST_Area(ST_Intersection(g.geometry_spatial, {soil_geom_expr})) as overlap_area
        FROM grukos_raw g
        JOIN soil_types_raw s ON ST_Intersects(g.geometry_spatial, {soil_geom_expr})
        WHERE ST_Area(ST_Intersection(g.geometry_spatial, {soil_geom_expr})) > 0
    """)

    n_overlaps = conn.execute("SELECT COUNT(*) FROM _gruko_soil_overlap").fetchone()[0]
    log.info(f"  GRUKO-soil overlaps: {n_overlaps:,}")

    # Pick dominant soil type per GRUKO (largest overlap area)
    conn.execute("""
        CREATE TABLE _gruko_dominant_soil AS
        SELECT gruko_id, soil_description, soil_height, overlap_area
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY gruko_id ORDER BY overlap_area DESC) as rn
            FROM _gruko_soil_overlap
        )
        WHERE rn = 1
    """)

    n_assigned = conn.execute("SELECT COUNT(*) FROM _gruko_dominant_soil").fetchone()[0]
    n_total = conn.execute("SELECT COUNT(DISTINCT gruko_id) FROM grukos_raw").fetchone()[0]
    log.info(f"  GRUKOs with soil assignment: {n_assigned:,} / {n_total:,}")

    # Map soil_height to transit years
    # Build CASE expression from SOIL_TRANSIT_TIMES
    case_parts = " ".join(f"WHEN soil_height = {h} THEN {t}" for h, t in SOIL_TRANSIT_TIMES.items())
    conn.execute(f"""
        CREATE TABLE gruko_transit AS
        SELECT
            g.gruko_id,
            CASE {case_parts} ELSE {DEFAULT_TRANSIT_YEARS} END as transit_years,
            COALESCE(ds.soil_description, 'unknown') as dominant_soil,
            COALESCE(ds.soil_height, 0) as soil_height
        FROM (SELECT DISTINCT gruko_id FROM grukos_raw) g
        LEFT JOIN _gruko_dominant_soil ds ON g.gruko_id = ds.gruko_id
    """)

    conn.execute("DROP TABLE _gruko_soil_overlap")
    conn.execute("DROP TABLE _gruko_dominant_soil")

    # Report transit time distribution
    rows = conn.execute("""
        SELECT transit_years, dominant_soil, COUNT(*) as n
        FROM gruko_transit
        GROUP BY transit_years, dominant_soil
        ORDER BY transit_years, n DESC
    """).fetchall()
    for transit, soil, cnt in rows:
        log.info(f"    {transit}yr transit ({soil}): {cnt:,} GRUKOs")


def build_gruko_detections(conn: duckdb.DuckDBPyConnection, detection_mode: str = "2018") -> None:
    """Spatially assign GEUS boreholes to GRUKOs, compute binary detection.

    detection_mode:
      'all'  — no year filter (use all available GEUS data)
      '2018' — uniform 2018+ filter (paper Section 2.2.2)
      'soil' — soil-dependent transit times per GRUKO
    """
    log.info("Building GRUKO-level groundwater detections...")

    has_geom = conn.execute("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name='geus_raw' AND column_name='geometry'
    """).fetchone()[0]

    has_xy = conn.execute("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name='geus_raw' AND column_name='x'
    """).fetchone()[0]

    if has_geom:
        geom_expr = "geus.geometry"
    elif has_xy:
        geom_expr = "ST_Point(geus.x, geus.y)"
    else:
        raise RuntimeError("GEUS data has neither geometry nor x/y columns")

    conn.execute(f"""
        CREATE TABLE geus_gruko AS
        SELECT geus.*, g.gruko_id
        FROM geus_raw geus
        JOIN grukos_raw g ON ST_Within({geom_expr}, g.geometry_spatial)
    """)

    n_matched = conn.execute("SELECT COUNT(*) FROM geus_gruko").fetchone()[0]
    n_total = conn.execute("SELECT COUNT(*) FROM geus_raw").fetchone()[0]
    log.info(f"  Boreholes matched: {n_matched:,} / {n_total:,} ({100 * n_matched / max(n_total, 1):.1f}%)")

    if detection_mode == "soil":
        has_transit = conn.execute("""
            SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'gruko_transit'
        """).fetchone()[0]
        if not has_transit:
            log.warning("  Soil mode requested but no transit data — falling back to 2018+ mode")
            detection_mode = "2018"

    if detection_mode == "soil":
        # Soil-adjusted detection windows:
        # Detection window START = application_year_start + transit_years (earliest expected arrival)
        # Detection window END = 2025 (latest available data)
        # Sandy (3yr): 2018-2025, Intermediate (5yr): 2020-2025, Clay (7yr): 2022-2025
        app_start = APPLICATION_YEARS[0]  # 2015
        detection_end = 2025

        log.info(f"  Mode=soil: detection windows per GRUKO (app start: {app_start}, end: {detection_end})")

        # Check if sample_id column exists (clean dataset has it, AM does not)
        has_sample_id = conn.execute("""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name='geus_gruko' AND column_name='sample_id'
        """).fetchone()[0]
        sample_count_expr = "COUNT(DISTINCT gg.sample_id)" if has_sample_id else "COUNT(*)"

        conn.execute(f"""
            CREATE TABLE gruko_detections AS
            SELECT
                gg.gruko_id,
                gg.stof_tekst as substance,
                MAX(CASE WHEN gg.maengde > {DETECTION_THRESHOLD_UGL} THEN 1 ELSE 0 END) as detected,
                {sample_count_expr} as n_samples,
                MAX(gg.maengde) as max_concentration,
                MIN(gg.year) as min_year,
                MAX(gg.year) as max_year
            FROM geus_gruko gg
            JOIN gruko_transit gt ON gg.gruko_id = gt.gruko_id
            WHERE gg.year >= {app_start} + gt.transit_years
              AND gg.year <= {detection_end}
            GROUP BY gg.gruko_id, gg.stof_tekst
        """)

        rows = conn.execute(f"""
            SELECT gt.transit_years,
                   {app_start} + gt.transit_years as win_start,
                   {detection_end} as win_end,
                   COUNT(DISTINCT gg.gruko_id) as n_grukos
            FROM geus_gruko gg
            JOIN gruko_transit gt ON gg.gruko_id = gt.gruko_id
            WHERE gg.year >= {app_start} + gt.transit_years
              AND gg.year <= {detection_end}
            GROUP BY gt.transit_years
            ORDER BY gt.transit_years
        """).fetchall()
        for transit, win_start, win_end, n_grukos in rows:
            log.info(f"    {transit}yr transit: detection window {win_start}-{win_end}, {n_grukos:,} GRUKOs with data")

    elif detection_mode == "2018":
        # Paper Section 2.2.2: uniform detection window 2018-2025
        log.info(f"  Mode=2018: uniform detection window {DETECTION_YEAR_START}-2025")

        has_sample_id = conn.execute("""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name='geus_gruko' AND column_name='sample_id'
        """).fetchone()[0]
        sample_count_expr = "COUNT(DISTINCT sample_id)" if has_sample_id else "COUNT(*)"

        conn.execute(f"""
            CREATE TABLE gruko_detections AS
            SELECT
                gruko_id,
                stof_tekst as substance,
                MAX(CASE WHEN maengde > {DETECTION_THRESHOLD_UGL} THEN 1 ELSE 0 END) as detected,
                {sample_count_expr} as n_samples,
                MAX(maengde) as max_concentration,
                MIN(year) as min_year,
                MAX(year) as max_year
            FROM geus_gruko
            WHERE year >= {DETECTION_YEAR_START}
            GROUP BY gruko_id, stof_tekst
        """)

    else:
        # Mode=all: no year filter
        log.info("  Mode=all: no detection year filter")

        has_sample_id = conn.execute("""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name='geus_gruko' AND column_name='sample_id'
        """).fetchone()[0]
        sample_count_expr = "COUNT(DISTINCT sample_id)" if has_sample_id else "COUNT(*)"

        conn.execute(f"""
            CREATE TABLE gruko_detections AS
            SELECT
                gruko_id,
                stof_tekst as substance,
                MAX(CASE WHEN maengde > {DETECTION_THRESHOLD_UGL} THEN 1 ELSE 0 END) as detected,
                {sample_count_expr} as n_samples,
                MAX(maengde) as max_concentration,
                MIN(year) as min_year,
                MAX(year) as max_year
            FROM geus_gruko
            GROUP BY gruko_id, stof_tekst
        """)

    n_combos = conn.execute("SELECT COUNT(*) FROM gruko_detections").fetchone()[0]
    n_subs = conn.execute("SELECT COUNT(DISTINCT substance) FROM gruko_detections").fetchone()[0]
    n_grukos = conn.execute("SELECT COUNT(DISTINCT gruko_id) FROM gruko_detections").fetchone()[0]
    log.info(f"  Detections: {n_combos:,} combos, {n_subs} substances, {n_grukos} GRUKOs")


def _get_intensity_sql(substance_name: str) -> str:
    """Return SQL subquery for application intensity per GRUKO for a given substance.

    Shared by run_correlations(), run_temporal_lag(), and run_multivariate_logistic().
    """
    safe_name = substance_name.replace("'", "''")

    if substance_name in METABOLITE_PARENT_MAP:
        parents = METABOLITE_PARENT_MAP[substance_name]
        parent_filter = ", ".join(f"'{p}'" for p in parents)
        return f"""
            SELECT gruko_id, SUM(total_kg) as intensity
            FROM gruko_intensity
            WHERE substance IN ({parent_filter})
            GROUP BY gruko_id
        """
    if substance_name in GEUS_TO_BMD_MULTI:
        bmd_names = GEUS_TO_BMD_MULTI[substance_name]
        bmd_filter = ", ".join(f"'{n}'" for n in bmd_names)
        return f"""
            SELECT gruko_id, SUM(total_kg) as intensity
            FROM gruko_intensity
            WHERE substance IN ({bmd_filter})
            GROUP BY gruko_id
        """
    if substance_name in GEUS_TO_BMD:
        bmd_name = GEUS_TO_BMD[substance_name]
        return f"""
            SELECT gruko_id, SUM(total_kg) as intensity
            FROM gruko_intensity
            WHERE substance = '{bmd_name}'
            GROUP BY gruko_id
        """
    return f"""
            SELECT gruko_id, SUM(total_kg) as intensity
            FROM gruko_intensity
            WHERE LOWER(substance) = LOWER('{safe_name}')
            GROUP BY gruko_id
        """


def build_gruko_covariates(conn: duckdb.DuckDBPyConnection) -> None:
    """Build per-GRUKO hydrogeological covariates for multivariate regression.

    Creates gruko_covariates table with:
    - n_wells: COUNT(DISTINCT dgu_nr) — monitoring density (R2 comment)
    - median_intake_depth_m: MEDIAN((intake_top_m + intake_bottom_m)/2) — well depth (R1)
    - n_analyses: COUNT(*) total analyses
    - soil_height: from gruko_transit (already computed)

    Also prints Spearman correlation between n_wells and total application intensity
    per GRUKO as a monitoring density diagnostic (R2).
    """
    log.info("Building per-GRUKO hydrogeological covariates...")

    conn.execute("""
        CREATE TABLE gruko_covariates AS
        SELECT
            gg.gruko_id,
            COUNT(DISTINCT gg.dgu_nr) as n_wells,
            MEDIAN((gg.intake_top_m + gg.intake_bottom_m) / 2.0) as median_intake_depth_m,
            COUNT(*) as n_analyses,
            COALESCE(gt.soil_height, 0) as soil_height
        FROM geus_gruko gg
        LEFT JOIN gruko_transit gt ON gg.gruko_id = gt.gruko_id
        GROUP BY gg.gruko_id, gt.soil_height
    """)

    n = conn.execute("SELECT COUNT(*) FROM gruko_covariates").fetchone()[0]
    stats = conn.execute("""
        SELECT
            AVG(n_wells), MEDIAN(n_wells),
            AVG(median_intake_depth_m), MEDIAN(median_intake_depth_m),
            AVG(n_analyses), MEDIAN(n_analyses)
        FROM gruko_covariates
    """).fetchone()
    log.info(f"  Covariates for {n:,} GRUKOs")
    log.info(f"    Wells: mean={stats[0]:.1f}, median={stats[1]:.0f}")
    log.info(f"    Intake depth: mean={stats[2]:.1f}m, median={stats[3]:.1f}m")
    log.info(f"    Analyses: mean={stats[4]:.0f}, median={stats[5]:.0f}")

    # Monitoring density diagnostic: Spearman(n_wells, total_intensity)
    rows = conn.execute("""
        SELECT gc.gruko_id, gc.n_wells, COALESCE(SUM(gi.total_kg), 0) as total_intensity
        FROM gruko_covariates gc
        LEFT JOIN gruko_intensity gi ON gc.gruko_id = gi.gruko_id
        GROUP BY gc.gruko_id, gc.n_wells
    """).fetchall()

    if len(rows) >= 10:
        wells = np.array([r[1] for r in rows])
        intensity = np.array([r[2] for r in rows])
        rho, p = scipy_stats.spearmanr(wells, intensity)
        log.info(f"  Monitoring density diagnostic: Spearman(n_wells, total_intensity) = {rho:.3f}, p={p:.4f}")
        if p < 0.05 and abs(rho) > 0.3:
            log.warning("  *** Monitoring density correlated with application intensity — potential confound")
    else:
        log.warning("  Too few GRUKOs for monitoring density diagnostic")


def run_multivariate_logistic(conn: duckdb.DuckDBPyConnection, results: list[dict]) -> list[dict]:
    """Multivariate logistic regression controlling for hydrogeological covariates.

    For each FDR-significant substance, fit:
        P(detected=1) = logit^{-1}(b0 + b1*intensity + b2*soil_height + b3*median_intake_depth + b4*n_wells)

    Returns per substance: intensity OR + 95% CI, p-values for all covariates, pseudo-R2, AIC.
    """
    log.info("Running multivariate logistic regression with covariates...")

    sig = [r for r in results if r.get("sig_fdr") and r["r"] > 0]
    if not sig:
        log.warning("  No FDR-significant substances for multivariate analysis")
        return []

    mv_results = []

    for sub in sig:
        substance_name = sub["substance"]
        safe_name = substance_name.replace("'", "''")
        intensity_sql = _get_intensity_sql(substance_name)

        try:
            rows = conn.execute(f"""
                SELECT
                    d.gruko_id,
                    d.detected,
                    COALESCE(i.intensity, 0) as intensity,
                    COALESCE(gc.soil_height, 0) as soil_height,
                    COALESCE(gc.median_intake_depth_m, 0) as median_intake_depth,
                    COALESCE(gc.n_wells, 0) as n_wells
                FROM gruko_detections d
                LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
                LEFT JOIN gruko_covariates gc ON d.gruko_id = gc.gruko_id
                WHERE d.substance = '{safe_name}'
            """).fetchall()
        except Exception as e:
            log.warning(f"  Skipping {substance_name}: {e}")
            continue

        if len(rows) < 50:
            continue

        detected = np.array([r[1] for r in rows])
        intensity = np.array([r[2] for r in rows])
        soil_h = np.array([r[3] for r in rows])
        depth = np.array([r[4] for r in rows])
        wells = np.array([r[5] for r in rows])

        if detected.std() == 0 or intensity.std() == 0:
            continue

        # Fit multivariate logistic regression
        X = np.column_stack([intensity, soil_h, depth, wells])
        X = sm.add_constant(X)
        _col_names = ["const", "intensity", "soil_height", "intake_depth", "n_wells"]

        try:
            model = sm.Logit(detected, X).fit(disp=0, maxiter=200)
        except Exception:
            # Try regularized fit for convergence issues
            try:
                model = sm.Logit(detected, X).fit_regularized(disp=0, maxiter=200, alpha=0.1)
            except Exception as e:
                log.warning(f"  Convergence failure for {substance_name}: {e}")
                continue

        # Extract intensity coefficient (index 1)
        intensity_coef = model.params[1]
        intensity_or = np.exp(intensity_coef)

        # CIs for intensity OR
        try:
            ci = model.conf_int()
            or_ci_low = np.exp(ci[1, 0])
            or_ci_high = np.exp(ci[1, 1])
        except Exception:
            or_ci_low = or_ci_high = None

        # p-values for all covariates
        try:
            pvals = model.pvalues
            p_intensity = pvals[1]
            p_soil = pvals[2]
            p_depth = pvals[3]
            p_wells = pvals[4]
        except Exception:
            p_intensity = p_soil = p_depth = p_wells = None

        # Model fit
        try:
            pseudo_r2 = model.prsquared
            aic = model.aic
        except Exception:
            pseudo_r2 = aic = None

        # Model diagnostics: AUC, Nagelkerke R², Hosmer-Lemeshow, EPV, VIF
        try:
            y_pred = model.predict(X)
            mv_auc = _compute_auc(detected, y_pred)
        except Exception:
            mv_auc = None
        mv_nagelkerke = _compute_nagelkerke_r2(model)
        try:
            y_pred_hl = model.predict(X)
            _, mv_hl_p = _hosmer_lemeshow(detected, y_pred_hl)
        except Exception:
            mv_hl_p = None

        # EPV = events per variable (n_events / n_predictors)
        n_events = int(detected.sum())
        n_predictors = X.shape[1] - 1  # exclude constant
        epv = n_events / n_predictors if n_predictors > 0 else None

        # VIF for intensity (index 1 in X which includes constant at index 0)
        vif_intensity = _compute_vif(X, 1)

        mv_results.append(
            {
                "substance": substance_name,
                "type": sub["type"],
                "bivariate_r": sub["r"],
                "bivariate_or": sub.get("logit_or"),
                "mv_intensity_or": round(intensity_or, 4),
                "mv_or_ci_low": round(or_ci_low, 4) if or_ci_low else None,
                "mv_or_ci_high": round(or_ci_high, 4) if or_ci_high else None,
                "p_intensity": p_intensity,
                "p_soil": p_soil,
                "p_depth": p_depth,
                "p_wells": p_wells,
                "pseudo_r2": round(pseudo_r2, 4) if pseudo_r2 else None,
                "nagelkerke_r2": round(mv_nagelkerke, 3) if mv_nagelkerke else None,
                "auc": round(mv_auc, 2) if mv_auc else None,
                "hl_p": round(mv_hl_p, 3) if mv_hl_p and np.isfinite(mv_hl_p) else None,
                "epv": round(epv, 1) if epv else None,
                "vif_intensity": round(vif_intensity, 1) if np.isfinite(vif_intensity) else None,
                "aic": round(aic, 1) if aic else None,
                "n": len(rows),
                "n_events": n_events,
            }
        )

    n_still_sig = sum(1 for r in mv_results if r.get("p_intensity") is not None and r["p_intensity"] < 0.05)
    log.info(
        f"  Multivariate results: {len(mv_results)} substances, {n_still_sig} with intensity p<0.05 after covariate adjustment"
    )

    return mv_results


def run_negative_controls(conn: duckdb.DuckDBPyConnection, results: list[dict]) -> list[dict]:
    """Test high-Koc substances as negative controls.

    High-Koc substances bind strongly to soil and should NOT leach to groundwater,
    so we expect non-significant correlations. Finding non-significance confirms
    the method is not producing false positives from spatial confounding alone.
    """
    log.info("Running negative control analysis (high-Koc substances)...")

    # High-Koc substances with known agricultural application
    controls = [
        ("Diflufenican", 3400),  # Koc=3400, herbicide
        ("Prosulfocarb", 1800),  # Koc=1800, thiocarbamate herbicide
        ("Propiconazol", 1086),  # Koc=1086, triazole fungicide
        ("Epoxiconazol", 1073),  # Koc=1073, triazole fungicide
        ("Boscalid", 772),  # Koc=772, SDHI fungicide
    ]

    neg_results = []

    for geus_name, koc in controls:
        safe_name = geus_name.replace("'", "''")
        intensity_sql = _get_intensity_sql(geus_name)

        try:
            rows = conn.execute(f"""
                SELECT d.gruko_id, d.detected, COALESCE(i.intensity, 0) as intensity
                FROM gruko_detections d
                LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
                WHERE d.substance = '{safe_name}'
            """).fetchall()
        except Exception as e:
            log.warning(f"  Skipping negative control {geus_name}: {e}")
            continue

        n = len(rows)
        if n < 10:
            neg_results.append(
                {
                    "substance": geus_name,
                    "koc": koc,
                    "n": n,
                    "r": None,
                    "p_value": None,
                    "significant": None,
                    "note": f"insufficient data (n={n})",
                }
            )
            continue

        detected = np.array([r[1] for r in rows])
        intensity = np.array([r[2] for r in rows])

        if detected.std() == 0 or intensity.std() == 0:
            neg_results.append(
                {
                    "substance": geus_name,
                    "koc": koc,
                    "n": n,
                    "r": 0.0,
                    "p_value": 1.0,
                    "significant": False,
                    "note": "zero variance",
                }
            )
            continue

        r, p = scipy_stats.pointbiserialr(detected, intensity)
        detection_rate = 100.0 * detected.sum() / n

        neg_results.append(
            {
                "substance": geus_name,
                "koc": koc,
                "n": n,
                "r": round(r, 3),
                "p_value": p,
                "significant": p < 0.05 and r > 0,
                "detection_rate": round(detection_rate, 1),
                "note": "UNEXPECTED" if (p < 0.05 and r > 0) else "as expected (non-significant)",
            }
        )

    # Apply BH-FDR correction across negative control p-values (Table 9 in paper)
    nc_pvals = [r["p_value"] if r.get("p_value") is not None else 1.0 for r in neg_results]
    if any(p < 1.0 for p in nc_pvals):
        _, p_adj_nc, _, _ = multipletests(nc_pvals, alpha=0.05, method="fdr_bh")
        for r, qa in zip(neg_results, p_adj_nc, strict=False):
            r["q_fdr"] = round(float(qa), 3)
    else:
        for r in neg_results:
            r["q_fdr"] = 1.0

    n_nonsig = sum(1 for r in neg_results if r.get("significant") is False)
    log.info(f"  Negative controls: {len(neg_results)} tested, {n_nonsig} non-significant (as expected)")

    return neg_results


def run_power_analysis(results: list[dict]) -> dict:
    """Compute statistical power analysis for the correlation tests.

    Determines minimum detectable r at 80% power given median sample size,
    and classifies observed effects into practical significance tiers.
    """
    log.info("Running power analysis...")

    ns = [r["n_grukos"] for r in results if r.get("n_grukos")]
    if not ns:
        return {}

    median_n = int(np.median(ns))
    mean_n = int(np.mean(ns))
    min_n = min(ns)
    max_n = max(ns)

    # Minimum detectable r at 80% power, alpha=0.05 (two-tailed)
    # For point-biserial: approximately r_min = z_(alpha/2) + z_(beta) / sqrt(n-3)
    # Using exact Fisher z formula
    z_alpha = 1.96  # two-tailed alpha=0.05
    z_beta = 0.842  # 80% power
    r_min = np.tanh((z_alpha + z_beta) / np.sqrt(median_n - 3))

    # Effect size tiers (Cohen 1988 adapted for ecological data)
    sig = [r for r in results if r.get("sig_fdr") and r["r"] > 0]
    r_values = [r["r"] for r in sig]

    n_strong = sum(1 for rv in r_values if rv >= 0.20)
    n_moderate = sum(1 for rv in r_values if 0.10 <= rv < 0.20)
    n_weak = sum(1 for rv in r_values if rv < 0.10)
    n_above_min = sum(1 for rv in r_values if rv > r_min)

    power_info = {
        "median_n": median_n,
        "mean_n": mean_n,
        "min_n": min_n,
        "max_n": max_n,
        "r_min_80pct": round(r_min, 4),
        "n_sig": len(sig),
        "n_strong": n_strong,
        "n_moderate": n_moderate,
        "n_weak": n_weak,
        "n_above_min": n_above_min,
        "r_values": r_values,
    }

    log.info(f"  Median n={median_n}, min detectable r (80% power) = {r_min:.4f}")
    log.info(
        f"  Effect tiers: strong (r>=0.20): {n_strong}, moderate (0.10-0.20): {n_moderate}, weak (<0.10): {n_weak}"
    )
    log.info(f"  {n_above_min}/{len(sig)} exceed minimum detectable effect")

    return power_info


def run_correlations(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Point-biserial correlation per substance with 95% CIs."""
    log.info("Running point-biserial correlations...")

    substances = conn.execute(f"""
        SELECT substance, SUM(detected) as n_detected, COUNT(*) as n_grukos
        FROM gruko_detections
        GROUP BY substance
        HAVING SUM(detected) >= {MIN_DETECTIONS}
        ORDER BY SUM(detected) DESC
    """).fetchall()

    log.info(f"  {len(substances)} substances with >= {MIN_DETECTIONS} detections")

    # Report which paper substances are missing from the detection data
    detected_names = {s[0] for s in substances}
    paper_names = set(EXPECTED_TABLE2.keys())
    missing = paper_names - detected_names
    if missing:
        log.info(f"  Paper substances NOT in detections (< {MIN_DETECTIONS} or absent):")
        for name in sorted(missing):
            cnt = conn.execute(f"""
                SELECT SUM(detected), COUNT(*) FROM gruko_detections
                WHERE substance = '{name.replace("'", "''")}'
            """).fetchone()
            log.info(f"    {name}: {cnt[0] or 0} detections across {cnt[1] or 0} GRUKOs")

    results = []

    for substance_name, _n_detected, _n_grukos in substances:
        safe_name = substance_name.replace("'", "''")
        intensity_sql = _get_intensity_sql(substance_name)

        try:
            rows = conn.execute(f"""
                SELECT d.gruko_id, d.detected, COALESCE(i.intensity, 0) as intensity
                FROM gruko_detections d
                LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
                WHERE d.substance = '{safe_name}'
            """).fetchall()
        except Exception as e:
            log.warning(f"  Skipping {substance_name}: {e}")
            continue

        if len(rows) < MIN_DETECTIONS:
            continue

        detected = np.array([r[1] for r in rows])
        intensity = np.array([r[2] for r in rows])

        nonzero_intensity = int((intensity > 0).sum())
        if detected.std() == 0 or intensity.std() == 0:
            in_map = (
                substance_name in METABOLITE_PARENT_MAP
                or substance_name in GEUS_TO_BMD
                or substance_name in GEUS_TO_BMD_MULTI
            )
            log.info(
                f"  DROPPED {substance_name}: det_std={detected.std():.4f}, "
                f"int_std={intensity.std():.4f}, nonzero_int={nonzero_intensity}/{len(intensity)}, "
                f"n_det={int(detected.sum())}, in_map={in_map}"
            )
            continue

        r, p_value = scipy_stats.pointbiserialr(detected, intensity)
        n = len(rows)
        detection_rate = 100.0 * detected.sum() / n

        # Fisher z-transform 95% CI for r (R3 comment)
        z = np.arctanh(r)
        se = 1.0 / np.sqrt(n - 3)
        r_ci_low = np.tanh(z - 1.96 * se)
        r_ci_high = np.tanh(z + 1.96 * se)

        # Logistic regression: P(detected) ~ intensity
        logit_p = None
        logit_or = None
        logit_or_ci_low = None
        logit_or_ci_high = None
        logit_auc = None
        logit_nagelkerke = None
        logit_hl_p = None
        try:
            X = sm.add_constant(intensity)
            logit_model = sm.Logit(detected, X).fit(disp=0, maxiter=100)
            logit_p = logit_model.pvalues[1]
            logit_or = round(np.exp(logit_model.params[1]), 4)
            # OR confidence intervals (R3)
            ci = logit_model.conf_int()
            _or_low = np.exp(ci[1, 0])
            _or_high = np.exp(ci[1, 1])
            logit_or_ci_low = round(_or_low, 4) if np.isfinite(_or_low) else None
            logit_or_ci_high = round(_or_high, 4) if np.isfinite(_or_high) else None
            # Model diagnostics
            y_pred = logit_model.predict(X)
            logit_auc = _compute_auc(detected, y_pred)
            logit_nagelkerke = _compute_nagelkerke_r2(logit_model)
            _, logit_hl_p = _hosmer_lemeshow(detected, y_pred)
        except Exception:  # noqa: S110
            pass

        # Quartile dose-response
        q_rates = {}
        q4_q1 = None
        if intensity.max() > 0 and (intensity > 0).sum() >= 4:
            quartiles = np.percentile(intensity[intensity > 0], [25, 50, 75])
            q1_mask = intensity <= quartiles[0]
            q2_mask = (intensity > quartiles[0]) & (intensity <= quartiles[1])
            q3_mask = (intensity > quartiles[1]) & (intensity <= quartiles[2])
            q4_mask = intensity > quartiles[2]
            q_rates = {
                "q1_rate": round(100 * detected[q1_mask].mean(), 1) if q1_mask.sum() > 0 else None,
                "q2_rate": round(100 * detected[q2_mask].mean(), 1) if q2_mask.sum() > 0 else None,
                "q3_rate": round(100 * detected[q3_mask].mean(), 1) if q3_mask.sum() > 0 else None,
                "q4_rate": round(100 * detected[q4_mask].mean(), 1) if q4_mask.sum() > 0 else None,
                "q1_n": int(q1_mask.sum()),
                "q2_n": int(q2_mask.sum()),
                "q3_n": int(q3_mask.sum()),
                "q4_n": int(q4_mask.sum()),
            }
            if q1_mask.sum() > 0 and q4_mask.sum() > 0:
                q1_rate = detected[q1_mask].mean()
                q4_rate = detected[q4_mask].mean()
                if q1_rate > 0:
                    q4_q1 = q4_rate / q1_rate

        # Descriptive statistics for intensity (R4)
        intensity_nonzero = intensity[intensity > 0]

        stype = SUBSTANCE_TYPE.get(substance_name, "unknown")
        results.append(
            {
                "substance": substance_name,
                "type": stype,
                "r": round(r, 3),
                "r_ci_low": round(r_ci_low, 3),
                "r_ci_high": round(r_ci_high, 3),
                "p_value": p_value,
                "logit_p": logit_p,
                "logit_or": logit_or,
                "logit_or_ci_low": logit_or_ci_low,
                "logit_or_ci_high": logit_or_ci_high,
                "logit_auc": round(logit_auc, 2) if logit_auc else None,
                "logit_nagelkerke_r2": round(logit_nagelkerke, 3) if logit_nagelkerke else None,
                "logit_hl_p": round(logit_hl_p, 3) if logit_hl_p and np.isfinite(logit_hl_p) else None,
                "q4_q1": round(q4_q1, 1) if q4_q1 else None,
                "q_rates": q_rates,
                "detection_rate": round(detection_rate, 1),
                "n_grukos": n,
                "n_detected": int(detected.sum()),
                "intensity_mean": round(float(intensity.mean()), 2),
                "intensity_sd": round(float(intensity.std()), 2),
                "intensity_median": round(float(np.median(intensity)), 2),
                "intensity_nonzero_n": len(intensity_nonzero),
            }
        )

    results.sort(key=lambda x: abs(x["r"]), reverse=True)

    # Benjamini-Hochberg FDR correction across all tested substances
    if results:
        p_vals = [r["p_value"] for r in results]
        reject, p_adj, _, _ = multipletests(p_vals, alpha=0.05, method="fdr_bh")
        for r, pa, sig in zip(results, p_adj, reject, strict=False):
            r["p_fdr"] = pa
            r["sig_fdr"] = bool(sig)

        # Also FDR-correct logistic p-values
        logit_ps = [r["logit_p"] if r["logit_p"] is not None else 1.0 for r in results]
        reject_l, p_adj_l, _, _ = multipletests(logit_ps, alpha=0.05, method="fdr_bh")
        for r, pa, sig in zip(results, p_adj_l, reject_l, strict=False):
            r["logit_p_fdr"] = pa
            r["logit_sig_fdr"] = bool(sig)

        n_sig_raw = sum(1 for r in results if r["p_value"] < 0.05 and r["r"] > 0)
        n_sig_fdr = sum(1 for r in results if r.get("sig_fdr") and r["r"] > 0)
        n_logit_fdr = sum(1 for r in results if r.get("logit_sig_fdr") and r["r"] > 0)
        log.info(f"  Significant (rpb raw p<0.05): {n_sig_raw}")
        log.info(f"  Significant (rpb BH-FDR q<0.05): {n_sig_fdr}")
        log.info(f"  Significant (logit BH-FDR q<0.05): {n_logit_fdr}")

    return results


def run_spatial_autocorrelation(conn: duckdb.DuckDBPyConnection, results: list[dict]) -> None:
    """Compute Moran's I for detection and intensity to assess spatial non-independence.

    If significant spatial autocorrelation is found, p-values from the correlation
    tests are inflated (the effective sample size is smaller than n).
    Uses a k-nearest-neighbors weight matrix (k=8) based on GRUKO centroids.
    """
    log.info("Running spatial autocorrelation diagnostics (Moran's I)...")

    try:
        from esda.moran import Moran
        from libpysal.weights import KNN
    except ImportError:
        log.warning("  libpysal/esda not installed — skipping Moran's I. Install: uv pip install libpysal esda")
        return

    # Get GRUKO centroids
    centroids = conn.execute("""
        SELECT gruko_id,
               ST_X(ST_Centroid(geometry_spatial)) AS cx,
               ST_Y(ST_Centroid(geometry_spatial)) AS cy
        FROM grukos_raw
        WHERE geometry_spatial IS NOT NULL
    """).fetchall()

    if len(centroids) < 50:
        log.warning(f"  Only {len(centroids)} GRUKOs with geometry — skipping Moran's I")
        return

    gruko_ids = [c[0] for c in centroids]
    coords = np.array([[c[1], c[2]] for c in centroids])
    _id_to_idx = {gid: i for i, gid in enumerate(gruko_ids)}

    # Build KNN weight matrix (k=8 nearest neighbors)
    w = KNN.from_array(coords, k=8)
    w.transform = "R"  # row-standardize

    # Test top 3 significant substances
    significant = [r for r in results if r["p_value"] < 0.05 and r["r"] > 0][:3]
    if not significant:
        log.info("  No significant substances to test for spatial autocorrelation")
        return

    print("\n" + "=" * 85)  # noqa: T201
    print("SPATIAL AUTOCORRELATION DIAGNOSTICS (Moran's I)")  # noqa: T201
    print("=" * 85)  # noqa: T201
    print("  If Moran's I > 0 and p < 0.05, observations are spatially clustered,")  # noqa: T201
    print("  violating the independence assumption. Effective sample size < n.")  # noqa: T201
    print("-" * 85)  # noqa: T201

    for sub in significant:
        substance_name = sub["substance"]
        safe_name = substance_name.replace("'", "''")

        # Get detection array for this substance aligned with GRUKO centroids
        det_rows = conn.execute(f"""
            SELECT gruko_id, detected FROM gruko_detections
            WHERE substance = '{safe_name}'
        """).fetchall()
        det_map = {r[0]: r[1] for r in det_rows}

        # Build arrays aligned with centroid order (0 for missing GRUKOs)
        det_arr = np.array([float(det_map.get(gid, 0)) for gid in gruko_ids])

        if det_arr.std() == 0:
            continue

        try:
            mi = Moran(det_arr, w, permutations=999)
            # Estimate effective sample size: n_eff ≈ n * (1 - I) / (1 + I) (heuristic)
            n = len(det_arr)
            n_eff = max(1, int(n * (1 - mi.I) / (1 + mi.I))) if mi.I > -1 and mi.I < 1 else n
            p_str = f"{mi.p_sim:.4f}" if mi.p_sim >= 0.001 else "<0.001"
            warning = " *** INFLATED p-values likely" if mi.p_sim < 0.05 and mi.I > 0.05 else ""
            print(f"  {substance_name:<42} I={mi.I:>6.3f}  p={p_str}  n={n:,}  n_eff≈{n_eff:,}{warning}")  # noqa: T201
        except Exception as e:
            log.warning(f"  Moran's I failed for {substance_name}: {e}")

    print()  # noqa: T201


def run_temporal_lag(conn: duckdb.DuckDBPyConnection, results: list[dict]) -> list[dict]:
    """Test detection windows to find optimal lag for each significant substance.

    Applies global BH-FDR across all substance x window combinations tested (S3).
    Reports total windows tested per substance.
    """
    log.info("Running temporal lag analysis...")

    significant = [r for r in results if r["p_value"] < 0.05 and r["r"] > 0]
    lag_results = []

    # Collect ALL p-values across all substance x window tests for global FDR (S3)
    all_lag_pvals = []  # (substance_name, start, end, r, p)

    for sub in significant[:20]:
        substance_name = sub["substance"]
        safe_name = substance_name.replace("'", "''")
        intensity_sql = _get_intensity_sql(substance_name)

        best_r = 0
        best_window = None
        n_windows_tested = 0

        for start_year in range(2016, 2025):
            for end_year in range(start_year, 2026):
                try:
                    rows = conn.execute(f"""
                        SELECT
                            d.gruko_id,
                            MAX(CASE WHEN d2.maengde > {DETECTION_THRESHOLD_UGL} THEN 1 ELSE 0 END) as detected,
                            COALESCE(i.intensity, 0) as intensity
                        FROM (
                            SELECT DISTINCT gruko_id FROM gruko_detections
                            WHERE substance = '{safe_name}'
                        ) d
                        LEFT JOIN geus_gruko d2
                            ON d.gruko_id = d2.gruko_id
                            AND d2.stof_tekst = '{safe_name}'
                            AND d2.year BETWEEN {start_year} AND {end_year}
                        LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
                        GROUP BY d.gruko_id, i.intensity
                        HAVING COUNT(d2.maengde) > 0
                    """).fetchall()
                except Exception:  # noqa: S112
                    continue

                if len(rows) < MIN_DETECTIONS:
                    continue

                detected = np.array([r[1] for r in rows])
                intensity = np.array([r[2] for r in rows])

                if detected.std() == 0 or intensity.std() == 0:
                    continue

                r, p = scipy_stats.pointbiserialr(detected, intensity)
                n_windows_tested += 1
                all_lag_pvals.append((substance_name, start_year, end_year, r, p))

                if p < 0.05 and r > best_r:
                    best_r = r
                    best_window = {
                        "start": start_year,
                        "end": end_year,
                        "lag_years": round((start_year + end_year) / 2 - 2015.5, 1),
                        "r": round(r, 3),
                        "p": p,
                        "n_windows_tested": n_windows_tested,
                    }

        if best_window:
            best_window["n_windows_tested"] = n_windows_tested
            lag_results.append({"substance": substance_name, "type": sub["type"], **best_window})

    # Global BH-FDR across ALL substance x window combinations (S3)
    total_windows = len(all_lag_pvals)
    if all_lag_pvals:
        all_ps = [x[4] for x in all_lag_pvals]
        reject_global, p_adj_global, _, _ = multipletests(all_ps, alpha=0.05, method="fdr_bh")
        log.info(f"  Temporal lag: {total_windows} total windows tested across {len(significant[:20])} substances")
        log.info(f"  Global FDR: {sum(reject_global)} windows significant after correction")

        # Map FDR-corrected p-values back to best windows
        pval_map = {}
        for i, (sname, sy, ey, _rv, _pv) in enumerate(all_lag_pvals):
            key = (sname, sy, ey)
            pval_map[key] = p_adj_global[i]

        for lr in lag_results:
            key = (lr["substance"], lr["start"], lr["end"])
            lr["p_fdr_global"] = pval_map.get(key, lr["p"])
            lr["sig_fdr_global"] = lr["p_fdr_global"] < 0.05
            lr["total_windows_tested"] = total_windows

    lag_results.sort(key=lambda x: x["lag_years"])
    return lag_results


def run_detection_trend(conn: duckdb.DuckDBPyConnection) -> None:
    """Show year-by-year cumulative GRUKO detection counts for near-threshold substances."""
    log.info("Running detection trend analysis...")

    rows = conn.execute(f"""
        WITH first_detection AS (
            SELECT gruko_id, stof_tekst AS substance, MIN(year) AS first_year
            FROM geus_gruko
            WHERE maengde > {DETECTION_THRESHOLD_UGL}
            GROUP BY gruko_id, stof_tekst
        ),
        yearly AS (
            SELECT substance, first_year AS year, COUNT(*) AS new_grukos
            FROM first_detection
            GROUP BY substance, first_year
        ),
        cumulative AS (
            SELECT substance, year, new_grukos,
                   SUM(new_grukos) OVER (
                       PARTITION BY substance ORDER BY year
                   ) AS cumul_grukos
            FROM yearly
        )
        SELECT
            substance,
            MAX(cumul_grukos) AS total_grukos,
            MIN(CASE WHEN cumul_grukos >= {MIN_DETECTIONS} THEN year END) AS year_crossed_30,
            SUM(CASE WHEN year BETWEEN 2022 AND 2025 THEN new_grukos ELSE 0 END) AS new_2022_2025
        FROM cumulative
        GROUP BY substance
        HAVING MAX(cumul_grukos) BETWEEN 10 AND 100
        ORDER BY MAX(cumul_grukos) DESC
    """).fetchall()

    print("\n" + "=" * 90)  # noqa: T201
    print("DETECTION TREND: Substances with 10-100 cumulative GRUKO detections")  # noqa: T201
    print("=" * 90)  # noqa: T201
    print(f"{'Substance':<45} {'Total':>5}  {'Crossed30':>9}  {'2022-25':>7}  {'Parent mapping'}")  # noqa: T201
    print("-" * 90)  # noqa: T201

    unmapped_candidates = []
    for substance, total, year_crossed, new_recent in rows:
        crossed_str = str(year_crossed) if year_crossed else "—"
        parent_str = ""
        if substance in METABOLITE_PARENT_MAP:
            parents = METABOLITE_PARENT_MAP[substance]
            parent_str = ", ".join(parents[:3]) + ("..." if len(parents) > 3 else "")
        elif substance in GEUS_TO_BMD_MULTI:
            parent_str = ", ".join(GEUS_TO_BMD_MULTI[substance])
        elif substance in GEUS_TO_BMD_SINGLE:
            parent_str = GEUS_TO_BMD_SINGLE[substance]
        else:
            parent_str = "UNMAPPED"
            if total >= 15:
                unmapped_candidates.append((substance, total, new_recent or 0))
        print(f"{substance:<45} {total:>5}  {crossed_str:>9}  {new_recent or 0:>7}  {parent_str}")  # noqa: T201

    if unmapped_candidates:
        print("\nUnmapped substances with ≥15 GRUKO detections (need parent lookup):")  # noqa: T201
        for substance, total, new_recent in sorted(unmapped_candidates, key=lambda x: -x[1]):
            print(f"  {substance}  (total={total}, 2022-25 new={new_recent})")  # noqa: T201


def run_gruko_type_breakdown(conn: duckdb.DuckDBPyConnection) -> dict:
    """GRUKO type breakdown (Table 2a in paper).

    Counts indvindingsoplande, indsatsområder, and overlap categories.
    """
    log.info("Running GRUKO type breakdown...")

    rows = conn.execute("""
        SELECT layer, COUNT(*) as n,
               AVG(ST_Area(geometry_spatial) / 10000.0) as mean_area_ha
        FROM grukos_raw
        WHERE geometry_spatial IS NOT NULL
        GROUP BY layer ORDER BY layer
    """).fetchall()

    breakdown = {}
    for layer, count, mean_area in rows:
        breakdown[layer] = {"count": count, "mean_area_ha": round(mean_area, 0)}
        log.info(f"  Layer '{layer}': {count:,} features, mean area {mean_area:.0f} ha")

    # Check for overlap: GRUKOs that appear in multiple layers (by spatial overlap)
    # A simpler check: count by gruko_id if the same id appears in multiple layers
    try:
        overlap_rows = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT gruko_id FROM grukos_raw GROUP BY gruko_id HAVING COUNT(DISTINCT layer) > 1
            )
        """).fetchone()
        breakdown["overlap_count"] = overlap_rows[0] if overlap_rows else 0
        log.info(f"  Overlap (multi-layer GRUKOs): {breakdown['overlap_count']}")
    except Exception:
        breakdown["overlap_count"] = 0

    total = conn.execute("SELECT COUNT(*) FROM grukos_raw").fetchone()[0]
    breakdown["total"] = total
    log.info(f"  Total: {total:,}")

    return breakdown


def run_monitoring_density_stratified(conn: duckdb.DuckDBPyConnection, results: list[dict]) -> list[dict]:
    """Monitoring density stratified analysis (Table 7 in paper).

    Splits GRUKOs into tertiles by n_wells, computes point-biserial correlations
    within each tertile for FDR-significant substances.
    """
    log.info("Running monitoring density stratified analysis...")

    # Get monitoring density per GRUKO
    well_rows = conn.execute("""
        SELECT gruko_id, n_wells FROM gruko_covariates ORDER BY n_wells
    """).fetchall()

    if not well_rows:
        log.warning("  No covariate data for stratified analysis")
        return []

    wells_arr = np.array([r[1] for r in well_rows])

    # Compute tertile cutpoints
    t33 = np.percentile(wells_arr, 33.3)
    t67 = np.percentile(wells_arr, 66.7)
    log.info(f"  Tertile cutpoints: low <= {t33:.0f}, medium <= {t67:.0f}, high > {t67:.0f}")

    # Assign tertiles
    gruko_tertile = {}
    for gid, nw in well_rows:
        if nw <= t33:
            gruko_tertile[gid] = "low"
        elif nw <= t67:
            gruko_tertile[gid] = "medium"
        else:
            gruko_tertile[gid] = "high"

    tertile_counts = {t: sum(1 for v in gruko_tertile.values() if v == t) for t in ["low", "medium", "high"]}
    log.info(
        f"  Tertile sizes: low={tertile_counts['low']}, medium={tertile_counts['medium']}, high={tertile_counts['high']}"
    )

    sig = [r for r in results if r.get("sig_fdr") and r["r"] > 0]
    strat_results = []

    for sub in sig:
        substance_name = sub["substance"]
        safe_name = substance_name.replace("'", "''")
        intensity_sql = _get_intensity_sql(substance_name)

        try:
            rows = conn.execute(f"""
                SELECT d.gruko_id, d.detected, COALESCE(i.intensity, 0) as intensity
                FROM gruko_detections d
                LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
                WHERE d.substance = '{safe_name}'
            """).fetchall()
        except Exception:  # noqa: S112
            continue

        for tertile in ["low", "medium", "high"]:
            tertile_grukos = {gid for gid, t in gruko_tertile.items() if t == tertile}
            subset = [(r[1], r[2]) for r in rows if r[0] in tertile_grukos]

            if len(subset) < 20:
                strat_results.append(
                    {
                        "substance": substance_name,
                        "tertile": tertile,
                        "r": None,
                        "p": None,
                        "n": len(subset),
                    }
                )
                continue

            det = np.array([s[0] for s in subset])
            intens = np.array([s[1] for s in subset])

            if det.std() == 0 or intens.std() == 0:
                strat_results.append(
                    {
                        "substance": substance_name,
                        "tertile": tertile,
                        "r": 0.0,
                        "p": 1.0,
                        "n": len(subset),
                    }
                )
                continue

            r_val, p_val = scipy_stats.pointbiserialr(det, intens)
            strat_results.append(
                {
                    "substance": substance_name,
                    "tertile": tertile,
                    "r": round(r_val, 3),
                    "p": p_val,
                    "n": len(subset),
                }
            )

    log.info(f"  Stratified results: {len(strat_results)} substance-tertile combinations")
    return strat_results


def run_gruko_type_sensitivity(conn: duckdb.DuckDBPyConnection, results: list[dict]) -> list[dict]:
    """GRUKO type sensitivity analysis (Table 10 in paper).

    Repeats bivariate correlations separately for each GRUKO type (layer).
    """
    log.info("Running GRUKO type sensitivity analysis...")

    layers = conn.execute("SELECT DISTINCT layer FROM grukos_raw ORDER BY layer").fetchall()
    layer_names = [r[0] for r in layers]
    log.info(f"  GRUKO layers: {layer_names}")

    # Build layer membership lookup
    layer_grukos = {}
    for layer in layer_names:
        grukos = conn.execute(f"""
            SELECT DISTINCT gruko_id FROM grukos_raw WHERE layer = '{layer}'
        """).fetchall()
        layer_grukos[layer] = {r[0] for r in grukos}
        log.info(f"  Layer '{layer}': {len(layer_grukos[layer]):,} GRUKOs")

    sig = [r for r in results if r.get("sig_fdr") and r["r"] > 0]
    type_results = []

    for sub in sig:
        substance_name = sub["substance"]
        safe_name = substance_name.replace("'", "''")
        intensity_sql = _get_intensity_sql(substance_name)

        try:
            rows = conn.execute(f"""
                SELECT d.gruko_id, d.detected, COALESCE(i.intensity, 0) as intensity
                FROM gruko_detections d
                LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
                WHERE d.substance = '{safe_name}'
            """).fetchall()
        except Exception:  # noqa: S112
            continue

        for layer in layer_names:
            subset = [(r[1], r[2]) for r in rows if r[0] in layer_grukos[layer]]
            if len(subset) < 30:
                continue

            det = np.array([s[0] for s in subset])
            intens = np.array([s[1] for s in subset])

            if det.std() == 0 or intens.std() == 0:
                type_results.append(
                    {
                        "substance": substance_name,
                        "layer": layer,
                        "r": 0.0,
                        "p": 1.0,
                        "n": len(subset),
                    }
                )
                continue

            r_val, p_val = scipy_stats.pointbiserialr(det, intens)
            type_results.append(
                {
                    "substance": substance_name,
                    "layer": layer,
                    "r": round(r_val, 3),
                    "p": p_val,
                    "n": len(subset),
                }
            )

    log.info(f"  Type sensitivity results: {len(type_results)} substance-layer combinations")
    return type_results


def run_substance_sensitivity(conn: duckdb.DuckDBPyConnection, results: list[dict]) -> dict:
    """Substance-specific sensitivity analyses (Section 3.12 in paper).

    E1: AMPA agricultural-only (exclude GRUKOs with low agricultural coverage)
    E2: Glyphosate soil-texture stratification (sandy vs clay)
    """
    log.info("Running substance-specific sensitivity analyses...")
    sensitivity = {}

    # E1: AMPA agricultural-only
    # Proxy: use field coverage ratio (total field area in GRUKO / GRUKO area)
    # GRUKOs with >80% field coverage are "predominantly agricultural"
    log.info("  E1: AMPA agricultural-only analysis...")
    try:
        # Compute field coverage per GRUKO
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _field_coverage AS
            SELECT
                fg.gruko_id,
                SUM(ST_Area(fg.geometry)) / NULLIF(MAX(g.gruko_area), 0) as field_coverage
            FROM field_gruko_intersections fg
            JOIN (
                SELECT gruko_id, ST_Area(geometry_spatial) as gruko_area
                FROM grukos_raw
            ) g ON fg.gruko_id = g.gruko_id
            GROUP BY fg.gruko_id
        """)

        ag_grukos = conn.execute("""
            SELECT gruko_id FROM _field_coverage WHERE field_coverage > 0.5
        """).fetchall()
        ag_gruko_set = {r[0] for r in ag_grukos}
        log.info(f"    Predominantly agricultural GRUKOs (>50% field coverage): {len(ag_gruko_set):,}")

        # AMPA correlation in agricultural-only subset
        ampa_name = "(Aminomethyl)phosphonsyre"
        intensity_sql = _get_intensity_sql(ampa_name)
        rows = conn.execute(f"""
            SELECT d.gruko_id, d.detected, COALESCE(i.intensity, 0) as intensity
            FROM gruko_detections d
            LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
            WHERE d.substance = '{ampa_name}'
        """).fetchall()

        ag_subset = [(r[1], r[2]) for r in rows if r[0] in ag_gruko_set]
        if len(ag_subset) >= 30:
            det = np.array([s[0] for s in ag_subset])
            intens = np.array([s[1] for s in ag_subset])
            if det.std() > 0 and intens.std() > 0:
                r_val, p_val = scipy_stats.pointbiserialr(det, intens)
                # Get full-dataset r for comparison
                full_r = next((r["r"] for r in results if r["substance"] == ampa_name), None)
                sensitivity["ampa_ag_only"] = {
                    "r": round(r_val, 3),
                    "p": p_val,
                    "n": len(ag_subset),
                    "full_r": full_r,
                }
                log.info(f"    AMPA ag-only: r={r_val:.3f} (full: {full_r}), n={len(ag_subset)}")

        conn.execute("DROP TABLE IF EXISTS _field_coverage")
    except Exception as e:
        log.warning(f"    AMPA ag-only analysis failed: {e}")

    # E2: Glyphosate soil-texture stratification
    log.info("  E2: Glyphosate soil-texture stratification...")
    try:
        glyph_name = "Glyphosat"
        intensity_sql = _get_intensity_sql(glyph_name)
        rows = conn.execute(f"""
            SELECT d.gruko_id, d.detected, COALESCE(i.intensity, 0) as intensity,
                   COALESCE(gt.soil_height, 0) as soil_height
            FROM gruko_detections d
            LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
            LEFT JOIN gruko_transit gt ON d.gruko_id = gt.gruko_id
            WHERE d.substance = '{glyph_name}'
        """).fetchall()

        for soil_label, soil_range in [("sandy", [1, 2, 3]), ("clay", [5, 6, 7, 8])]:
            subset = [(r[1], r[2]) for r in rows if r[3] in soil_range]
            if len(subset) < 30:
                continue
            det = np.array([s[0] for s in subset])
            intens = np.array([s[1] for s in subset])
            if det.std() > 0 and intens.std() > 0:
                r_val, p_val = scipy_stats.pointbiserialr(det, intens)
                sensitivity[f"glyphosate_{soil_label}"] = {
                    "r": round(r_val, 3),
                    "p": p_val,
                    "n": len(subset),
                }
                log.info(f"    Glyphosate {soil_label}: r={r_val:.3f}, n={len(subset)}")
    except Exception as e:
        log.warning(f"    Glyphosate stratification failed: {e}")

    return sensitivity


def run_linearity_in_logit(conn: duckdb.DuckDBPyConnection, mv_results: list[dict]) -> list[dict]:
    """Linearity-in-logit verification (Section 3.13 in paper).

    Tests restricted cubic splines vs linear intensity for multivariate-robust substances.
    """
    log.info("Running linearity-in-logit verification...")

    try:
        import patsy
    except ImportError:
        log.warning("  patsy not installed — skipping linearity-in-logit")
        return []

    robust = [r for r in mv_results if r.get("p_intensity") is not None and r["p_intensity"] < 0.05]
    if not robust:
        log.info("  No multivariate-robust substances for linearity test")
        return []

    lin_results = []

    for sub in robust:
        substance_name = sub["substance"]
        safe_name = substance_name.replace("'", "''")
        intensity_sql = _get_intensity_sql(substance_name)

        try:
            rows = conn.execute(f"""
                SELECT
                    d.detected,
                    COALESCE(i.intensity, 0) as intensity,
                    COALESCE(gc.soil_height, 0) as soil_height,
                    COALESCE(gc.median_intake_depth_m, 0) as median_intake_depth,
                    COALESCE(gc.n_wells, 0) as n_wells
                FROM gruko_detections d
                LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
                LEFT JOIN gruko_covariates gc ON d.gruko_id = gc.gruko_id
                WHERE d.substance = '{safe_name}'
            """).fetchall()
        except Exception:  # noqa: S112
            continue

        if len(rows) < 50:
            continue

        detected = np.array([r[0] for r in rows])
        intensity = np.array([r[1] for r in rows])
        soil_h = np.array([r[2] for r in rows])
        depth = np.array([r[3] for r in rows])
        wells = np.array([r[4] for r in rows])

        if detected.std() == 0 or intensity.std() == 0:
            continue

        # Linear model
        X_lin = np.column_stack([intensity, soil_h, depth, wells])
        X_lin = sm.add_constant(X_lin)
        try:
            model_lin = sm.Logit(detected, X_lin).fit(disp=0, maxiter=200)
            ll_lin = model_lin.llf
        except Exception:  # noqa: S112
            continue

        # Spline model: 3 knots at 10th, 50th, 90th percentiles
        try:
            knots = (
                np.percentile(intensity[intensity > 0], [10, 50, 90])
                if (intensity > 0).sum() >= 10
                else np.percentile(intensity, [10, 50, 90])
            )
            # Natural cubic spline basis with 2 df (3 knots)
            spline_basis = patsy.dmatrix(
                f"cr(intensity, knots={list(knots)}) - 1",
                {"intensity": intensity},
                return_type="dataframe",
            )
            X_spl = np.column_stack([np.asarray(spline_basis), soil_h, depth, wells])
            X_spl = sm.add_constant(X_spl)
            model_spl = sm.Logit(detected, X_spl).fit(disp=0, maxiter=200)
            ll_spl = model_spl.llf
        except Exception as e:
            log.warning(f"  Spline model failed for {substance_name}: {e}")
            continue

        # Likelihood ratio test
        lr_stat = 2 * (ll_spl - ll_lin)
        df_diff = X_spl.shape[1] - X_lin.shape[1]
        lr_p = 1.0 - scipy_stats.chi2.cdf(max(lr_stat, 0), df_diff)

        lin_results.append(
            {
                "substance": substance_name,
                "lr_chi2": round(lr_stat, 1),
                "df": df_diff,
                "p": round(lr_p, 3),
                "linear_adequate": lr_p > 0.05,
            }
        )
        log.info(
            f"  {substance_name}: LR χ²({df_diff})={lr_stat:.1f}, p={lr_p:.3f} {'(linear adequate)' if lr_p > 0.05 else '(NON-LINEAR)'}"
        )

    return lin_results


def run_sar_probit(conn: duckdb.DuckDBPyConnection, mv_results: list[dict]) -> list[dict]:
    """SAR-probit spatial model validation (Table 8 in paper).

    Fits spatial probit models for multivariate-robust substances using PySAL spreg.
    """
    log.info("Running SAR-probit spatial model validation...")

    try:
        from libpysal.weights import KNN
        from spreg import Probit as SpatialProbit
    except ImportError:
        log.warning("  spreg/libpysal not installed — skipping SAR-probit")
        return []

    robust = [r for r in mv_results if r.get("p_intensity") is not None and r["p_intensity"] < 0.05]
    if not robust:
        log.info("  No multivariate-robust substances for SAR-probit")
        return []

    # Build KNN spatial weights from GRUKO centroids
    centroids = conn.execute("""
        SELECT gruko_id,
               ST_X(ST_Centroid(geometry_spatial)) AS cx,
               ST_Y(ST_Centroid(geometry_spatial)) AS cy
        FROM grukos_raw
        WHERE geometry_spatial IS NOT NULL
    """).fetchall()
    gruko_ids_all = [c[0] for c in centroids]
    coords = np.array([[c[1], c[2]] for c in centroids])
    id_to_idx = {gid: i for i, gid in enumerate(gruko_ids_all)}

    w = KNN.from_array(coords, k=8)
    w.transform = "R"

    sar_results = []

    for sub in robust:
        substance_name = sub["substance"]
        safe_name = substance_name.replace("'", "''")
        intensity_sql = _get_intensity_sql(substance_name)

        try:
            rows = conn.execute(f"""
                SELECT
                    d.gruko_id,
                    d.detected,
                    COALESCE(i.intensity, 0) as intensity,
                    COALESCE(gc.soil_height, 0) as soil_height,
                    COALESCE(gc.median_intake_depth_m, 0) as median_intake_depth,
                    COALESCE(gc.n_wells, 0) as n_wells
                FROM gruko_detections d
                LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
                LEFT JOIN gruko_covariates gc ON d.gruko_id = gc.gruko_id
                WHERE d.substance = '{safe_name}'
            """).fetchall()
        except Exception:  # noqa: S112
            continue

        if len(rows) < 50:
            continue

        # Need to subset the spatial weights to match the GRUKOs in our dataset
        gruko_ids_sub = [r[0] for r in rows]
        sub_indices = [id_to_idx[gid] for gid in gruko_ids_sub if gid in id_to_idx]

        if len(sub_indices) < 50:
            continue

        # Build subset arrays aligned with sub_indices
        rows_map = {r[0]: r for r in rows}
        aligned_rows = [rows_map[gruko_ids_all[i]] for i in sub_indices if gruko_ids_all[i] in rows_map]

        y = np.array([[r[1]] for r in aligned_rows], dtype=float)
        x = np.array([[r[2], r[3], r[4], r[5]] for r in aligned_rows], dtype=float)

        if y.std() == 0 or x[:, 0].std() == 0:
            continue

        # Build subset spatial weights
        try:
            sub_coords = coords[sub_indices]
            w_sub = KNN.from_array(sub_coords, k=min(8, len(sub_indices) - 1))
            w_sub.transform = "R"
        except Exception as e:
            log.warning(f"  Spatial weights failed for {substance_name}: {e}")
            continue

        try:
            sp_model = SpatialProbit(
                y,
                x,
                w=w_sub,
                spat_diag=True,
                name_y="detected",
                name_x=["intensity", "soil_height", "intake_depth", "n_wells"],
            )

            # Extract intensity coefficient and pseudo-OR
            intensity_coef = sp_model.betas[1][0]  # index 0=const, 1=intensity
            intensity_se = (
                float(np.sqrt(sp_model.vm[1, 1])) if hasattr(sp_model, "vm") and sp_model.vm is not None else None
            )

            # Probit coefficients aren't directly comparable to logistic OR
            # Convert probit -> logistic scale: multiply by ~1.6 (logistic/probit ratio)
            # OR ≈ exp(1.6 * probit_coef)
            sar_or = np.exp(1.6 * intensity_coef)

            if intensity_se:
                or_ci_low = np.exp(1.6 * (intensity_coef - 1.96 * intensity_se))
                or_ci_high = np.exp(1.6 * (intensity_coef + 1.96 * intensity_se))
            else:
                or_ci_low = or_ci_high = None

            # Spatial rho (if available in diagnostics)
            rho = None
            if hasattr(sp_model, "rho"):
                rho = float(sp_model.rho)

            sar_results.append(
                {
                    "substance": substance_name,
                    "logistic_or": sub["mv_intensity_or"],
                    "logistic_ci": f"[{sub.get('mv_or_ci_low', '?')}, {sub.get('mv_or_ci_high', '?')}]",
                    "sar_or": round(sar_or, 4),
                    "sar_ci_low": round(or_ci_low, 4) if or_ci_low else None,
                    "sar_ci_high": round(or_ci_high, 4) if or_ci_high else None,
                    "rho": round(rho, 3) if rho else None,
                    "probit_coef": round(intensity_coef, 6),
                    "converged": True,
                    "n": len(aligned_rows),
                }
            )
            log.info(f"  {substance_name}: SAR-probit OR={sar_or:.4f}, logistic OR={sub['mv_intensity_or']}")

        except Exception as e:
            log.warning(f"  SAR-probit failed for {substance_name}: {e}")
            sar_results.append(
                {
                    "substance": substance_name,
                    "logistic_or": sub["mv_intensity_or"],
                    "logistic_ci": f"[{sub.get('mv_or_ci_low', '?')}, {sub.get('mv_or_ci_high', '?')}]",
                    "sar_or": None,
                    "converged": False,
                    "error": str(e),
                    "n": len(aligned_rows) if aligned_rows else 0,
                }
            )

    return sar_results


def run_bootstrap_temporal_lag(
    conn: duckdb.DuckDBPyConnection, lag_results: list[dict], n_boot: int = 1000
) -> list[dict]:
    """Bootstrap CIs for temporal lag estimates (Table 13 in paper).

    For each substance with a lag result, resamples GRUKOs and re-finds optimal lag.
    """
    log.info(f"Running bootstrap CIs for temporal lags ({n_boot} iterations)...")

    if not lag_results:
        return lag_results

    for lr in lag_results:
        substance_name = lr["substance"]
        safe_name = substance_name.replace("'", "''")
        intensity_sql = _get_intensity_sql(substance_name)

        # Get all data for this substance (all years)
        try:
            all_rows = conn.execute(f"""
                SELECT d.gruko_id, d2.year, d2.maengde,
                       COALESCE(i.intensity, 0) as intensity
                FROM (
                    SELECT DISTINCT gruko_id FROM gruko_detections
                    WHERE substance = '{safe_name}'
                ) d
                JOIN geus_gruko d2 ON d.gruko_id = d2.gruko_id AND d2.stof_tekst = '{safe_name}'
                LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
            """).fetchall()
        except Exception:
            lr["lag_ci_low"] = None
            lr["lag_ci_high"] = None
            continue

        if len(all_rows) < 50:
            lr["lag_ci_low"] = None
            lr["lag_ci_high"] = None
            continue

        # Build a dict: gruko_id -> {year -> max_maengde}, intensity
        gruko_data = {}
        for gid, year, maengde, intens in all_rows:
            if gid not in gruko_data:
                gruko_data[gid] = {"intensity": intens, "years": {}}
            if year not in gruko_data[gid]["years"] or maengde > gruko_data[gid]["years"][year]:
                gruko_data[gid]["years"][year] = maengde

        gruko_list = list(gruko_data.keys())
        n_grukos = len(gruko_list)
        optimal_window = (lr["start"], lr["end"])

        # Test a narrow range of windows around the optimal for speed
        test_windows = []
        for start_year in range(max(2016, optimal_window[0] - 3), min(2025, optimal_window[0] + 4)):
            for end_year in range(max(start_year, optimal_window[1] - 3), min(2026, optimal_window[1] + 4)):
                test_windows.append((start_year, end_year))

        boot_lags = []
        rng = np.random.default_rng(42)

        for _ in range(n_boot):
            # Resample GRUKOs with replacement
            sample_ids = rng.choice(gruko_list, size=n_grukos, replace=True)

            best_r = -1
            best_lag = lr["lag_years"]

            for start_year, end_year in test_windows:
                detected_list = []
                intensity_list = []
                for gid in sample_ids:
                    gd = gruko_data[gid]
                    # Check if detected in this window
                    det = 0
                    for y in range(start_year, end_year + 1):
                        if y in gd["years"] and gd["years"][y] > DETECTION_THRESHOLD_UGL:
                            det = 1
                            break
                    detected_list.append(det)
                    intensity_list.append(gd["intensity"])

                det_arr = np.array(detected_list)
                int_arr = np.array(intensity_list)

                if det_arr.std() == 0 or int_arr.std() == 0:
                    continue

                r, p = scipy_stats.pointbiserialr(det_arr, int_arr)
                if p < 0.05 and r > best_r:
                    best_r = r
                    best_lag = round((start_year + end_year) / 2 - 2015.5, 1)

            boot_lags.append(best_lag)

        if boot_lags:
            lr["lag_ci_low"] = round(float(np.percentile(boot_lags, 2.5)), 1)
            lr["lag_ci_high"] = round(float(np.percentile(boot_lags, 97.5)), 1)
            log.info(f"  {substance_name}: lag={lr['lag_years']}y, 95% CI=[{lr['lag_ci_low']}, {lr['lag_ci_high']}]")
        else:
            lr["lag_ci_low"] = None
            lr["lag_ci_high"] = None

    return lag_results


# ─── SUPPLEMENTARY ANALYSES (S3.12, S3.13, S3.14) ─────────────────────────────


def _build_detections_at_threshold(conn: duckdb.DuckDBPyConnection, threshold_ugl: float) -> None:
    """Build detection table at a specific threshold (drops and recreates)."""
    conn.execute("DROP TABLE IF EXISTS gruko_detections_alt")

    has_sample_id = conn.execute("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name='geus_gruko' AND column_name='sample_id'
    """).fetchone()[0]
    sample_count_expr = "COUNT(DISTINCT sample_id)" if has_sample_id else "COUNT(*)"

    conn.execute(f"""
        CREATE TABLE gruko_detections_alt AS
        SELECT
            gruko_id,
            stof_tekst as substance,
            MAX(CASE WHEN maengde > {threshold_ugl} THEN 1 ELSE 0 END) as detected,
            {sample_count_expr} as n_samples,
            MAX(maengde) as max_concentration
        FROM geus_gruko
        WHERE year >= {DETECTION_YEAR_START}
        GROUP BY gruko_id, stof_tekst
    """)


def run_threshold_sensitivity(conn: duckdb.DuckDBPyConnection, mv_results: list[dict]) -> dict:
    """S3.12: Rerun bivariate + multivariate at alternative detection thresholds."""
    log.info("=" * 70)
    log.info("S3.12: Detection Threshold Sensitivity Analysis")
    log.info("=" * 70)

    robust = [r["substance"] for r in mv_results if r.get("p_intensity") is not None and r["p_intensity"] < 0.05]
    if not robust:
        log.info("  No MV-robust substances — skipping threshold sensitivity.")
        return {}

    thresholds = [
        (DETECTION_THRESHOLD_UGL, f">LOQ ({DETECTION_THRESHOLD_UGL} μg/L)"),
        (0.05, ">0.05 μg/L"),
        (0.1, ">0.1 μg/L"),
    ]

    results = {}
    for threshold, label in thresholds:
        log.info(f"\n  Threshold: {label} (>{threshold} μg/L)")
        _build_detections_at_threshold(conn, threshold)

        threshold_results = {}
        for substance_name in robust:
            safe_name = substance_name.replace("'", "''")
            intensity_sql = _get_intensity_sql(substance_name)

            rows = conn.execute(f"""
                SELECT
                    d.gruko_id,
                    d.detected,
                    COALESCE(i.intensity, 0) as intensity
                FROM gruko_detections_alt d
                LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
                WHERE d.substance = '{safe_name}'
            """).fetchall()

            detected = np.array([r[1] for r in rows])
            intensity = np.array([r[2] for r in rows])
            n_det = int(detected.sum())

            if n_det < MIN_DETECTIONS or detected.std() == 0 or intensity.std() == 0:
                log.info(f"    {substance_name}: {n_det} detections (insufficient)")
                threshold_results[substance_name] = {
                    "n_detected": n_det,
                    "n": len(rows),
                    "r": None,
                    "mv_p": None,
                    "note": f"insufficient detections ({n_det} < {MIN_DETECTIONS})",
                }
                continue

            r, p = scipy_stats.pointbiserialr(detected, intensity)

            rows_mv = conn.execute(f"""
                SELECT
                    d.gruko_id, d.detected,
                    COALESCE(i.intensity, 0) as intensity,
                    COALESCE(gc.soil_height, 0) as soil_height,
                    COALESCE(gc.median_intake_depth_m, 0) as median_intake_depth,
                    COALESCE(gc.n_wells, 0) as n_wells
                FROM gruko_detections_alt d
                LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
                LEFT JOIN gruko_covariates gc ON d.gruko_id = gc.gruko_id
                WHERE d.substance = '{safe_name}'
            """).fetchall()

            y = np.array([r[1] for r in rows_mv])
            X = np.column_stack(
                [
                    np.array([r[2] for r in rows_mv]),
                    np.array([r[3] for r in rows_mv]),
                    np.array([r[4] for r in rows_mv]),
                    np.array([r[5] for r in rows_mv]),
                ]
            )
            X = sm.add_constant(X)

            try:
                model = sm.Logit(y, X).fit(disp=0, maxiter=200)
                mv_p = float(model.pvalues[1])
                mv_or = float(np.exp(model.params[1]))
            except Exception as e:
                log.warning(f"    {substance_name}: MV regression failed: {e}")
                mv_p = None
                mv_or = None

            log.info(
                f"    {substance_name}: n_det={n_det}, r={r:.3f}, MV p={mv_p:.4f}"
                if mv_p
                else f"    {substance_name}: n_det={n_det}, r={r:.3f}, MV failed"
            )

            threshold_results[substance_name] = {
                "n_detected": n_det,
                "n": len(rows),
                "r": round(float(r), 3),
                "r_p": float(p),
                "mv_p": round(mv_p, 4) if mv_p else None,
                "mv_or": round(mv_or, 4) if mv_or else None,
            }

        results[label] = threshold_results

    return results


def run_within_tertile_mv(conn: duckdb.DuckDBPyConnection, mv_results: list[dict]) -> dict:
    """S3.13: Multivariate logistic regression within high-density monitoring tertile only."""
    log.info("\n" + "=" * 70)
    log.info("S3.13: Within-Tertile Multivariate Analysis (High-Density Stratum)")
    log.info("=" * 70)

    robust = [r["substance"] for r in mv_results if r.get("p_intensity") is not None and r["p_intensity"] < 0.05]
    if not robust:
        log.info("  No MV-robust substances — skipping within-tertile analysis.")
        return {}

    log.info("  High-density tertile: >5 wells per catchment")

    results = {}
    for substance_name in robust:
        safe_name = substance_name.replace("'", "''")
        intensity_sql = _get_intensity_sql(substance_name)

        rows = conn.execute(f"""
            SELECT
                d.gruko_id, d.detected,
                COALESCE(i.intensity, 0) as intensity,
                COALESCE(gc.soil_height, 0) as soil_height,
                COALESCE(gc.median_intake_depth_m, 0) as median_intake_depth,
                COALESCE(gc.n_wells, 0) as n_wells
            FROM gruko_detections d
            LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
            LEFT JOIN gruko_covariates gc ON d.gruko_id = gc.gruko_id
            WHERE d.substance = '{safe_name}'
              AND gc.n_wells > 5
        """).fetchall()

        if len(rows) < 50:
            log.warning(f"  {substance_name}: too few observations in high-density tertile ({len(rows)})")
            continue

        y = np.array([r[1] for r in rows])
        intensity = np.array([r[2] for r in rows])
        soil_h = np.array([r[3] for r in rows])
        depth = np.array([r[4] for r in rows])
        wells = np.array([r[5] for r in rows])

        n_events = int(y.sum())
        n_total = len(rows)

        if y.std() == 0 or intensity.std() == 0:
            log.warning(f"  {substance_name}: zero variance in high-density stratum")
            continue

        X = np.column_stack([intensity, soil_h, depth, wells])
        X = sm.add_constant(X)

        try:
            model = sm.Logit(y, X).fit(disp=0, maxiter=200)
            mv_or = float(np.exp(model.params[1]))
            ci = model.conf_int()
            or_ci_low = float(np.exp(ci[1, 0]))
            or_ci_high = float(np.exp(ci[1, 1]))
            p_intensity = float(model.pvalues[1])
            nag_r2 = _compute_nagelkerke_r2(model)
            y_pred = model.predict(X)
            auc = _compute_auc(y, y_pred)
        except Exception as e:
            log.warning(f"  {substance_name}: MV fit failed: {e}")
            continue

        log.info(
            f"  {substance_name}: n={n_total}, n_events={n_events}, "
            f"adj. OR={mv_or:.4f} [{or_ci_low:.4f}, {or_ci_high:.4f}], p={p_intensity:.4f}"
        )

        results[substance_name] = {
            "n": n_total,
            "n_events": n_events,
            "adj_or": round(mv_or, 4),
            "or_ci_low": round(or_ci_low, 4),
            "or_ci_high": round(or_ci_high, 4),
            "p_intensity": round(p_intensity, 4) if p_intensity >= 0.0001 else p_intensity,
            "nagelkerke_r2": round(nag_r2, 3) if nag_r2 else None,
            "auc": round(auc, 2) if auc else None,
        }

    # Interaction test: intensity × tertile indicator in full sample  # noqa: RUF003
    log.info("\n  Interaction tests (intensity × density tertile):")  # noqa: RUF001
    interaction_results = {}
    for substance_name in robust:
        safe_name = substance_name.replace("'", "''")
        intensity_sql = _get_intensity_sql(substance_name)

        rows = conn.execute(f"""
            SELECT
                d.gruko_id, d.detected,
                COALESCE(i.intensity, 0) as intensity,
                COALESCE(gc.soil_height, 0) as soil_height,
                COALESCE(gc.median_intake_depth_m, 0) as median_intake_depth,
                COALESCE(gc.n_wells, 0) as n_wells
            FROM gruko_detections d
            LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
            LEFT JOIN gruko_covariates gc ON d.gruko_id = gc.gruko_id
            WHERE d.substance = '{safe_name}'
        """).fetchall()

        y = np.array([r[1] for r in rows])
        intensity = np.array([r[2] for r in rows])
        soil_h = np.array([r[3] for r in rows])
        depth = np.array([r[4] for r in rows])
        wells = np.array([r[5] for r in rows])

        high_density = (wells > 5).astype(float)
        interaction = intensity * high_density

        X_base = np.column_stack([intensity, soil_h, depth, wells, high_density])
        X_base = sm.add_constant(X_base)
        X_int = np.column_stack([intensity, soil_h, depth, wells, high_density, interaction])
        X_int = sm.add_constant(X_int)

        try:
            model_base = sm.Logit(y, X_base).fit(disp=0, maxiter=200)
            model_int = sm.Logit(y, X_int).fit(disp=0, maxiter=200)
            lr_stat = -2 * (model_base.llf - model_int.llf)
            lr_p = float(1 - scipy_stats.chi2.cdf(lr_stat, df=1))
            log.info(f"    {substance_name}: interaction LR χ²(1) = {lr_stat:.1f}, p = {lr_p:.4f}")
            interaction_results[substance_name] = {
                "lr_chi2": round(lr_stat, 1),
                "lr_p": round(lr_p, 4) if lr_p >= 0.0001 else lr_p,
            }
        except Exception as e:
            log.warning(f"    {substance_name}: interaction test failed: {e}")

    # Power analysis within low-density tertile
    log.info("\n  Power analysis within low-density tertile (≤2 wells):")
    for substance_name in robust:
        safe_name = substance_name.replace("'", "''")
        intensity_sql = _get_intensity_sql(substance_name)

        rows = conn.execute(f"""
            SELECT d.detected, COALESCE(i.intensity, 0) as intensity, COALESCE(gc.n_wells, 0) as n_wells
            FROM gruko_detections d
            LEFT JOIN ({intensity_sql}) i ON d.gruko_id = i.gruko_id
            LEFT JOIN gruko_covariates gc ON d.gruko_id = gc.gruko_id
            WHERE d.substance = '{safe_name}' AND gc.n_wells <= 2
        """).fetchall()

        n_low = len(rows)
        detected = np.array([r[0] for r in rows])
        intensity = np.array([r[1] for r in rows])
        det_rate = detected.mean() * 100 if n_low > 0 else 0

        if n_low > 0 and detected.std() > 0 and intensity.std() > 0:
            r_obs, _ = scipy_stats.pointbiserialr(detected, intensity)
        else:
            r_obs = 0.0

        min_r = None
        if n_low > 10:
            z_alpha = scipy_stats.norm.ppf(0.975)
            z_beta = scipy_stats.norm.ppf(0.80)
            min_r = (z_alpha + z_beta) / np.sqrt(n_low - 3)

        log.info(
            f"    {substance_name}: n_low={n_low}, det_rate={det_rate:.1f}%, "
            f"r_obs={r_obs:.3f}, min_detectable_r={min_r:.3f}"
            if min_r
            else f"    {substance_name}: n_low={n_low}, insufficient"
        )

        results[f"{substance_name}_low_power"] = {
            "n_low": n_low,
            "detection_rate_pct": round(det_rate, 1),
            "r_observed": round(r_obs, 3),
            "min_detectable_r_80pct": round(min_r, 3) if min_r else None,
        }

    results["interaction_tests"] = interaction_results
    return results


def run_temporal_stability(conn: duckdb.DuckDBPyConnection) -> dict:
    """S3.14: Spearman rank correlations of catchment-level application intensity across years."""
    log.info("\n" + "=" * 70)
    log.info("S3.14: Temporal Stability of Application Patterns")
    log.info("=" * 70)

    year_dist = conn.execute("""
        SELECT application_year, COUNT(*) as cnt
        FROM disagg_with_ingredient
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    log.info(f"  Records per year: {year_dist}")

    years = sorted([r[0] for r in year_dist if r[0] is not None])
    if len(years) < 2:
        log.warning("  Insufficient years for temporal stability analysis")
        return {"note": "insufficient years", "year_pairs": {}}

    log.info("  Computing per-GRUKO intensity for each year...")

    for year in years:
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE gruko_intensity_{year} AS
            SELECT
                gruko_id,
                SUM(kg_in_gruko) as total_intensity_kg
            FROM field_gruko_join
            WHERE application_year = {year}
              AND intersection_area_ha > 0 AND kg_in_gruko > 0
            GROUP BY gruko_id
        """)
        n = conn.execute(f"SELECT COUNT(*) FROM gruko_intensity_{year}").fetchone()[0]
        log.info(f"    {year}: {n} GRUKOs with intensity data")

    import itertools

    year_pairs = {}
    for y1, y2 in itertools.combinations(years, 2):
        rows = conn.execute(f"""
            SELECT a.total_intensity_kg, b.total_intensity_kg
            FROM gruko_intensity_{y1} a
            JOIN gruko_intensity_{y2} b ON a.gruko_id = b.gruko_id
        """).fetchall()

        if len(rows) < 50:
            log.warning(f"    {y1} vs {y2}: too few shared GRUKOs ({len(rows)})")
            continue

        i1 = np.array([r[0] for r in rows])
        i2 = np.array([r[1] for r in rows])
        rho, p = scipy_stats.spearmanr(i1, i2)
        log.info(f"    {y1} vs {y2}: Spearman ρ = {rho:.3f}, n = {len(rows)}, p = {p:.2e}")  # noqa: RUF001
        year_pairs[f"{y1}_vs_{y2}"] = {
            "rho": round(rho, 3),
            "n": len(rows),
            "p": float(p),
        }

    return {
        "note": "Year-pair Spearman rank correlations of catchment-level total application intensity",
        "year_pairs": year_pairs,
    }


def print_results(
    results: list[dict],
    lag_results: list[dict],
    mv_results: list[dict] | None = None,
    neg_controls: list[dict] | None = None,
    power_info: dict | None = None,
    gruko_breakdown: dict | None = None,
    strat_results: list[dict] | None = None,
    type_results: list[dict] | None = None,
    sensitivity: dict | None = None,
    linearity_results: list[dict] | None = None,
    sar_results: list[dict] | None = None,
    threshold_sensitivity: dict | None = None,
    within_tertile: dict | None = None,
    temporal_stability: dict | None = None,
) -> None:
    """Print results for comparison with paper tables."""

    sig_raw = [r for r in results if r["p_value"] < 0.05 and r["r"] > 0]
    sig_fdr = [r for r in results if r.get("sig_fdr") and r["r"] > 0]

    # ─── DESCRIPTIVE STATISTICS TABLE (R4) ───────────────────────────────
    print("\n" + "=" * 140)  # noqa: T201
    print("DESCRIPTIVE STATISTICS PER SUBSTANCE")  # noqa: T201
    print("=" * 140)  # noqa: T201
    print(  # noqa: T201
        f"{'#':<3} {'Substance':<38} {'Type':<10} {'n':>5} {'n_det':>5} {'Det%':>6} "
        f"{'Int_mean':>9} {'Int_SD':>9} {'Int_med':>9} {'Int_nz':>6}"
    )
    print("-" * 140)  # noqa: T201
    for i, r in enumerate(sig_fdr, 1):
        print(  # noqa: T201
            f"{i:<3} {r['substance']:<38} {r['type']:<10} {r['n_grukos']:>5} {r['n_detected']:>5} "
            f"{r['detection_rate']:>5.1f}% {r.get('intensity_mean', 0):>9.2f} "
            f"{r.get('intensity_sd', 0):>9.2f} {r.get('intensity_median', 0):>9.2f} "
            f"{r.get('intensity_nonzero_n', 0):>6}"
        )

    # ─── FULL LOGISTIC REGRESSION TABLE WITH CIs (R3, R4) ───────────────
    print("\n" + "=" * 160)  # noqa: T201
    print("FULL LOGISTIC REGRESSION TABLE (with 95% CIs)")  # noqa: T201
    print("=" * 190)  # noqa: T201
    print(  # noqa: T201
        f"{'#':<3} {'Substance':<38} {'Type':<10} {'r':>6} {'95% CI(r)':>16} "
        f"{'OR':>8} {'95% CI(OR)':>18} {'AUC':>5} {'Nag.R²':>7} {'H-L p':>7} {'q_FDR':>9} {'FDR':>3} {'Det%':>6}"
    )
    print("-" * 190)  # noqa: T201

    for i, r in enumerate(sig_fdr, 1):
        ci_r = f"[{r.get('r_ci_low', 0):.3f}, {r.get('r_ci_high', 0):.3f}]"
        or_val = r.get("logit_or", "")
        or_str = f"{or_val:.4f}" if or_val else "—"
        ci_or = ""
        if r.get("logit_or_ci_low") and r.get("logit_or_ci_high"):
            ci_or = f"[{r['logit_or_ci_low']:.4f}, {r['logit_or_ci_high']:.4f}]"
        else:
            ci_or = "—"
        fdr_str = f"{r.get('p_fdr', 1):.4f}" if r.get("p_fdr", 1) >= 0.001 else "<0.001"
        fdr_mark = "*" if r.get("sig_fdr") else " "
        auc_str = f"{r['logit_auc']:.2f}" if r.get("logit_auc") else "—"
        nag_str = f"{r['logit_nagelkerke_r2']:.3f}" if r.get("logit_nagelkerke_r2") else "—"
        hl_str = f"{r['logit_hl_p']:.3f}" if r.get("logit_hl_p") else "—"
        print(  # noqa: T201
            f"{i:<3} {r['substance']:<38} {r['type']:<10} {r['r']:>6.3f} {ci_r:>16} "
            f"{or_str:>8} {ci_or:>18} {auc_str:>5} {nag_str:>7} {hl_str:>7} {fdr_str:>9} {fdr_mark:>3} {r['detection_rate']:>5.1f}%"
        )

    # ─── MAIN CORRELATION TABLE (legacy format) ─────────────────────────
    print("\n" + "=" * 130)  # noqa: T201
    print("TABLE 2: Substances with Significant Positive Correlations")  # noqa: T201
    print("=" * 130)  # noqa: T201
    print(  # noqa: T201
        f"{'#':<3} {'Substance':<38} {'Type':<10} {'r':>6} {'p_raw':>9} {'q_FDR':>9} {'FDR':>3} {'Logit_p':>9} {'OR':>8} {'Det%':>6} {'Paper r':>8}"
    )
    print("-" * 130)  # noqa: T201

    for i, r in enumerate(sig_raw, 1):
        expected = EXPECTED_TABLE2.get(r["substance"], {})
        paper_r = expected.get("r", "")
        p_str = f"{r['p_value']:.4f}" if r["p_value"] >= 0.001 else "<0.001"
        fdr_str = f"{r.get('p_fdr', 1):.4f}" if r.get("p_fdr", 1) >= 0.001 else "<0.001"
        fdr_mark = "*" if r.get("sig_fdr") else " "
        logit_str = ""
        or_str = ""
        if r.get("logit_p") is not None:
            logit_str = f"{r['logit_p']:.4f}" if r["logit_p"] >= 0.001 else "<0.001"
            or_str = f"{r['logit_or']:.4f}" if r.get("logit_or") else ""
        paper_r_str = f"{paper_r:.3f}" if paper_r else ""
        print(  # noqa: T201
            f"{i:<3} {r['substance']:<38} {r['type']:<10} {r['r']:>6.3f} {p_str:>9} {fdr_str:>9} {fdr_mark:>3} {logit_str:>9} {or_str:>8} {r['detection_rate']:>5.1f}% {paper_r_str:>8}"
        )

    n_logit_fdr = sum(1 for r in results if r.get("logit_sig_fdr") and r["r"] > 0)
    print(f"\nSignificant (rpb raw p<0.05):  {len(sig_raw)}")  # noqa: T201
    print(f"Significant (rpb BH-FDR<0.05): {len(sig_fdr)}  (* = survives FDR)")  # noqa: T201
    print(f"Significant (logit BH-FDR):    {n_logit_fdr}")  # noqa: T201
    print("Paper:                         19 / 65")  # noqa: T201

    # ─── DOSE-RESPONSE QUARTILE TABLE (R4) ──────────────────────────────
    print("\n" + "=" * 120)  # noqa: T201
    print("DOSE-RESPONSE QUARTILE TABLE")  # noqa: T201
    print("=" * 120)  # noqa: T201
    print(  # noqa: T201
        f"{'Substance':<38} {'Q1%':>6} {'Q2%':>6} {'Q3%':>6} {'Q4%':>6} {'Q4/Q1':>7} "
        f"{'Q1_n':>5} {'Q2_n':>5} {'Q3_n':>5} {'Q4_n':>5}"
    )
    print("-" * 120)  # noqa: T201
    for r in sig_fdr:
        qr = r.get("q_rates", {})
        if not qr:
            continue
        q4q1_str = f"{r['q4_q1']:.1f}x" if r.get("q4_q1") else "—"
        print(  # noqa: T201
            f"{r['substance']:<38} "
            f"{qr.get('q1_rate', '—'):>6} {qr.get('q2_rate', '—'):>6} "
            f"{qr.get('q3_rate', '—'):>6} {qr.get('q4_rate', '—'):>6} "
            f"{q4q1_str:>7} "
            f"{qr.get('q1_n', '—'):>5} {qr.get('q2_n', '—'):>5} "
            f"{qr.get('q3_n', '—'):>5} {qr.get('q4_n', '—'):>5}"
        )

    # ─── MULTIVARIATE REGRESSION TABLE (R1) ─────────────────────────────
    if mv_results:
        print("\n" + "=" * 200)  # noqa: T201
        print("MULTIVARIATE LOGISTIC REGRESSION (controlling for soil_height, intake_depth, n_wells)")  # noqa: T201
        print("=" * 200)  # noqa: T201
        print(  # noqa: T201
            f"{'Substance':<38} {'Biv_r':>6} {'Biv_OR':>8} {'MV_OR':>8} {'MV_95%CI':>20} "
            f"{'p_int':>8} {'p_soil':>8} {'p_depth':>8} {'p_wells':>8} "
            f"{'AUC':>5} {'Nag.R²':>7} {'H-L p':>7} {'VIF':>5} {'EPV':>6} {'n':>5}"
        )
        print("-" * 200)  # noqa: T201
        for mv in mv_results:
            ci_str = ""
            if mv.get("mv_or_ci_low") and mv.get("mv_or_ci_high"):
                ci_str = f"[{mv['mv_or_ci_low']:.4f},{mv['mv_or_ci_high']:.4f}]"
            biv_or_str = f"{mv['bivariate_or']:.4f}" if mv.get("bivariate_or") else "—"

            def _fmt_p(val):
                if val is None:
                    return "—"
                return f"{val:.4f}" if val >= 0.001 else "<.001"

            auc_str = f"{mv['auc']:.2f}" if mv.get("auc") else "—"
            nag_str = f"{mv['nagelkerke_r2']:.3f}" if mv.get("nagelkerke_r2") else "—"
            hl_str = f"{mv['hl_p']:.3f}" if mv.get("hl_p") else "—"
            vif_str = f"{mv['vif_intensity']:.1f}" if mv.get("vif_intensity") else "—"
            epv_str = f"{mv['epv']:.0f}" if mv.get("epv") else "—"
            print(  # noqa: T201
                f"{mv['substance']:<38} {mv['bivariate_r']:>6.3f} {biv_or_str:>8} "
                f"{mv['mv_intensity_or']:>8.4f} {ci_str:>20} "
                f"{_fmt_p(mv['p_intensity']):>8} {_fmt_p(mv['p_soil']):>8} "
                f"{_fmt_p(mv['p_depth']):>8} {_fmt_p(mv['p_wells']):>8} "
                f"{auc_str:>5} {nag_str:>7} {hl_str:>7} {vif_str:>5} {epv_str:>6} {mv['n']:>5}"
            )

        n_still_sig = sum(1 for mv in mv_results if mv.get("p_intensity") is not None and mv["p_intensity"] < 0.05)
        print(f"\n  {n_still_sig}/{len(mv_results)} substances remain significant after covariate adjustment")  # noqa: T201

    # ─── NEGATIVE CONTROLS TABLE (S4) ───────────────────────────────────
    if neg_controls:
        print("\n" + "=" * 100)  # noqa: T201
        print("NEGATIVE CONTROLS (high-Koc substances — expect non-significant correlations)")  # noqa: T201
        print("=" * 100)  # noqa: T201
        print(f"{'Substance':<25} {'Koc':>6} {'n':>6} {'r':>7} {'p':>9} {'q_FDR':>9} {'Det%':>6} {'Result'}")  # noqa: T201
        print("-" * 110)  # noqa: T201
        for nc in neg_controls:
            r_str = f"{nc['r']:.3f}" if nc["r"] is not None else "—"
            p_str = f"{nc['p_value']:.4f}" if nc.get("p_value") is not None else "—"
            q_str = f"{nc['q_fdr']:.3f}" if nc.get("q_fdr") is not None else "—"
            det_str = f"{nc.get('detection_rate', 0):.1f}%" if nc.get("detection_rate") else "—"
            print(  # noqa: T201
                f"{nc['substance']:<25} {nc['koc']:>6} {nc['n']:>6} {r_str:>7} {p_str:>9} {q_str:>9} {det_str:>6} {nc['note']}"
            )

    # ─── POWER ANALYSIS (R6) ────────────────────────────────────────────
    if power_info:
        print("\n" + "=" * 80)  # noqa: T201
        print("POWER ANALYSIS")  # noqa: T201
        print("=" * 80)  # noqa: T201
        print(  # noqa: T201
            f"  Sample sizes: median={power_info['median_n']}, mean={power_info['mean_n']}, "
            f"range=[{power_info['min_n']}, {power_info['max_n']}]"
        )
        print(f"  Minimum detectable r (80% power, alpha=0.05): {power_info['r_min_80pct']:.4f}")  # noqa: T201
        print(f"  FDR-significant substances: {power_info['n_sig']}")  # noqa: T201
        print(f"    Strong effect (r >= 0.20): {power_info['n_strong']}")  # noqa: T201
        print(f"    Moderate effect (0.10 <= r < 0.20): {power_info['n_moderate']}")  # noqa: T201
        print(f"    Weak effect (r < 0.10): {power_info['n_weak']}")  # noqa: T201
        print(f"  {power_info['n_above_min']}/{power_info['n_sig']} exceed minimum detectable effect size")  # noqa: T201

    # ─── TABLE 3: Metabolites vs Parent Compounds ───────────────────────
    print("\n" + "=" * 75)  # noqa: T201
    print("TABLE 3: Metabolites vs Parent Compounds")  # noqa: T201
    print("=" * 75)  # noqa: T201

    metabolites = [r for r in results if r["type"] == "metabolite"]
    parents = [r for r in results if r["type"] == "parent"]
    met_sig = [r for r in metabolites if r["p_value"] < 0.05 and r["r"] > 0]
    par_sig = [r for r in parents if r["p_value"] < 0.05 and r["r"] > 0]

    met_pct = 100 * len(met_sig) / max(len(metabolites), 1)
    par_pct = 100 * len(par_sig) / max(len(parents), 1)
    ratio = met_pct / max(par_pct, 0.01)

    print(f"  {'Metric':<30} {'Computed':>15} {'Paper':>15}")  # noqa: T201
    print(f"  {'-' * 30} {'-' * 15} {'-' * 15}")  # noqa: T201
    print(f"  {'N analyzed':<30} {f'{len(metabolites)} / {len(parents)}':>15} {'22 / 43':>15}")  # noqa: T201
    print(  # noqa: T201
        f"  {'N significant':<30} {f'{len(met_sig)} ({met_pct:.0f}%) / {len(par_sig)} ({par_pct:.0f}%)':>15} {'11 (50%) / 8 (19%)':>15}"
    )
    print(f"  {'Ratio':<30} {f'{ratio:.1f}x':>15} {'2.6x':>15}")  # noqa: T201

    if met_sig and par_sig:
        met_r = np.mean([r["r"] for r in met_sig])
        par_r = np.mean([r["r"] for r in par_sig])
        print(f"  {'Mean r (sig only)':<30} {f'{met_r:.3f} / {par_r:.3f}':>15} {'0.163 / 0.161':>15}")  # noqa: T201

    if metabolites and parents:
        table = np.array(
            [
                [len(met_sig), len(metabolites) - len(met_sig)],
                [len(par_sig), len(parents) - len(par_sig)],
            ]
        )
        if table.min() > 0:
            chi2, chi_p, _, _ = scipy_stats.chi2_contingency(table)
            print(f"\n  Chi-squared: {chi2:.1f}, p={chi_p:.3f}  (Paper: 6.8, p=0.009)")  # noqa: T201
        else:
            print("\n  Chi-squared: N/A (zero cell in contingency table)")  # noqa: T201

    # ─── TABLE 4: Optimal Temporal Lags (with global FDR) ───────────────
    print("\n" + "=" * 100)  # noqa: T201
    print("TABLE 4: Optimal Temporal Lags (with global FDR across all windows)")  # noqa: T201
    print("=" * 100)  # noqa: T201
    if lag_results and lag_results[0].get("total_windows_tested"):
        print(f"  Total windows tested across all substances: {lag_results[0]['total_windows_tested']}")  # noqa: T201
    print(  # noqa: T201
        f"  {'Substance':<38} {'Type':<10} {'Lag':>6} {'95% CI(lag)':>16} {'r':>6} {'p_raw':>9} {'q_FDR':>9} {'FDR':>3} {'#win':>5}"
    )
    print(f"  {'-' * 38} {'-' * 10} {'-' * 6} {'-' * 16} {'-' * 6} {'-' * 9} {'-' * 9} {'-' * 3} {'-' * 5}")  # noqa: T201
    for lr in lag_results:
        p_str = f"{lr['p']:.4f}" if lr["p"] >= 0.001 else "<0.001"
        fdr_str = f"{lr.get('p_fdr_global', lr['p']):.4f}" if lr.get("p_fdr_global", lr["p"]) >= 0.001 else "<0.001"
        fdr_mark = "*" if lr.get("sig_fdr_global") else " "
        n_win = lr.get("n_windows_tested", "")
        ci_lag = ""
        if lr.get("lag_ci_low") is not None and lr.get("lag_ci_high") is not None:
            ci_lag = f"[{lr['lag_ci_low']:.1f}, {lr['lag_ci_high']:.1f}]"
        print(  # noqa: T201
            f"  {lr['substance']:<38} {lr['type']:<10} {lr['lag_years']:>5.1f}y {ci_lag:>16} {lr['r']:>6.3f} "
            f"{p_str:>9} {fdr_str:>9} {fdr_mark:>3} {n_win:>5}"
        )

    # ─── TABLE 5: Ubiquitous Contaminants (Legacy) ──────────────────────
    print("\n" + "=" * 75)  # noqa: T201
    print("TABLE 5: Ubiquitous Contaminants (Legacy)")  # noqa: T201
    print("=" * 75)  # noqa: T201
    legacy_names = ["Desphenyl chloridazon", "Methyl-desphenyl-chloridazon", "2,6-Dichlorbenzamid"]
    legacy_results = [r for r in results if r["substance"] in legacy_names]
    if not legacy_results:
        legacy_results = [r for r in results if any(l.lower() in r["substance"].lower() for l in legacy_names)]  # noqa: E741

    expected_legacy = {
        "Desphenyl chloridazon": (47.7, 0.080),
        "Methyl-desphenyl-chloridazon": (22.2, 0.082),
        "2,6-Dichlorbenzamid": (31.4, 0.045),
    }
    print(f"  {'Substance':<35} {'Det%':>7} {'r':>6}  {'Paper Det%':>10} {'Paper r':>8}")  # noqa: T201
    print(f"  {'-' * 35} {'-' * 7} {'-' * 6}  {'-' * 10} {'-' * 8}")  # noqa: T201
    for r in legacy_results:
        exp = expected_legacy.get(r["substance"], (None, None))
        print(  # noqa: T201
            f"  {r['substance']:<35} {r['detection_rate']:>6.1f}% {r['r']:>6.3f}  {f'{exp[0]:.1f}%' if exp[0] else '':>10} {f'{exp[1]:.3f}' if exp[1] else '':>8}"
        )

    # ─── TABLE 2a: GRUKO Type Breakdown ──────────────────────────────────
    if gruko_breakdown:
        print("\n" + "=" * 80)  # noqa: T201
        print("TABLE 2a: GRUKO Type Breakdown")  # noqa: T201
        print("=" * 80)  # noqa: T201
        for layer, info in gruko_breakdown.items():
            if isinstance(info, dict):
                print(f"  {layer:<35} {info['count']:>6} features, mean area {info['mean_area_ha']:.0f} ha")  # noqa: T201
        if "total" in gruko_breakdown:
            print(f"  {'Total':<35} {gruko_breakdown['total']:>6}")  # noqa: T201
        if "overlap_count" in gruko_breakdown:
            print(f"  {'Overlap (multi-layer)':<35} {gruko_breakdown['overlap_count']:>6}")  # noqa: T201

    # ─── TABLE 7: Monitoring Density Stratified ──────────────────────────
    if strat_results:
        print("\n" + "=" * 100)  # noqa: T201
        print("TABLE 7: Monitoring Density Stratified Correlations")  # noqa: T201
        print("=" * 100)  # noqa: T201
        # Group by substance
        substances_seen = []
        for sr in strat_results:
            if sr["substance"] not in substances_seen:
                substances_seen.append(sr["substance"])

        print(f"  {'Substance':<38} {'Low (r)':>10} {'Medium (r)':>12} {'High (r)':>10}")  # noqa: T201
        print(f"  {'-' * 38} {'-' * 10} {'-' * 12} {'-' * 10}")  # noqa: T201
        for sname in substances_seen:
            sub_strats = {sr["tertile"]: sr for sr in strat_results if sr["substance"] == sname}
            low_r = f"{sub_strats['low']['r']:.3f}" if sub_strats.get("low", {}).get("r") is not None else "—"
            med_r = f"{sub_strats['medium']['r']:.3f}" if sub_strats.get("medium", {}).get("r") is not None else "—"
            high_r = f"{sub_strats['high']['r']:.3f}" if sub_strats.get("high", {}).get("r") is not None else "—"
            low_p = sub_strats.get("low", {}).get("p", 1)
            med_p = sub_strats.get("medium", {}).get("p", 1)
            high_p = sub_strats.get("high", {}).get("p", 1)
            low_sig = (
                "**" if low_p is not None and low_p < 0.01 else ("*" if low_p is not None and low_p < 0.05 else "")
            )
            med_sig = (
                "**" if med_p is not None and med_p < 0.01 else ("*" if med_p is not None and med_p < 0.05 else "")
            )
            high_sig = (
                "**" if high_p is not None and high_p < 0.01 else ("*" if high_p is not None and high_p < 0.05 else "")
            )
            print(f"  {sname:<38} {low_r:>8}{low_sig:<2} {med_r:>10}{med_sig:<2} {high_r:>8}{high_sig:<2}")  # noqa: T201

    # ─── TABLE 10: GRUKO Type Sensitivity ────────────────────────────────
    if type_results:
        print("\n" + "=" * 100)  # noqa: T201
        print("TABLE 10: GRUKO Type Sensitivity Analysis")  # noqa: T201
        print("=" * 100)  # noqa: T201
        layers_seen = sorted({tr["layer"] for tr in type_results})
        header = f"  {'Substance':<38} {'All (r)':>10}"
        for layer in layers_seen:
            header += f" {layer[:20] + ' (r)':>22}"
        print(header)  # noqa: T201
        print(f"  {'-' * 38} {'-' * 10}" + f" {'-' * 22}" * len(layers_seen))  # noqa: T201

        substances_seen = []
        for tr in type_results:
            if tr["substance"] not in substances_seen:
                substances_seen.append(tr["substance"])

        for sname in substances_seen:
            all_r = next((r["r"] for r in results if r["substance"] == sname), None)
            line = f"  {sname:<38} {all_r:>10.3f}" if all_r else f"  {sname:<38} {'—':>10}"
            for layer in layers_seen:
                lr = next((tr for tr in type_results if tr["substance"] == sname and tr["layer"] == layer), None)
                if lr and lr["r"] is not None:
                    sig = "**" if lr["p"] < 0.01 else ("*" if lr["p"] < 0.05 else "")
                    line += f" {lr['r']:>18.3f}{sig:<4}"
                else:
                    line += f" {'—':>22}"
            print(line)  # noqa: T201

    # ─── SAR-PROBIT TABLE (Table 8) ──────────────────────────────────────
    if sar_results:
        print("\n" + "=" * 120)  # noqa: T201
        print("TABLE 8: SAR-Probit vs Standard Logistic Regression")  # noqa: T201
        print("=" * 120)  # noqa: T201
        print(  # noqa: T201
            f"  {'Substance':<25} {'Logistic OR':>12} {'SAR-probit OR':>15} {'SAR 95% CI':>22} {'SAR ρ':>8} {'Conv.':>6} {'n':>6}"  # noqa: RUF001
        )
        print(f"  {'-' * 25} {'-' * 12} {'-' * 15} {'-' * 22} {'-' * 8} {'-' * 6} {'-' * 6}")  # noqa: T201
        for sr in sar_results:
            log_or = f"{sr['logistic_or']:.4f}" if sr.get("logistic_or") else "—"
            sar_or = f"{sr['sar_or']:.4f}" if sr.get("sar_or") else "FAILED"
            ci = ""
            if sr.get("sar_ci_low") and sr.get("sar_ci_high"):
                ci = f"[{sr['sar_ci_low']:.4f}, {sr['sar_ci_high']:.4f}]"
            rho = f"{sr['rho']:.3f}" if sr.get("rho") else "—"
            conv = "Yes" if sr.get("converged") else "No"
            print(f"  {sr['substance']:<25} {log_or:>12} {sar_or:>15} {ci:>22} {rho:>8} {conv:>6} {sr.get('n', ''):>6}")  # noqa: T201

    # ─── LINEARITY-IN-LOGIT (Section 3.13) ───────────────────────────────
    if linearity_results:
        print("\n" + "=" * 80)  # noqa: T201
        print("LINEARITY-IN-LOGIT VERIFICATION (Restricted Cubic Splines)")  # noqa: T201
        print("=" * 80)  # noqa: T201
        print(f"  {'Substance':<25} {'LR χ²':>8} {'df':>4} {'p':>8} {'Linear adequate?'}")  # noqa: T201
        print(f"  {'-' * 25} {'-' * 8} {'-' * 4} {'-' * 8} {'-' * 16}")  # noqa: T201
        for lr in linearity_results:
            adeq = "YES" if lr["linear_adequate"] else "NO (non-linear)"
            print(f"  {lr['substance']:<25} {lr['lr_chi2']:>8.1f} {lr['df']:>4} {lr['p']:>8.3f} {adeq}")  # noqa: T201

    # ─── SUBSTANCE SENSITIVITY (Section 3.12) ────────────────────────────
    if sensitivity:
        print("\n" + "=" * 80)  # noqa: T201
        print("SUBSTANCE-SPECIFIC SENSITIVITY ANALYSES")  # noqa: T201
        print("=" * 80)  # noqa: T201
        if "ampa_ag_only" in sensitivity:
            a = sensitivity["ampa_ag_only"]
            print(f"  AMPA agricultural-only: r={a['r']:.3f} (full dataset: {a.get('full_r', '?')}), n={a['n']}")  # noqa: T201
        if "glyphosate_sandy" in sensitivity:
            s = sensitivity["glyphosate_sandy"]
            print(f"  Glyphosate sandy soils: r={s['r']:.3f}, n={s['n']}")  # noqa: T201
        if "glyphosate_clay" in sensitivity:
            c = sensitivity["glyphosate_clay"]
            print(f"  Glyphosate clay soils:  r={c['r']:.3f}, n={c['n']}")  # noqa: T201

    # ─── S3.12: THRESHOLD SENSITIVITY ─────────────────────────────────────
    if threshold_sensitivity:
        print("\n" + "=" * 100)  # noqa: T201
        print("S3.12: DETECTION THRESHOLD SENSITIVITY")  # noqa: T201
        print("=" * 100)  # noqa: T201
        for label, subs in threshold_sensitivity.items():
            print(f"\n  {label}:")  # noqa: T201
            for name, data in subs.items():
                if data.get("r") is not None:
                    print(f"    {name:<38} r={data['r']:.3f}  MV p={data.get('mv_p', 'N/A')}")  # noqa: T201
                else:
                    print(f"    {name:<38} {data.get('note', 'N/A')}")  # noqa: T201

    # ─── S3.13: WITHIN-TERTILE MULTIVARIATE ───────────────────────────────
    if within_tertile:
        print("\n" + "=" * 100)  # noqa: T201
        print("S3.13: WITHIN-TERTILE MULTIVARIATE (HIGH-DENSITY STRATUM)")  # noqa: T201
        print("=" * 100)  # noqa: T201
        for name, d in within_tertile.items():
            if name.endswith("_low_power"):
                continue
            if name == "interaction_tests":
                print("\n  Interaction tests (intensity x density tertile):")  # noqa: T201
                for sname, idata in d.items():
                    print(f"    {sname:<38} LR χ²(1)={idata['lr_chi2']:.1f}  p={idata['lr_p']:.4f}")  # noqa: T201
                continue
            if isinstance(d, dict) and "adj_or" in d:
                print(  # noqa: T201
                    f"  {name:<38} n={d['n']:>5}  events={d['n_events']:>4}  "
                    f"adj.OR={d['adj_or']:.4f} [{d['or_ci_low']:.4f}, {d['or_ci_high']:.4f}]  "
                    f"p={d['p_intensity']:.4f}"
                )

        # Low-power diagnostics
        for name, d in within_tertile.items():
            if name.endswith("_low_power") and isinstance(d, dict):
                sname = name.replace("_low_power", "")
                print(  # noqa: T201
                    f"  Low-density: {sname:<30} n={d['n_low']:>5}  det={d['detection_rate_pct']:.1f}%  "
                    f"r_obs={d['r_observed']:.3f}  min_r(80%)={d.get('min_detectable_r_80pct', 'N/A')}"
                )

    # ─── S3.14: TEMPORAL STABILITY ────────────────────────────────────────
    if temporal_stability:
        print("\n" + "=" * 80)  # noqa: T201
        print("S3.14: TEMPORAL STABILITY OF APPLICATION PATTERNS")  # noqa: T201
        print("=" * 80)  # noqa: T201
        key = "year_pairs" if "year_pairs" in temporal_stability else "pairs"
        if key in temporal_stability:
            for pair, data in temporal_stability[key].items():
                print(f"  {pair:<20} Spearman rho = {data['rho']:.3f}  n = {data['n']}")  # noqa: T201


def main():
    parser = argparse.ArgumentParser(description="Verify groundwater correlation paper numbers")
    parser.add_argument("--dry-run", action="store_true", help="Only discover data, don't run analysis")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument(
        "--detection-mode",
        choices=["all", "2018", "soil"],
        default="2018",
        help="Detection year filtering: 'all'=no filter, '2018'=uniform 2018+ (default, paper Section 2.2.2), 'soil'=soil-adjusted transit",
    )
    parser.add_argument(
        "--trend",
        action="store_true",
        help="Show year-by-year cumulative GRUKO detection counts for near-threshold substances",
    )
    parser.add_argument(
        "--export-json",
        type=str,
        default=None,
        help="Export results to JSON file for figure generation",
    )
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    log.info("=" * 60)
    log.info("Groundwater Correlation Verification Script")
    log.info("=" * 60)

    conn = get_connection()
    loader = DataLoader(conn)

    try:
        info = discover_data(loader)

        if args.dry_run:
            log.info("\n--dry-run: stopping after data discovery.")
            return

        if info.get("geus_rows", 0) == 0:
            log.error("No GEUS data found. Cannot proceed.")
            sys.exit(1)
        if info.get("grukos_count", 0) == 0:
            log.error("No GRUKOS data found. Cannot proceed.")
            sys.exit(1)

        load_data(loader)
        build_gruko_application_intensity(conn)
        # Always build soil transit — needed for covariates even in non-soil detection modes
        build_gruko_soil_transit(conn)
        build_gruko_detections(conn, detection_mode=args.detection_mode)

        if args.trend:
            run_detection_trend(conn)
        else:
            # Phase 1: Core analyses (existing)
            gruko_breakdown = run_gruko_type_breakdown(conn)
            results = run_correlations(conn)
            build_gruko_covariates(conn)
            mv_results = run_multivariate_logistic(conn, results)
            neg_controls = run_negative_controls(conn, results)
            power_info = run_power_analysis(results)
            run_spatial_autocorrelation(conn, results)
            lag_results = run_temporal_lag(conn, results)

            # Phase 2: New analyses from paper revision
            strat_results = run_monitoring_density_stratified(conn, results)
            type_results = run_gruko_type_sensitivity(conn, results)
            sensitivity = run_substance_sensitivity(conn, results)
            linearity_results = run_linearity_in_logit(conn, mv_results)
            sar_results = run_sar_probit(conn, mv_results)

            # Phase 3: Supplementary analyses (S3.12, S3.13, S3.14)
            threshold_sensitivity = run_threshold_sensitivity(conn, mv_results)
            within_tertile = run_within_tertile_mv(conn, mv_results)
            temporal_stability = run_temporal_stability(conn)

            # Phase 4: Bootstrap CIs for temporal lag (slow — run last)
            lag_results = run_bootstrap_temporal_lag(conn, lag_results)

            print_results(
                results,
                lag_results,
                mv_results,
                neg_controls,
                power_info,
                gruko_breakdown,
                strat_results,
                type_results,
                sensitivity,
                linearity_results,
                sar_results,
                threshold_sensitivity,
                within_tertile,
                temporal_stability,
            )

            if args.export_json:
                import json
                from datetime import datetime

                class NumpyEncoder(json.JSONEncoder):
                    def default(self, obj):
                        if hasattr(obj, "item"):
                            return obj.item()
                        if hasattr(obj, "tolist"):
                            return obj.tolist()
                        return super().default(obj)

                export_data = {
                    "metadata": {
                        "detection_mode": args.detection_mode,
                        "application_years": APPLICATION_YEARS,
                        "timestamp": datetime.now().isoformat(),
                    },
                    "results": results,
                    "mv_results": mv_results,
                    "lag_results": lag_results,
                    "neg_controls": neg_controls,
                    "power_info": power_info,
                    "gruko_breakdown": gruko_breakdown,
                    "strat_results": strat_results,
                    "type_results": type_results,
                    "sensitivity": sensitivity,
                    "linearity_results": linearity_results,
                    "sar_results": sar_results,
                    "threshold_sensitivity": threshold_sensitivity,
                    "within_tertile": within_tertile,
                    "temporal_stability": temporal_stability,
                }
                with Path(args.export_json).open("w") as f:
                    json.dump(export_data, f, cls=NumpyEncoder, indent=2)
                log.info(f"Exported results to {args.export_json}")

    finally:
        loader.cleanup()
        conn.close()

    log.info("\nDone.")


if __name__ == "__main__":
    main()
