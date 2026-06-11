# ATC-CNK-mapping-medications-belgium

[Intego](https://www.intego.be/) project: cleaning of medications data. This repo
extracts an ATC–CNK mapping table for Belgian medications from publicly maintained
sources. It is an **intermediate step** in the wider Intego data-cleaning and
standardization workflow.

The pipeline has two stages: a Jupyter notebook (Python) parses the raw SAM XML and
writes intermediate `.parquet` files, then an R script merges them with the WHO
ATC-alteration list into the final CNK→ATC mapping table.

## Inputs

Two data sources are needed. Neither the large SAM files nor any generated output is
tracked in git (see `.gitignore`); only the code and the small alterations file are.

1. **SAM export (large, downloaded manually).** Download the most recent export from
   the eHealth SAM2 database:
   <https://www.vas.ehealth.fgov.be/websamcivics/samcivics/>
   This produces a folder named `sam-XXXX` containing several XML files; place it in
   `data/sam-XXXX/`. Only the `AMP-*.xml` file is used.
   ⚠️ This file is ~1 GB. Parsing it needs several GB of free RAM and takes a few
   minutes — this is expected, not a hang.

2. **ATC alterations (small, committed to the repo).** `atc_alterations.xlsx` is the
   cumulative ATC-code alteration list maintained by the WHO Collaborating Centre for
   Drug Statistics Methodology:
   <https://atcddd.fhi.no/atc_ddd_alterations__cumulative/atc_alterations/>
   It is committed so the pipeline runs out of the box. Refresh it manually from that
   page when a new version is published (roughly yearly).

## How to run

1. Download a SAM export into `data/sam-XXXX/` (see Inputs).
2. In `atc_cnk_map.ipynb`, set `SAM_DIR` to the new folder name, then run section 3.4
   ("Final extraction"). Sections 3.1–3.3 are exploratory and not part of the pipeline.
   This writes `amp_data.parquet`, `ampp_data.parquet`, and `ampp_dmpp.parquet`.
3. Run `get_mapping.R`. This merges the parquet files, applies the ATC alterations,
   and writes the final `cnk_to_atc_mapping.csv`.

## Notes

- One-to-many ATC alterations (codes that were split into several new codes) are
  deliberately excluded from the alteration step; only unambiguous one-to-one
  replacements are applied.
- Requires Python (pandas, pyarrow) and R (arrow, dplyr, readxl, stringr).

## Data source & citation

This work uses data from the Intego primary care database (Academic Centre for
General Practice, Department of Public Health and Primary Care, KU Leuven). The Intego-II data
resource profile:

> Zayed AM, Janssens A, Caspers M, Mamouris P, Beerten SG, De Burghgraeve T,
> Libin PJK, Neyens T, Delvaux N, Van Pottelbergh G, Aertgeerts B, Vaes B.
> Data resource profile: the Intego-II primary care database.
> *International Journal of Epidemiology*. 2025;54(6):dyaf200.
> doi:[10.1093/ije/dyaf200](https://doi.org/10.1093/ije/dyaf200)

Foundational reference:

> Truyers C, Goderis G, Dewitte H, Akker M van den, Buntinx F.
> The Intego database: background, methods and basic results of a Flemish general
> practice-based continuous morbidity registration project.
> *BMC Medical Informatics and Decision Making*. 2014;14:48.
> doi:[10.1186/1472-6947-14-48](https://doi.org/10.1186/1472-6947-14-48)

## Related: vaccine data (Vaccinnet)

This repo maps medications from the SAM database, which **already includes vaccines**
(ATC group J07 — currently 216 J07 CNK codes in the mapping). The Vaccinnet source
below is a separate, complementary vaccine reference used elsewhere in the Intego
cleaning; it is **not** produced by the code in this repository.

Vaccine codes are downloaded from the Flemish Agency for Care and Health (Zorg en
Gezondheid), under "Codes vaccins":
<https://www.zorg-en-gezondheid.be/per-domein/infectieziekten-en-vaccinaties/vaccinaties/vaccinnet/vaccinnet-integreren-in-uw-softwarepakket>

The relevant file is the CNK–ATC vaccine code table
(`vaccincodes_Vaccinnet_CNK_ATC_<date>.xlsx`).

⚠️ Vaccinnet is migrating to "Vaccinnet 2.0" (Vitalink as authoritative source, FHIR
standard) during 2025–2026. Existing KMEHR-based connections and the current download
page remain available until the full rollout, but the location and format of this
file are expected to change. See also the Vitalink technical documentation:
<https://www.vitalink.be/gebruikers/ik-ben-softwareleverancier/technische-documentatie>