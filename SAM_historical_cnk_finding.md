# Finding: SAM current exports omit long-discontinued products

**Date:** 2026-06-12
**Context:** ATC–CNK mapping for Intego (medication cleaning → OMOP)
**Trigger:** A colleague found CNK codes present in Intego primary-care records but
missing from the generated mapping table (example: CNK `2650752`, Pantoprazol Apotex).

## Summary

A *current* SAM full export does not contain products that were discontinued well
before the export date. SAM is an authoritative **current-state** source with temporal
tracking of live and recently-valid products — not a complete historical archive of
every CNK code that has circulated. For a longitudinal database like Intego, which
holds prescriptions written under codes retired years ago, a current SAM export is
therefore structurally incomplete for historical mapping. This is a property of the
data source, not a bug in the extraction code.

## Evidence (CNK 2650752, Pantoprazol Apotex)

- **Absent from the AMP (Actual Medicines) file in both exports checked:** the 2024
  export (`sam-8305`) and the 2026 export (`sam-12065`). The product's *name* still
  appears in historical `Data` periods of a related AMP entry, but the current entry
  has been superseded (the product now resolves to Pantoprazol Krka, CNK `2568079`),
  and the old CNK is no longer carried in any `Dmpp`.
- **Present only in the RMB (Reimbursement) file**, as a `ReimbursementContext` with
  `code="2650752"`, validity `from=2017-07-01 to=2017-12-31`. This confirms the code
  is genuinely old (retired ~2017–2018). RMB carries the CNK but **no ATC** and **no
  successor link**, so it cannot serve as a CNK→ATC source.
- **Not recoverable by accumulating the exports we have:** the code is absent from the
  2024 export too, so a union of available exports does not close the gap for codes
  retired this long ago.

## What this is NOT

- Not an extraction bug. Once the correct AMP file was parsed (the 2026 export
  contains 19,869 `Amp` elements), extraction works as intended.
- Not solvable by choosing a different file in the same export: CNK and ATC are
  designed to meet in AMP; other domains carry one or the other (VMP has ATC but not
  package-level CNK; RMB has CNK but not ATC). A different file in the same current
  export shares the same temporal cutoff.

## Implications for the mapping pipeline

Current SAM alone cannot map the full history of Intego prescriptions. Options:

1. **Accumulate across SAM exports over time** — build the CNK→ATC table as a union of
   successive exports (keeping the most recent ATC per CNK), so codes captured in an
   earlier export persist after they drop out of later ones. This helps for *future*
   churn but does **not** recover codes already retired before our earliest export.
2. **Obtain an authoritative historical CNK→ATC source** — needed to cover codes
   retired before the exports we hold (e.g. `2650752`, retired ~2017).

## Open questions (for the team)

- **How large is the gap?** Run the colleague's full list of flagged CNKs against the
  current extract to get an absolute number/percentage missing. This determines how
  much the fix is worth.
- **What is the authoritative source for historical / superseded CNK codes?** The APB
  (issuer of CNK codes) and RIZIV-INAMI are the natural candidates. Does Intego already
  have a licensed data channel to either? Has this been solved before internally?
- **Are older SAM exports archived** (by eHealth, or internally) that would extend the
  accumulation approach further back?

## Status

- Extraction pipeline: working and verified against the 2026 export.
- This finding: documented here; emailed to Maarten (2026-06-12).
- Next: quantify gap size; identify authoritative historical CNK source.
