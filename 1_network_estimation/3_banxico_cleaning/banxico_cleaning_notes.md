# Banxico Cleaning Step

## What I Did

This step cleans the two Banxico remittances workbooks used for the calibration exercise:

1. `Estado de origen de los ingresos por remesas provenientes de Estados Unidos.xlsx`
2. `Ingresos por remesas, distribucion por municipio.xlsx`

The cleaning script is:

- `1_network_estimation/3_banxico_cleaning/scripts/3_clean_banxico_remittances.R`

The script:

1. Reads both Banxico workbooks.
2. Keeps quarterly data from `2013Q1` to `2024Q4`.
3. Drops municipality series labeled `No identificado`.
4. Normalizes state and municipality names by removing accents, trimming spacing, and title-casing text.
5. Matches Banxico municipality names against the migration-weighting municipality universe in `1_network_estimation/2_migration_matrix_estimation/clean final data/clean_municipality_universe.csv`.
6. Applies a small set of safe aliases when the Banxico label is a shortened or alternate official name.
7. Writes six CSV files to `1_network_estimation/3_banxico_cleaning/output`.

## Main Assumptions

1. The weighting-matrix municipality universe is the reference universe. Any Banxico municipality that cannot be safely matched to that universe is left out of the cleaned municipality panel.
2. `No identificado` is dropped before matching because those series do not correspond to a municipality in the weighting matrices.
3. Matching is conservative. A municipality is only accepted when one of these conditions holds: exact same-state match after normalization, explicit safe alias within the same state, or a canonical same-state match that resolves to a unique municipality.
4. Ambiguous cross-state names are not forced. If a name like `Rosario` or `Villa Hidalgo` could refer to more than one state and the weighting universe does not pin it down safely, it remains unresolved.

## How Names Were Harmonized

The script uses three levels of harmonization:

1. Text normalization:
   remove accents, collapse repeated whitespace, and standardize to title case.
2. State standardization:
   examples include `Distrito Federal` -> `Ciudad De Mexico`, `Coahuila` -> `Coahuila De Zaragoza`, and `Veracruz` -> `Veracruz De Ignacio De La Llave`.
3. Municipality aliases:
   safe one-to-one aliases were added when Banxico uses a shortened or alternate label that clearly corresponds to one municipality already present in the weighting universe. Examples include `Silao` -> `Silao De La Victoria`, `Tlaquepaque` -> `San Pedro Tlaquepaque`, `Medellin` -> `Medellin De Bravo`, and `Batopilas` -> `Batopilas De Manuel Gomez Morin`.

## Remaining Unmatched Municipalities

Some Banxico municipalities still do not appear in the weighting-matrix universe, or remain ambiguous after normalization. These are reported in:

- `1_network_estimation/3_banxico_cleaning/output/banxico_municipality_unresolved_report.csv`

They are also present at the row level in:

- `1_network_estimation/3_banxico_cleaning/output/banxico_municipality_dropped_rows_2013q1_2024q4.csv`

These dropped rows represent the remittance mass that is outside the current weighting universe.

## Output CSV Files

The script writes six CSV files:

1. `output/banxico_origin_state_remittances_2013q1_2024q4.csv`
   Quarterly remittances by US origin state. One row per quarter-state pair.
2. `output/banxico_municipality_remittances_2013q1_2024q4.csv`
   Cleaned quarterly remittances by Mexican municipality after harmonization to the weighting universe. One row per quarter-state-municipality pair.
3. `output/banxico_municipality_mapping_report.csv`
   Municipality-level matching report. Shows whether each Banxico municipality-state pair was resolved, how it was resolved, and candidate states when unresolved.
4. `output/banxico_municipality_unresolved_report.csv`
   Subset of the mapping report containing only unresolved municipality-state pairs.
5. `output/banxico_municipality_dropped_rows_2013q1_2024q4.csv`
   Raw quarterly Banxico municipality rows that were excluded because their municipality-state pair was unresolved.
6. `output/banxico_municipality_cleaning_summary.csv`
   Compact summary of the cleaning step, including counts of resolved and unresolved pairs and the total remittance mass kept versus dropped.
