# Remittance Calibration Step

## What This Step Does

This module builds a quarterly panel of estimated remittance flows from each US state to each Mexican municipality for `2013Q1` to `2024Q4`.

It combines:

1. The cleaned Banxico origin-state remittance totals from `1_network_estimation/3_banxico_cleaning/output/banxico_origin_state_remittances_2013q1_2024q4.csv`
2. The cleaned Banxico municipality remittance totals from `1_network_estimation/3_banxico_cleaning/output/banxico_municipality_remittances_2013q1_2024q4.csv`
3. The yearly migration weighting matrices from `1_network_estimation/2_migration_matrix_estimation/migration_weighting_matrices_2`

The script is:

- `1_network_estimation/4_remittance_calibration/scripts/1_calibrate_remittance_flows.R`

## Calibration Logic

For each quarter:

1. The yearly weighting matrix for that quarter's year is loaded.
2. Each US-state column is normalized so it becomes a within-state municipality share.
3. These shares are multiplied by the Banxico origin-state totals to build a seed matrix.
4. The Banxico municipality totals are rescaled so they sum to the same quarterly total as the kept origin-state totals.
5. Iterative proportional fitting is run so the final matrix matches both:
   - the quarterly US-state origin totals
   - the rescaled quarterly Mexican municipality totals

## Important Assumptions

1. The weighting matrices define the geographic support of the allocation.
   Municipalities outside the cleaned Banxico matching universe are already excluded upstream.

2. The sender margin is restricted to the 50 US states plus the District of Columbia.
   `Puerto Rico` and `No Identificado` are dropped because they do not exist as columns in the weighting matrices.

3. Some Banxico sender labels are harmonized to the weighting-matrix state names.
   Examples include `Nueva York` -> `New York`, `Carolina Del Norte` -> `North Carolina`, and `Washington, D.c.` -> `District Of Columbia`.

4. The municipality margin is rescaled quarter by quarter before calibration.
   This is necessary because the origin-state and municipality Banxico totals do not match exactly in levels.

5. Missing weighting columns use the average weighting matrix as fallback.
   This occurs for:
   - `Florida` in `2013`
   - `Alaska` in `2020`
   - `Connecticut` in `2024`

6. A tiny positive regularization is added before IPFP.
   This avoids structural-zero infeasibility and has a negligible quantitative effect on the fitted totals.

## Output Files

The outputs are written to `1_network_estimation/4_remittance_calibration/output`.

1. `calibrated_remittance_flows_master_2013q1_2024q4.csv`
   Master long panel for all quarters from `2013Q1` to `2024Q4`.

2. `calibrated_remittance_flows_2013.csv` through `calibrated_remittance_flows_2024.csv`
   One yearly long panel per year.

3. `calibration_diagnostics_2013q1_2024q4.csv`
   Quarter-level diagnostics including:
   - kept sender total
   - dropped sender total
   - raw municipality total
   - municipality scaling factor
   - IPFP iteration count
   - convergence flag
   - fallback states used

4. `origin_state_mapping_summary.csv`
   Sender-side mapping summary showing how Banxico origin labels were kept or dropped.
