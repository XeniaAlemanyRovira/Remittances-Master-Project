# Sender-Side Calibrated Extension

This folder extends the geospatial project from receiving municipalities to sending communities.

## Data source

The sender-side panel comes from:

- `../1_cleaned_panels/panel_sending_states_pga_calibrated.csv`

That panel is built from the cleaned remittances matrix originally sourced from:

- Branch: `antxi/clean-data-FINAL-VER`
- Path: `1_network_estimation/4_remittance_calibration/output/calibrated_remittance_flows_master_2013q1_2024q4.csv`

## Unit of analysis

Unlike the original receiver-side project, the sender-side unit is:

- `US state x quarter`

Treatment is defined using the remittance-weighted mean earthquake exposure of the Mexican municipalities that each US state sends to.

## Econometric pipeline

This extension mirrors the receiver-side workflow as closely as the data allow:

1. Callaway and Sant'Anna staggered DiD by sender exposure partition
2. Cohort-level ATT table
3. Robustness check using the low-dose sender group as controls
4. Event-study outputs
5. Spillover regressions using:
   - geographic proximity across sender states
   - portfolio-overlap network exposure across sender states
   - joint geographic + network exposure

## Important sender-side constraint

Under the remittance-weighted sender exposure measure, no sender states ever reach a `20+` average PGA bin. As a result, the sender-side analysis only identifies:

- `4-10`
- `10-20`

Also, all treated sender states first enter treatment in `2017Q3`, so the sender-side design has only one treatment cohort.

## Main result

The sender-side extension does not reproduce the positive receiver-side pattern. Instead, the estimated sender-side effects are negative:

- `4-10`: negative ATT
- `10-20`: more negative ATT

The low-dose-controls robustness check keeps the negative sign for the `10-20` sender group.

The spillover tests do not show strong evidence of sender-side spillovers through either geographic proximity or sender-network overlap.

## Contents

- `run_sender_side_calibrated_analysis.R`
- `output/`
