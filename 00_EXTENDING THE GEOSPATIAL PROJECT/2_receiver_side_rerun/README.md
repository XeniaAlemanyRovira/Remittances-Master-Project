# Receiver-Side Calibrated Rerun

This folder re-runs the receiver-side geospatial analysis using the cleaned remittance matrix rather than the original Banxico-based municipality panel.

## Data source

The calibrated receiver-side panel comes from:

- `../1_cleaned_panels/panel_remittances_pga_calibrated.csv`

This panel is built from the cleaned remittance matrix originally sourced from:

- Branch: `antxi/clean-data-FINAL-VER`
- Path: `1_network_estimation/4_remittance_calibration/output/calibrated_remittance_flows_master_2013q1_2024q4.csv`

## Econometric methods

The rerun keeps the same receiver-side econometric logic used by the original geospatial project:

1. Callaway and Sant'Anna staggered DiD by PGA partition (`4-10`, `10-20`, `20+`)
2. Robustness check using the low-dose treated group as controls
3. Spillover regressions using:
   - inverse-distance geographic exposure
   - migration-network exposure
   - joint geographic + network exposure

## Main result

The cleaned-matrix rerun preserves the original intuition almost exactly:

- `4-10`: positive ATT (`0.507`)
- `10-20`: positive ATT (`0.494`)
- `20+`: negative ATT (`-0.514`)

The spillover pattern is also very similar:

- geographic spillovers are positive in the univariate specification
- network spillovers are positive in the univariate specification
- in the joint specification, the geographic term collapses toward zero while the network term remains the stronger channel

## Contents

- `run_receiver_side_calibrated_analysis.R`
- `input/`
- `output/`
