# Sender Corridor-Level Calibrated Rerun

This folder re-runs the sender-side analysis using the more interpretable unit:

- `US state x Mexican municipality x quarter`

Instead of asking whether total remittances from a whole US state rise after its recipient portfolio becomes exposed, this rerun asks whether remittances sent along a specific sender-to-recipient corridor rise when the recipient municipality is hit by an earthquake.

## Data source

The corridor panel is built from the cleaned remittance matrix originally sourced from:

- Branch: `antxi/clean-data-FINAL-VER`
- Path: `1_network_estimation/4_remittance_calibration/output/calibrated_remittance_flows_master_2013q1_2024q4.csv`

Recipient municipality earthquake exposure is merged from the calibrated receiver-side panel in:

- `../../1_cleaned_panels/panel_remittances_pga_calibrated.csv`

## Size note

The built corridor-level panel `panel_sender_corridor_pga_calibrated.csv` is very large, about `1.29 GB`, so it is intended to be generated locally from the build script rather than versioned directly on GitHub.

## Econometric methods

This rerun mirrors the receiver-side pipeline as closely as possible:

1. Callaway and Sant'Anna staggered DiD by recipient PGA partition
2. Cohort-level ATT table
3. Robustness check using the low-dose group as controls
4. Event-study outputs
5. Spillover regressions using recipient-municipality:
   - geographic proximity
   - migration-network exposure
   - joint geographic + network exposure

## Main result

This corridor-level sender specification aligns much more closely with the receiver-side intuition than the aggregated sender-state specification did.

Baseline ATT estimates are:

- `4-10`: essentially zero
- `10-20`: positive and statistically significant
- `20+`: positive and statistically significant

So once we look directly at remittances sent from each US state to the earthquake-affected municipalities, the response becomes weakly positive rather than negative.

The low-dose-controls robustness check weakens the precision, but the signs remain positive for the `10-20` and `20+` bins.

## Contents

- `build_sender_corridor_panel_pga.R`
- `run_sender_corridor_calibrated_analysis.R`
- `output/`
