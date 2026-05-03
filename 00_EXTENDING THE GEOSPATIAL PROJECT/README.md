# Extending the Geospatial Project

This branch is the workspace for extending the geospatial project in two directions.

## 1. Re-run the geospatial project with the cleaned remittances matrix

We re-run the receiver-side geospatial analysis using the cleaned remittances matrix from:

- Branch: `antxi/clean-data-FINAL-VER`
- Path: `1_network_estimation/4_remittance_calibration/output/calibrated_remittance_flows_master_2013q1_2024q4.csv`

The cleaned receiver-side panel and its build script are stored in:

- `1_cleaned_panels/panel_remittances_pga_calibrated.csv`
- `1_cleaned_panels/panel_remittances_pga_calibrated_match_report.csv`
- `1_cleaned_panels/build_calibrated_panel_remittances_pga.R`

## 2. Extend the project to examine sending communities

We also extend the project to study sender-side responses using the same cleaned remittance matrix.

Given the available data, the sender-side extension is currently built at the `US state x quarter` level rather than the sender-municipality level.

The sender-side panel and its build script are stored in:

- `1_cleaned_panels/panel_sending_states_pga_calibrated.csv`
- `1_cleaned_panels/panel_sending_states_pga_calibrated_match_report.csv`
- `1_cleaned_panels/build_sender_state_panel_remittances_pga.R`
