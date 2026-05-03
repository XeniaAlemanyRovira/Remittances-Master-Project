# Sender Corridor Output Files Guide

This note explains what each main `.csv` file in this folder contains and how to interpret it.

This rerun uses the corridor:

- `US state x Mexican municipality x quarter`

as the unit of analysis.

That makes it a more direct sender-side analog to the receiver-side project, because treatment is still defined by the earthquake exposure of the Mexican receiving municipality, while the outcome is the remittance flow sent along that specific corridor.

## 1. `panel_sender_corridor_pga_calibrated.csv`

### What it contains

This is the main merged corridor-level analysis panel.

Each row is one:

- US sender state
- Mexican recipient municipality
- quarter

Columns include:

- sender and recipient identifiers
- remittance flow
- corridor-level remittance diagnostics
- recipient municipality earthquake exposure

### Main interpretation

This is the core dataset used for the sender corridor rerun. It is the best file to use if you want to inspect raw corridor dynamics directly.

## 2. `panel_sender_corridor_pga_calibrated_match_report.csv`

### What it contains

This file reports matching quality for each corridor.

Columns:

- `us_state`
- `state`
- `municipality`
- `matched_quarters`
- `total_quarters`
- `fully_matched`
- `total_remittances_musd`

### Main interpretation

This is a data-quality file. It shows which corridors matched cleanly to the recipient municipality earthquake panel and which ones did not.

## 3. `dropped_unmatched_sender_corridors.csv`

### What it contains

This file lists the sender-recipient corridors that were dropped before estimation because recipient earthquake exposure could not be matched.

Columns:

- `us_state`
- `state`
- `municipality`

### Main interpretation

This is the sender-corridor analogue of the dropped municipality list on the receiver side. It documents which corridors were excluded due to matching issues rather than due to econometric filtering.

## 4. `sender_corridor_treated_overview.csv`

### What it contains

This file lists all treated sender corridors together with their treatment metadata.

Columns include:

- `corridor_id`
- `us_state`
- `state`
- `municipality`
- `recipient_id`
- `dose`
- `G`
- `event`
- `partition`

### Main interpretation

This is the easiest file for understanding the treated sample. It tells you which specific sender-to-recipient links are considered treated, how strong the recipient shock is, and which PGA partition they belong to.

## 5. `sender_corridor_att_table_calibrated.csv`

### What it contains

This is the main baseline ATT table using Callaway-Sant'Anna staggered DiD with never-treated corridors as controls.

Each row is a recipient PGA partition:

- `4-10`
- `10-20`
- `20+`

Columns:

- `partition`
- `n_cohorts`
- `n_treated`
- `n_controls`
- `att`
- `se`
- `ci_lo`, `ci_hi`
- `p_val`
- `sig`

### Main interpretation

This is the main sender-corridor result.

It shows:

- `4-10`: essentially zero effect
- `10-20`: positive and statistically significant
- `20+`: positive and statistically significant

So once the sender-side analysis is defined at the corridor level rather than the whole-state level, the results move much closer to the receiver-side intuition.

## 6. `sender_corridor_att_table_low_dose_controls_calibrated.csv`

### What it contains

This is the low-dose-controls robustness version of the ATT table.

The `4-10` group is used as the comparison group for the higher-dose bins.

### Main interpretation

This file tests whether the positive sender-corridor pattern survives when the controls are restricted to mildly exposed corridors.

The signs remain positive for `10-20` and `20+`, but statistical precision weakens.

So the direction of the effect is stable, even if the robustness specification is less precise.

## 7. `sender_corridor_att_by_cohort_calibrated.csv`

### What it contains

This file breaks the baseline ATT down by treatment cohort.

Each row is one treatment cohort within one PGA partition.

Columns:

- `partition`
- `cohort_events`
- `year`, `quarter`
- `cohort_time_index`
- `cohort_treated_corridors`
- `att`
- `se`
- `ci_lo`, `ci_hi`
- `p_val`
- `sig`

### Main interpretation

This file shows whether the average ATT is concentrated in one earthquake cohort or appears across multiple cohorts.

It is useful for diagnosing heterogeneity across events and for checking whether the positive corridor-level pattern is broadly distributed across cohorts.

## 8. `sender_corridor_att_by_cohort_low_dose_controls_calibrated.csv`

### What it contains

This is the cohort-level version of the low-dose-controls robustness check.

The columns are the same as in the baseline cohort table.

### Main interpretation

This file is mainly useful for checking which cohorts drive the robustness results once low-dose corridors are used as controls.

## 9. `sender_corridor_event_study_baseline_calibrated.csv`

### What it contains

This is the event-study output for the baseline corridor-level specification.

Each row gives the estimated ATT at a relative event time within a PGA partition.

Columns:

- `partition`
- `egt`
- `att`
- `se`
- `ci_lo`, `ci_hi`

### Main interpretation

This file shows:

- whether pre-trends are approximately flat
- when the corridor-level remittance response appears
- whether the positive effect builds over time

It is the dynamic version of the main ATT table.

## 10. `sender_corridor_event_study_low_dose_controls_calibrated.csv`

### What it contains

This is the event-study output for the low-dose-controls robustness specification.

### Main interpretation

This checks whether the dynamic response pattern still looks similar when mildly exposed corridors are used as controls.

## 11. `sender_corridor_spillover_results_calibrated.csv`

### What it contains

This file contains corridor-level spillover regression coefficients.

Rows correspond to:

- `geographic`
- `network`
- `joint`

The spillover exposures are calculated at the recipient municipality level and then merged onto untreated sender corridors.

Columns:

- `term`
- `estimate`
- `std.error`
- `statistic`
- `p.value`
- `conf.low`, `conf.high`
- `model`

### Main interpretation

These regressions test whether untreated sender corridors respond when their recipient municipalities are geographically close to, or migration-network connected with, treated municipalities.

In this rerun, the spillover coefficients are positive and statistically meaningful, including in the joint specification.

That suggests sender corridor responses may diffuse not only through the directly hit municipalities, but also through connected recipient municipalities.

## Bottom line

Taken together, these corridor-level files tell a clearer sender-side story than the aggregated sender-state version:

- the negative sender-state ATT was largely an aggregation artifact
- once we look at actual sender-to-recipient corridors, the effects become weakly positive
- the corridor-level sender results are much closer to the receiver-side intuition
- positive spillover channels also appear when treatment is defined at the recipient-municipality level
