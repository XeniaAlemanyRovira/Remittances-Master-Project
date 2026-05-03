# Sender-Side Output Files Guide

This note explains what each `.csv` file in this folder contains and how to interpret it.

All sender-side files are based on the cleaned remittances matrix from:

- Branch: `antxi/clean-data-FINAL-VER`
- Path: `1_network_estimation/4_remittance_calibration/output/calibrated_remittance_flows_master_2013q1_2024q4.csv`

## Important design note

The sender-side extension uses `US state x quarter` as the unit of analysis. Treatment is defined using the remittance-weighted mean PGA exposure of the Mexican municipalities that each US state sends remittances to.

Because this sender-side exposure is an average, no sender states reach a `20+` average PGA bin. So the sender-side output only identifies:

- `4-10`
- `10-20`

Also, all treated sender states first enter treatment in `2017Q3`, so there is only one sender-side treatment cohort.

## 1. `sender_treated_states_overview.csv`

### What it contains

This file lists the sender states that are ever treated under the sender-side exposure measure.

Columns:

- `sender_id`: US state
- `event`: dominant earthquake event in the first treated quarter
- `dose`: maximum remittance-weighted mean PGA exposure
- `G`: first treatment quarter index
- `partition`: sender exposure bin

### Main interpretation

This is the clearest overview of the sender treatment sample. It shows:

- which US states are ever exposed
- how strongly they are exposed
- which sender partition they belong to

It also makes clear that the sender-side treatment is concentrated in a small set of states and a single treatment cohort.

## 2. `sender_att_table_calibrated.csv`

### What it contains

This is the baseline sender-side ATT table using Callaway-Sant'Anna with never-treated sender states as controls.

Each row is a sender exposure partition.

Columns:

- `partition`: sender exposure bin
- `n_cohorts`: number of treatment cohorts
- `n_treated`: number of treated sender states
- `n_controls`: number of control sender states
- `att`: average treatment effect
- `se`: standard error
- `ci_lo`, `ci_hi`: 95% confidence interval
- `p_val`: p-value
- `sig`: significance stars

### Main interpretation

This is the main sender-side result. Unlike the receiver-side analysis, the sender-side effects are negative:

- the `4-10` sender group shows a small negative ATT
- the `10-20` sender group shows a somewhat larger negative ATT

So when sender communities are defined through remittance-weighted exposure to affected Mexican municipalities, the response is not an increase in remittances from those sender states. If anything, the average sender-side response is slightly negative.

## 3. `sender_att_table_low_dose_controls_calibrated.csv`

### What it contains

This is the robustness version of the sender-side ATT table. It uses the low-dose sender states (`4-10`) as the control group for the `10-20` partition.

Columns are the same as in the baseline ATT table.

### Main interpretation

This checks whether the sender-side negative result depends on comparing treated states to never-treated states only.

The `10-20` sender group remains negative and statistically significant, so the sign does not disappear when the comparison group is restricted to other sender states that were also exposed, but more mildly.

## 4. `sender_att_by_cohort_calibrated.csv`

### What it contains

This is the baseline ATT broken down by treatment cohort.

Each row is one cohort within one sender exposure partition.

Columns:

- `partition`: sender exposure bin
- `cohort_events`: event label or labels associated with the treatment quarter
- `year`, `quarter`: calendar timing of the cohort
- `cohort_time_index`: internal quarter index
- `cohort_treated_states`: number of treated sender states in that partition and cohort
- `att`: cohort-specific ATT
- `se`: standard error
- `ci_lo`, `ci_hi`: 95% confidence interval
- `p_val`: p-value
- `sig`: significance stars

### Main interpretation

Because all treated sender states first enter treatment in the same quarter, this file mainly confirms that the sender-side results come from one common `2017Q3` cohort rather than from multiple staggered cohorts.

It also shows that the `10-20` sender states are very few, so that part of the sender-side evidence should be interpreted cautiously.

## 5. `sender_att_by_cohort_low_dose_controls_calibrated.csv`

### What it contains

This is the cohort-specific version of the low-dose-controls robustness check.

It contains the same columns as the baseline cohort file, but only for the sender partitions estimated under the low-dose-controls design.

### Main interpretation

This confirms that the robustness exercise is driven by the same single sender-side treatment cohort and should therefore be read as a focused sensitivity check rather than as a multi-cohort robustness result.

## 6. `sender_event_study_baseline_calibrated.csv`

### What it contains

This is the event-study output for the baseline sender-side specification.

Each row gives the estimated ATT at a relative event time within a sender exposure partition.

Columns:

- `partition`: sender exposure bin
- `egt`: event time in quarters relative to first treatment
- `att`: estimated ATT at that event time
- `se`: standard error
- `ci_lo`, `ci_hi`: 95% confidence interval

Interpretation of `egt`:

- negative values: pre-treatment quarters
- `0`: first treated quarter
- positive values: post-treatment quarters

### Main interpretation

This file shows the timing of sender-side effects. The broad pattern is:

- no strong positive post-treatment buildup like the one seen on the receiver side
- more negative post-treatment movement, especially for the more exposed sender states

Because the sender-side sample is much smaller and has only one cohort, these dynamics are more fragile than in the receiver-side analysis.

## 7. `sender_event_study_low_dose_controls_calibrated.csv`

### What it contains

This is the event-study output for the sender-side low-dose-controls specification.

The columns are the same as in the baseline event-study file.

### Main interpretation

This checks whether the negative sender-side pattern survives when the control group is restricted to mildly exposed sender states plus never-treated states.

The main post-treatment sign remains negative, but the event-study should be interpreted carefully because the `10-20` sender group contains only 3 states.

## 8. `sender_spillover_results_calibrated.csv`

### What it contains

This file contains sender-side spillover regression coefficients.

Rows correspond to three specifications:

- `geographic`: inverse-distance exposure across US sender states
- `network`: overlap in remittance destination portfolios across sender states
- `joint`: both channels included together

Columns:

- `term`: estimated coefficient
- `estimate`: coefficient value
- `std.error`: standard error
- `statistic`: test statistic
- `p.value`: p-value
- `conf.low`, `conf.high`: 95% confidence interval
- `model`: model specification

### Main interpretation

These regressions test whether untreated sender states react because they are close to or network-connected with treated sender states.

The main result is that none of the sender-side spillover coefficients are statistically meaningful. So, unlike the receiver-side project, the sender-side extension does not show strong evidence of spillovers through either geography or sender-network overlap.

## Bottom line

Taken together, the sender-side files suggest a different pattern from the receiver-side analysis:

- sender treatment is much thinner than receiver treatment
- there is only one sender-side cohort
- no `20+` sender exposure bin exists under the remittance-weighted mean exposure measure
- the estimated sender-side effects are negative rather than positive
- sender-side spillovers are weak or absent

So the extension is informative, but it should be interpreted as a more data-constrained companion exercise rather than as a sender-side mirror image of the receiver-side design.
