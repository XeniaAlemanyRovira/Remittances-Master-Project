# Receiver-Side Output Files Guide

This note explains what each `.csv` file in this folder contains and how to interpret it.

The files all come from the receiver-side rerun of the geospatial project using the cleaned remittances matrix from:

- Branch: `antxi/clean-data-FINAL-VER`
- Path: `1_network_estimation/4_remittance_calibration/output/calibrated_remittance_flows_master_2013q1_2024q4.csv`

## 1. `dropped_unmatched_receiver_municipalities.csv`

### What it contains

This file lists the Mexican municipalities that were dropped before estimation because the calibrated remittance panel could not be matched to the geospatial municipality panel.

Columns:

- `state`: Mexican state
- `municipality`: Mexican municipality name

### Main interpretation

This is a data-quality and transparency file, not a regression result. It shows which municipalities are excluded because of name or universe mismatches. The receiver-side estimates are based on the matched municipalities only.

## 2. `receiver_att_table_calibrated.csv`

### What it contains

This is the main baseline ATT table using the same Callaway-Sant'Anna staggered DiD design as the original geospatial project, with never-treated municipalities as controls.

Each row is a PGA treatment partition:

- `4-10`
- `10-20`
- `20+`

Columns:

- `partition`: PGA bin
- `n_cohorts`: number of treatment cohorts used in that partition
- `n_treated`: number of treated municipalities in that partition
- `n_controls`: number of control municipalities
- `att`: average treatment effect on treated municipalities
- `se`: standard error
- `ci_lo`, `ci_hi`: 95% confidence interval
- `p_val`: p-value
- `sig`: significance stars

### Main interpretation

This is the cleanest summary of the receiver-side effect. It shows that:

- municipalities in the `4-10` and `10-20` PGA bins experience positive remittance responses
- municipalities in the `20+` PGA bin experience a negative remittance response

So the main intuition survives the cleaner matrix: moderate earthquakes are associated with increased remittances, while the most severe earthquakes are associated with lower remittances.

## 3. `receiver_att_table_low_dose_controls_calibrated.csv`

### What it contains

This is the robustness version of the baseline ATT table. Instead of using only never-treated municipalities as controls, it uses the low-dose treated municipalities (`4-10`) as the comparison group for the higher-dose bins.

Rows:

- `10-20`
- `20+`

Columns are the same as in the baseline ATT table.

### Main interpretation

This checks whether the results depend on comparing earthquake-hit municipalities to places that never experienced an earthquake. The results remain very similar:

- `10-20` stays positive and statistically significant
- `20+` stays negative and statistically significant

That makes the main receiver-side conclusion more credible.

## 4. `receiver_att_by_cohort_calibrated.csv`

### What it contains

This file breaks the baseline ATT results down by treatment cohort rather than averaging everything within each PGA bin.

Each row is one treatment cohort within one PGA partition.

Columns:

- `partition`: PGA bin
- `cohort_events`: earthquake event or events belonging to that treatment quarter
- `year`, `quarter`: calendar timing of the cohort
- `cohort_time_index`: internal quarter index used in estimation
- `cohort_treated_munis`: number of treated municipalities in that cohort
- `att`: cohort-specific ATT
- `se`: standard error
- `ci_lo`, `ci_hi`: 95% confidence interval
- `p_val`: p-value
- `sig`: significance stars

### Main interpretation

This file shows how much heterogeneity there is across earthquake cohorts. It is useful for checking whether the average ATT is driven by only one event or whether the sign pattern appears across multiple cohorts.

In this rerun:

- several moderate-shock cohorts show strong positive effects
- the severe-shock partition is more unstable and less precise cohort by cohort

That matches the broader story that the negative `20+` result is based on a smaller and noisier set of extreme events.

## 5. `receiver_att_by_cohort_low_dose_controls_calibrated.csv`

### What it contains

This is the cohort-specific version of the low-dose-controls robustness check.

Each row is one treatment cohort within either:

- `10-20`
- `20+`

The columns are the same as in the baseline cohort file.

### Main interpretation

This lets you verify whether the robustness result also holds cohort by cohort. It is especially useful for checking whether the moderate positive effects and severe negative effects persist once the comparison group is restricted to municipalities that were also exposed to mild shaking.

The broad pattern remains similar, which supports the stability of the receiver-side results.

## 6. `receiver_event_study_baseline_calibrated.csv`

### What it contains

This is the event-study output for the baseline specification.

Each row gives the estimated ATT at a given event time for a given PGA partition.

Columns:

- `partition`: PGA bin
- `egt`: event time in quarters relative to treatment
- `att`: estimated ATT at that event time
- `se`: standard error
- `ci_lo`, `ci_hi`: 95% confidence interval

Interpretation of `egt`:

- negative values: pre-earthquake quarters
- `0`: earthquake quarter
- positive values: post-earthquake quarters

### Main interpretation

This file is used to assess:

- whether treated and control municipalities had similar pre-trends before treatment
- how the effect evolves after the earthquake

The main pattern is:

- `4-10` and `10-20` show positive post-treatment dynamics that build over time
- `20+` turns negative around treatment, but with wider confidence intervals later on

So the event study supports the same intuition as the ATT table, while adding the timing dimension.

## 7. `receiver_event_study_low_dose_controls_calibrated.csv`

### What it contains

This is the event-study output for the robustness specification that uses low-dose municipalities as controls.

The columns are the same as in the baseline event-study file.

### Main interpretation

This file checks whether the event-study pattern survives when the control group is restricted to municipalities that also experienced mild shaking.

The main interpretation is that:

- the post-treatment dynamics remain very similar
- the pre-treatment fit is generally cleaner

That is reassuring because it suggests the results are not driven by comparing seismically active municipalities to fundamentally different never-treated ones.

## 8. `receiver_spillover_results_calibrated.csv`

### What it contains

This file contains the spillover regression coefficients.

Rows correspond to three specifications:

- `geographic`: spillovers through inverse-distance geographic exposure
- `network`: spillovers through migration-network exposure
- `joint`: both channels entered together

Columns:

- `term`: estimated coefficient
- `estimate`: coefficient value
- `std.error`: standard error
- `statistic`: test statistic
- `p.value`: p-value
- `conf.low`, `conf.high`: 95% confidence interval
- `model`: model specification

### Main interpretation

This file tests whether municipalities that were not directly treated still respond because they are connected to treated municipalities.

The key result is:

- geographic spillovers are positive when tested alone
- network spillovers are also positive when tested alone
- when both are included jointly, the geographic effect collapses toward zero while the network effect remains the stronger channel

So the evidence points more toward spillovers traveling through migration networks than through pure physical proximity.

## Bottom line

Taken together, these files tell a coherent story:

- the cleaned remittances matrix reproduces the main receiver-side intuition of the original geospatial project
- moderate earthquake exposure is associated with higher remittances into receiving municipalities
- the most severe earthquake exposure is associated with lower remittances
- the robustness checks support this pattern
- spillovers seem to move more through migrant networks than through geography alone
