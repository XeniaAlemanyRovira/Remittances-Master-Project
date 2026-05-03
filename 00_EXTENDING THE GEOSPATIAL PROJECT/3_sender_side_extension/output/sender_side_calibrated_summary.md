# Sender-side calibrated extension

Analysis date: 2026-05-03
Panel rows used: 2448
Sender states in panel: 51
Ever-treated sender states (mean PGA > 4): 19
Available sender treatment partitions: 10-20, 4-10

## Main design note
The sender-side extension keeps the receiver-side econometric pipeline, but the unit of observation is the US sending state by quarter and treatment is defined using remittance-weighted mean recipient PGA exposure.
Because no sender states ever reach a remittance-weighted mean exposure above 20%g, the sender-side analysis only identifies the 4-10 and 10-20 bins.

## Baseline ATT estimates
# A tibble: 2 × 10
  partition n_cohorts n_treated n_controls     att     se   ci_lo    ci_hi
  <fct>         <int>     <int>      <int>   <dbl>  <dbl>   <dbl>    <dbl>
1 4-10              1        16         32 -0.0396 0.0159 -0.0708 -0.00840
2 10-20             1         3         32 -0.0625 0.0152 -0.0923 -0.0327 
# ℹ 2 more variables: p_val <dbl>, sig <chr>

## Baseline ATT by cohort
# A tibble: 2 × 12
  partition cohort_events   year quarter cohort_time_index cohort_treated_states
  <chr>     <chr>          <dbl>   <dbl>             <dbl>                 <int>
1 4-10      2017_M7.1_mat…  2017       3                19                    16
2 10-20     2017_M7.1_mat…  2017       3                19                     3
# ℹ 6 more variables: att <dbl>, se <dbl>, ci_lo <dbl>, ci_hi <dbl>,
#   p_val <dbl>, sig <chr>

## Low-dose-controls ATT estimates
# A tibble: 1 × 10
  partition n_cohorts n_treated n_controls     att     se   ci_lo   ci_hi
  <fct>         <int>     <int>      <int>   <dbl>  <dbl>   <dbl>   <dbl>
1 10-20             1         3         48 -0.0493 0.0161 -0.0810 -0.0177
# ℹ 2 more variables: p_val <dbl>, sig <chr>

## Low-dose-controls ATT by cohort
# A tibble: 1 × 12
  partition cohort_events   year quarter cohort_time_index cohort_treated_states
  <chr>     <chr>          <dbl>   <dbl>             <dbl>                 <int>
1 10-20     2017_M7.1_mat…  2017       3                19                     3
# ℹ 6 more variables: att <dbl>, se <dbl>, ci_lo <dbl>, ci_hi <dbl>,
#   p_val <dbl>, sig <chr>

## Spillover coefficients
# A tibble: 4 × 8
  term             estimate std.error statistic p.value conf.low conf.high model
  <chr>               <dbl>     <dbl>     <dbl>   <dbl>    <dbl>     <dbl> <chr>
1 post:geo_pga_c    0.01000    0.0429    0.233    0.817  -0.0776    0.0976 geog…
2 post:network_pg… -0.00161    0.125    -0.0128   0.990  -0.257     0.254  netw…
3 post:network_pg… -0.0124     0.150    -0.0829   0.934  -0.318     0.293  joint
4 post:geo_pga_c    0.0123     0.0546    0.225    0.823  -0.0991    0.124  joint

## Saved event-study and ATT plots
- output/plots/sender_event_study_baseline_*.png
- output/plots/sender_event_study_low_dose_controls_*.png
- output/plots/sender_att_comparison_baseline.png
- output/plots/sender_att_comparison_low_dose_controls.png
