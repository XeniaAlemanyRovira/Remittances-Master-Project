# Receiver-side calibrated rerun

Analysis date: 2026-05-03
Panel rows used: 115872
Matched municipalities retained: 2414
Dropped unmatched municipalities: 17

## Baseline ATT estimates
# A tibble: 3 × 10
  partition n_cohorts n_treated n_controls    att     se  ci_lo   ci_hi
  <fct>         <int>     <int>      <int>  <dbl>  <dbl>  <dbl>   <dbl>
1 4-10              7       754       1136  0.507 0.0200  0.468  0.546 
2 10-20             7       329       1136  0.494 0.0291  0.437  0.551 
3 20+               7       195       1136 -0.514 0.245  -0.995 -0.0334
# ℹ 2 more variables: p_val <dbl>, sig <chr>

## Baseline ATT by cohort
# A tibble: 18 × 12
   partition cohort_events   year quarter cohort_time_index cohort_treated_munis
   <chr>     <chr>          <dbl>   <dbl>             <dbl>                <int>
 1 4-10      2014_M7.2_coy…  2014       2                 6                   19
 2 4-10      2014_M6.9_pue…  2014       3                 7                   19
 3 4-10      2017_M7.1_mat…  2017       3                19                  764
 4 4-10      2018_M7.2_pin…  2018       1                21                  149
 5 4-10      2020_M7.4_San…  2020       2                30                  221
 6 4-10      2021_M7.0_aca…  2021       3                35                   15
 7 10-20     2014_M7.2_coy…  2014       2                 6                   19
 8 10-20     2014_M6.9_pue…  2014       3                 7                   19
 9 10-20     2017_M7.1_mat…  2017       3                19                  764
10 10-20     2018_M7.2_pin…  2018       1                21                  149
11 10-20     2020_M7.4_San…  2020       2                30                  221
12 10-20     2021_M7.0_aca…  2021       3                35                   15
13 20+       2014_M7.2_coy…  2014       2                 6                   19
14 20+       2014_M6.9_pue…  2014       3                 7                   19
15 20+       2017_M7.1_mat…  2017       3                19                  764
16 20+       2018_M7.2_pin…  2018       1                21                  149
17 20+       2020_M7.4_San…  2020       2                30                  221
18 20+       2021_M7.0_aca…  2021       3                35                   15
# ℹ 6 more variables: att <dbl>, se <dbl>, ci_lo <dbl>, ci_hi <dbl>,
#   p_val <dbl>, sig <chr>

## Low-dose-controls ATT estimates
# A tibble: 2 × 10
  partition n_cohorts n_treated n_controls    att     se  ci_lo   ci_hi    p_val
  <fct>         <int>     <int>      <int>  <dbl>  <dbl>  <dbl>   <dbl>    <dbl>
1 10-20             7       329       1890  0.494 0.0285  0.438  0.550  3.09e-67
2 20+               7       195       1890 -0.514 0.247  -0.998 -0.0306 3.71e- 2
# ℹ 1 more variable: sig <chr>

## Low-dose-controls ATT by cohort
# A tibble: 12 × 12
   partition cohort_events   year quarter cohort_time_index cohort_treated_munis
   <chr>     <chr>          <dbl>   <dbl>             <dbl>                <int>
 1 10-20     2014_M7.2_coy…  2014       2                 6                   19
 2 10-20     2014_M6.9_pue…  2014       3                 7                   19
 3 10-20     2017_M7.1_mat…  2017       3                19                  764
 4 10-20     2018_M7.2_pin…  2018       1                21                  149
 5 10-20     2020_M7.4_San…  2020       2                30                  221
 6 10-20     2021_M7.0_aca…  2021       3                35                   15
 7 20+       2014_M7.2_coy…  2014       2                 6                   19
 8 20+       2014_M6.9_pue…  2014       3                 7                   19
 9 20+       2017_M7.1_mat…  2017       3                19                  764
10 20+       2018_M7.2_pin…  2018       1                21                  149
11 20+       2020_M7.4_San…  2020       2                30                  221
12 20+       2021_M7.0_aca…  2021       3                35                   15
# ℹ 6 more variables: att <dbl>, se <dbl>, ci_lo <dbl>, ci_hi <dbl>,
#   p_val <dbl>, sig <chr>

## Spillover coefficients
# A tibble: 4 × 8
  term             estimate std.error statistic p.value conf.low conf.high model
  <chr>               <dbl>     <dbl>     <dbl>   <dbl>    <dbl>     <dbl> <chr>
1 post:geo_pga_c   0.108       0.0249    4.35   1.51e-5  0.0593     0.157  geog…
2 post:network_pg… 0.168       0.0849    1.98   4.85e-2  0.00114    0.335  netw…
3 post:network_pg… 0.164       0.0863    1.90   5.86e-2 -0.00597    0.333  joint
4 post:geo_pga_c   0.000545    0.0371    0.0147 9.88e-1 -0.0724     0.0735 joint

## Saved event-study and ATT plots
- output/plots/receiver_event_study_baseline_*.png
- output/plots/receiver_event_study_low_dose_controls_*.png
- output/plots/receiver_att_comparison_baseline.png
- output/plots/receiver_att_comparison_low_dose_controls.png
