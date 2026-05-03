# Sender corridor-level calibrated rerun

Analysis date: 2026-05-03
Panel rows used: 5909472
Unique corridors retained: 123114
Unique treated recipient municipalities: 1278
Dropped unmatched corridors: 1887

## Main design note
This rerun uses the US state x Mexican municipality x quarter corridor as the unit of analysis.
Treatment is assigned from the recipient municipality earthquake exposure, while the outcome is the remittance flow sent along that specific corridor.

## Baseline ATT estimates
# A tibble: 3 × 10
  partition n_cohorts n_treated n_controls      att      se     ci_lo   ci_hi
  <fct>         <int>     <int>      <int>    <dbl>   <dbl>     <dbl>   <dbl>
1 4-10              7     38454      57936 0.000267 0.00193 -0.00351  0.00405
2 10-20             7     16779      57936 0.00604  0.00266  0.000830 0.0113 
3 20+               7      9945      57936 0.00821  0.00325  0.00183  0.0146 
# ℹ 2 more variables: p_val <dbl>, sig <chr>

## Baseline ATT by cohort
# A tibble: 21 × 12
   partition cohort_events                        year quarter cohort_time_index
   <chr>     <chr>                               <dbl>   <dbl>             <dbl>
 1 4-10      2014_M7.2_coyuquilla_norte           2014       2                 6
 2 4-10      2014_M6.9_puerto_madero              2014       3                 7
 3 4-10      2017_M7.1_matzaco; 2017_M8.2_chiap…  2017       3                19
 4 4-10      2018_M7.2_pinotepa                   2018       1                21
 5 4-10      2020_M7.4_Santa_Maria_Xadani         2020       2                30
 6 4-10      2021_M7.0_acapulco                   2021       3                35
 7 4-10      2022_M7.6_aguililla                  2022       3                39
 8 10-20     2014_M7.2_coyuquilla_norte           2014       2                 6
 9 10-20     2014_M6.9_puerto_madero              2014       3                 7
10 10-20     2017_M7.1_matzaco; 2017_M8.2_chiap…  2017       3                19
# ℹ 11 more rows
# ℹ 7 more variables: cohort_treated_corridors <int>, att <dbl>, se <dbl>,
#   ci_lo <dbl>, ci_hi <dbl>, p_val <dbl>, sig <chr>

## Low-dose-controls ATT estimates
# A tibble: 2 × 10
  partition n_cohorts n_treated n_controls     att      se    ci_lo   ci_hi
  <fct>         <int>     <int>      <int>   <dbl>   <dbl>    <dbl>   <dbl>
1 10-20             7     16779      96390 0.00312 0.00256 -0.00190 0.00814
2 20+               7      9945      96390 0.00440 0.00311 -0.00168 0.0105 
# ℹ 2 more variables: p_val <dbl>, sig <chr>

## Low-dose-controls ATT by cohort
# A tibble: 14 × 12
   partition cohort_events                        year quarter cohort_time_index
   <chr>     <chr>                               <dbl>   <dbl>             <dbl>
 1 10-20     2014_M7.2_coyuquilla_norte           2014       2                 6
 2 10-20     2014_M6.9_puerto_madero              2014       3                 7
 3 10-20     2017_M7.1_matzaco; 2017_M8.2_chiap…  2017       3                19
 4 10-20     2018_M7.2_pinotepa                   2018       1                21
 5 10-20     2020_M7.4_Santa_Maria_Xadani         2020       2                30
 6 10-20     2021_M7.0_acapulco                   2021       3                35
 7 10-20     2022_M7.6_aguililla                  2022       3                39
 8 20+       2014_M7.2_coyuquilla_norte           2014       2                 6
 9 20+       2014_M6.9_puerto_madero              2014       3                 7
10 20+       2017_M7.1_matzaco; 2017_M8.2_chiap…  2017       3                19
11 20+       2018_M7.2_pinotepa                   2018       1                21
12 20+       2020_M7.4_Santa_Maria_Xadani         2020       2                30
13 20+       2021_M7.0_acapulco                   2021       3                35
14 20+       2022_M7.6_aguililla                  2022       3                39
# ℹ 7 more variables: cohort_treated_corridors <int>, att <dbl>, se <dbl>,
#   ci_lo <dbl>, ci_hi <dbl>, p_val <dbl>, sig <chr>

## Spillover coefficients
# A tibble: 4 × 8
  term             estimate std.error statistic p.value conf.low conf.high model
  <chr>               <dbl>     <dbl>     <dbl>   <dbl>    <dbl>     <dbl> <chr>
1 post:geo_pga_c    0.0399    0.00929      4.30 1.73e-5 0.0217      0.0582 geog…
2 post:network_pg…  0.00662   0.00304      2.18 2.93e-2 0.000666    0.0126 netw…
3 post:network_pg…  0.0117    0.00509      2.29 2.21e-2 0.00168     0.0216 joint
4 post:geo_pga_c    0.0357    0.00977      3.65 2.63e-4 0.0165      0.0548 joint

## Saved event-study and ATT plots
- output/plots/sender_corridor_event_study_baseline_*.png
- output/plots/sender_corridor_event_study_low_dose_controls_*.png
- output/plots/sender_corridor_att_comparison_baseline.png
- output/plots/sender_corridor_att_comparison_low_dose_controls.png
