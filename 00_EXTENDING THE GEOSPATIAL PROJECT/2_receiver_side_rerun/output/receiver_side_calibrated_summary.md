# Receiver-side calibrated rerun

Analysis date: 2026-05-03
Panel rows used: 115872
Matched municipalities retained: 2414
Dropped unmatched municipalities: 17

## Baseline ATT estimates
# A tibble: 3 × 10
  partition n_cohorts n_treated n_controls    att     se  ci_lo   ci_hi
  <fct>         <int>     <int>      <int>  <dbl>  <dbl>  <dbl>   <dbl>
1 4-10              7       754       1136  0.507 0.0201  0.467  0.546 
2 10-20             7       329       1136  0.494 0.0291  0.437  0.551 
3 20+               7       195       1136 -0.514 0.245  -0.995 -0.0333
# ℹ 2 more variables: p_val <dbl>, sig <chr>

## Low-dose-controls ATT estimates
# A tibble: 2 × 10
  partition n_cohorts n_treated n_controls    att     se  ci_lo   ci_hi    p_val
  <fct>         <int>     <int>      <int>  <dbl>  <dbl>  <dbl>   <dbl>    <dbl>
1 10-20             7       329       1890  0.494 0.0280  0.440  0.549  9.58e-70
2 20+               7       195       1890 -0.514 0.243  -0.990 -0.0387 3.40e- 2
# ℹ 1 more variable: sig <chr>

## Spillover coefficients
# A tibble: 4 × 8
  term             estimate std.error statistic p.value conf.low conf.high model
  <chr>               <dbl>     <dbl>     <dbl>   <dbl>    <dbl>     <dbl> <chr>
1 post:geo_pga_c   0.108       0.0249    4.35   1.51e-5  0.0593     0.157  geog…
2 post:network_pg… 0.168       0.0849    1.98   4.85e-2  0.00114    0.335  netw…
3 post:network_pg… 0.164       0.0863    1.90   5.86e-2 -0.00597    0.333  joint
4 post:geo_pga_c   0.000545    0.0371    0.0147 9.88e-1 -0.0724     0.0735 joint
