# Hurricane Ian and US-to-Mexico remittances ----
#
# Identification idea:
#   Compare Florida-origin remittance flows to the same Mexican municipality
#   against flows from other US states, before and after Hurricane Ian.


suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(fixest)
  library(DRDID)
  library(broom)
  library(modelsummary)
  library(scales)
})

options(scipen = 999)
options("modelsummary_format_numeric_latex" = "plain")
setFixest_notes(FALSE)

# Paths ----

analysis_dir <- "./Shocks/Ian hurricane"
data_file <- "./1_network_estimation/4_remittance_calibration/output/calibrated_remittance_flows_master_2013q1_2024q4.csv"

output_dir <- file.path(analysis_dir, "outputs")
plot_dir <- file.path(output_dir, "plots")
table_dir <- file.path(output_dir, "tables")

dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(table_dir, showWarnings = FALSE, recursive = TRUE)

# Parameters ----

ian_landfall <- as.Date("2022-09-28")
post_start <- as.Date("2022-10-01")
post_quarter_index <- 2022L * 4L + 4L
event_ref_quarter <- -1L
event_window <- -16:8
carolina_control_states <- c("North Carolina", "South Carolina")

region_levels <- c(
  "North",
  "Bajio/West",
  "Central",
  "South",
  "Gulf/Southeast"
)

mx_region_lookup <- data.table(
  mx_state = c(
    "Baja California", "Baja California Sur", "Chihuahua",
    "Coahuila De Zaragoza", "Durango", "Nuevo Leon", "Sinaloa",
    "Sonora", "Tamaulipas",
    "Aguascalientes", "Colima", "Guanajuato", "Jalisco",
    "Michoacan De Ocampo", "Nayarit", "Queretaro",
    "San Luis Potosi", "Zacatecas",
    "Ciudad De Mexico", "Estado De Mexico", "Hidalgo", "Morelos",
    "Puebla", "Tlaxcala",
    "Chiapas", "Guerrero", "Oaxaca",
    "Campeche", "Quintana Roo", "Tabasco",
    "Veracruz De Ignacio De La Llave", "Yucatan"
  ),
  mx_region = c(
    rep("North", 9),
    rep("Bajio/West", 9),
    rep("Central", 6),
    rep("South", 3),
    rep("Gulf/Southeast", 5)
  )
)

# Load and construct panel ----

remittances_time_series <- fread(data_file)
setDT(remittances_time_series)

remittances_time_series[, period_date := as.Date(period_date)]
remittances_time_series[, quarter_index := year * 4L + quarter]
remittances_time_series[, mx_municipality_id := paste(mx_state, mx_municipality, sep = " - ")]

remittances_time_series[
  mx_region_lookup,
  mx_region := i.mx_region,
  on = "mx_state"
]

if (remittances_time_series[is.na(mx_region), .N] > 0) {
  missing_regions <- remittances_time_series[
    is.na(mx_region),
    sort(unique(mx_state))
  ]
  stop("Missing Mexican region assignment for: ", paste(missing_regions, collapse = ", "))
}

remittances_time_series[, mx_region := factor(mx_region, levels = region_levels)]
remittances_time_series[, florida := as.integer(us_state == "Florida")]
remittances_time_series[, post_ian := as.integer(period_date >= post_start)]
remittances_time_series[, treatment := florida * post_ian]
remittances_time_series[, relative_quarter := quarter_index - post_quarter_index]
remittances_time_series[, log1p_remittances := log1p(remittances_musd)]

# Numeric fixed-effect identifiers keep fixest fast on the 6 million-row panel.
remittances_time_series[, pair_id := .GRP, by = .(us_state, mx_municipality_id)]
remittances_time_series[, municipality_period_id := .GRP, by = .(mx_municipality_id, period_date)]

setorder(remittances_time_series, us_state, mx_municipality_id, period_date)

# Preliminary tables ----

panel_summary <- data.table(
  observations = nrow(remittances_time_series),
  us_states = remittances_time_series[, uniqueN(us_state)],
  mx_states = remittances_time_series[, uniqueN(mx_state)],
  mx_municipalities = remittances_time_series[, uniqueN(mx_municipality_id)],
  quarters = remittances_time_series[, uniqueN(period_date)],
  first_quarter = remittances_time_series[, min(period_date)],
  last_quarter = remittances_time_series[, max(period_date)],
  ian_landfall = ian_landfall,
  first_treated_quarter = post_start
)

fwrite(panel_summary, file.path(table_dir, "panel_summary.csv"))

state_quarter <- remittances_time_series[
  ,
  .(remittances_musd = sum(remittances_musd, na.rm = TRUE)),
  by = .(us_state, period_date, year_quarter, quarter_index, post_ian)
]

setorder(state_quarter, us_state, period_date)
state_quarter[, log1p_total_remittances := log1p(remittances_musd)]
state_quarter[
  ,
  florida_group := fifelse(us_state == "Florida", "Florida", "Other states")
]

state_pre_post_summary <- state_quarter[
  ,
  .(
    state_quarters = .N,
    avg_quarterly_total_musd = mean(remittances_musd, na.rm = TRUE),
    median_quarterly_total_musd = median(remittances_musd, na.rm = TRUE),
    avg_quarterly_log1p_total = mean(log1p_total_remittances, na.rm = TRUE)
  ),
  by = .(florida_group, post_ian)
][order(florida_group, post_ian)]

fwrite(state_pre_post_summary, file.path(table_dir, "state_pre_post_summary.csv"))

origin_state_pre <- remittances_time_series[
  period_date < post_start,
  .(
    pre_total_musd = sum(remittances_musd, na.rm = TRUE),
    pre_avg_quarterly_musd = sum(remittances_musd, na.rm = TRUE) / uniqueN(period_date)
  ),
  by = us_state
][order(-pre_total_musd)]

origin_state_pre[, pre_rank := seq_len(.N)]
origin_state_pre[
  ,
  pre_share_of_all_us_remittances := pre_total_musd / sum(pre_total_musd)
]

fwrite(origin_state_pre, file.path(table_dir, "origin_state_pre_ian_rankings.csv"))

top_florida_destinations <- remittances_time_series[
  period_date < post_start,
  .(
    pre_florida_total_musd = sum(remittances_musd[us_state == "Florida"], na.rm = TRUE),
    pre_all_states_total_musd = sum(remittances_musd, na.rm = TRUE),
    pre_avg_quarterly_florida_musd =
      sum(remittances_musd[us_state == "Florida"], na.rm = TRUE) / uniqueN(period_date)
  ),
  by = .(mx_region, mx_state, mx_municipality, mx_municipality_id)
]

top_florida_destinations[
  ,
  pre_florida_share := pre_florida_total_musd / pre_all_states_total_musd
]
setorder(top_florida_destinations, -pre_florida_total_musd)

top_50_destinations <- head(copy(top_florida_destinations), 50)
top_50_destinations[, pre_florida_rank := seq_len(.N)]
top_50_municipalities <- top_50_destinations[, mx_municipality_id]

fwrite(
  top_50_destinations,
  file.path(table_dir, "top_50_florida_destination_municipalities_pre_ian.csv")
)

region_exposure <- remittances_time_series[
  ,
  .(
    municipalities = uniqueN(mx_municipality_id),
    pre_florida_total_musd =
      sum(remittances_musd[period_date < post_start & us_state == "Florida"], na.rm = TRUE),
    pre_all_states_total_musd =
      sum(remittances_musd[period_date < post_start], na.rm = TRUE),
    post_florida_total_musd =
      sum(remittances_musd[period_date >= post_start & us_state == "Florida"], na.rm = TRUE),
    post_all_states_total_musd =
      sum(remittances_musd[period_date >= post_start], na.rm = TRUE)
  ),
  by = mx_region
]

region_exposure[
  ,
  `:=`(
    pre_florida_share = pre_florida_total_musd / pre_all_states_total_musd,
    post_florida_share = post_florida_total_musd / post_all_states_total_musd
  )
]
setorder(region_exposure, mx_region)

fwrite(region_exposure, file.path(table_dir, "region_exposure_pre_post_ian.csv"))

# Preliminary plots ----


origin_group_quarter <- remittances_time_series[
  ,
  .(remittances_musd = sum(remittances_musd, na.rm = TRUE)),
  by = .(
    period_date,
    origin_group = fifelse(us_state == "Florida", "Florida", "Other states pooled")
  )
]

origin_group_quarter[
  ,
  pre_mean := mean(remittances_musd[period_date < post_start], na.rm = TRUE),
  by = origin_group
]
origin_group_quarter[, remittance_index := 100 * remittances_musd / pre_mean]

plot_origin_index <- ggplot(
  origin_group_quarter,
  aes(x = period_date, y = remittance_index, color = origin_group)
) +
  geom_hline(yintercept = 100, color = "grey75", linewidth = 0.35) +
  geom_line(linewidth = 0.8) +
  geom_point(size = 1.3) +
  geom_vline(
    xintercept = post_start,
    linetype = "dashed",
    linewidth = 0.8,
    color = "grey20"
  ) +
  scale_color_manual(values = c("Florida" = "#C23B22", "Other states pooled" = "#2F6F9F")) +
  scale_y_continuous(labels = label_number(accuracy = 1)) +
  labs(
    title = "Indexed remittances before and after Hurricane Ian",
    subtitle = "Series are indexed to their own pre-Ian quarterly average (=100).",
    x = NULL,
    y = "Index",
    color = NULL
  ) +
  theme_minimal(base_size = 12) +
  theme(
    legend.position = "top",
    panel.grid.minor = element_blank()
  )

ggsave(
  filename = file.path(plot_dir, "indexed_remittances_florida_vs_other_states.png"),
  plot = plot_origin_index,
  width = 9,
  height = 5,
  dpi = 300
)

origin_group_quarter_no_carolinas <- remittances_time_series[
  !us_state %chin% carolina_control_states,
  .(remittances_musd = sum(remittances_musd, na.rm = TRUE)),
  by = .(
    period_date,
    origin_group = fifelse(
      us_state == "Florida",
      "Florida",
      "Other states pooled, excluding NC/SC"
    )
  )
]

origin_group_quarter_no_carolinas[
  ,
  pre_mean := mean(remittances_musd[period_date < post_start], na.rm = TRUE),
  by = origin_group
]
origin_group_quarter_no_carolinas[
  ,
  remittance_index := 100 * remittances_musd / pre_mean
]

plot_origin_index_no_carolinas <- ggplot(
  origin_group_quarter_no_carolinas,
  aes(x = period_date, y = remittance_index, color = origin_group)
) +
  geom_hline(yintercept = 100, color = "grey75", linewidth = 0.35) +
  geom_line(linewidth = 0.8) +
  geom_point(size = 1.3) +
  geom_vline(
    xintercept = post_start,
    linetype = "dashed",
    linewidth = 0.8,
    color = "grey20"
  ) +
  scale_color_manual(
    values = c(
      "Florida" = "#C23B22",
      "Other states pooled, excluding NC/SC" = "#2F6F9F"
    )
  ) +
  scale_y_continuous(labels = label_number(accuracy = 1)) +
  labs(
    title = "Indexed remittances with North/South Carolina excluded from controls",
    subtitle = "Series are indexed to their own pre-Ian quarterly average (=100).",
    x = NULL,
    y = "Index",
    color = NULL
  ) +
  theme_minimal(base_size = 12) +
  theme(
    legend.position = "top",
    panel.grid.minor = element_blank()
  )

ggsave(
  filename = file.path(plot_dir, "indexed_remittances_florida_vs_clean_controls_no_carolinas.png"),
  plot = plot_origin_index_no_carolinas,
  width = 9,
  height = 5,
  dpi = 300
)

florida_region_quarter <- remittances_time_series[
  us_state == "Florida",
  .(remittances_musd = sum(remittances_musd, na.rm = TRUE)),
  by = .(mx_region, period_date)
]

florida_region_quarter[
  ,
  pre_mean := mean(remittances_musd[period_date < post_start], na.rm = TRUE),
  by = mx_region
]
florida_region_quarter[, remittance_index := 100 * remittances_musd / pre_mean]

plot_region_index <- ggplot(
  florida_region_quarter,
  aes(x = period_date, y = remittance_index, color = mx_region)
) +
  geom_hline(yintercept = 100, color = "grey75", linewidth = 0.35) +
  geom_line(linewidth = 0.8) +
  geom_vline(
    xintercept = post_start,
    linetype = "dashed",
    linewidth = 0.8,
    color = "grey20"
  ) +
  scale_y_continuous(labels = label_number(accuracy = 1)) +
  labs(
    title = "Florida-origin remittances by Mexican region",
    subtitle = "Each regional series is indexed to its own pre-Ian quarterly average (=100).",
    x = NULL,
    y = "Index",
    color = "Mexican region"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    legend.position = "top",
    panel.grid.minor = element_blank()
  )

ggsave(
  filename = file.path(plot_dir, "florida_remittances_by_mexican_region_index.png"),
  plot = plot_region_index,
  width = 9,
  height = 5,
  dpi = 300
)

plot_top_destinations <- ggplot(
  head(top_florida_destinations, 20),
  aes(
    x = reorder(mx_municipality_id, pre_florida_total_musd),
    y = pre_florida_total_musd,
    fill = mx_region
  )
) +
  geom_col(width = 0.75) +
  coord_flip() +
  scale_y_continuous(labels = label_number(accuracy = 1)) +
  labs(
    title = "Top Mexican municipality destinations for Florida remittances",
    subtitle = "Total Florida-origin remittances in the pre-Ian period.",
    x = NULL,
    y = "Pre-Ian remittances (million USD)",
    fill = "Mexican region"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    legend.position = "bottom",
    panel.grid.minor = element_blank()
  )

ggsave(
  filename = file.path(plot_dir, "top_20_florida_destination_municipalities_pre_ian.png"),
  plot = plot_top_destinations,
  width = 9,
  height = 6,
  dpi = 300
)

# TWFE specifications ----


did_twfe_quarter_fe <- feols(
  log1p_remittances ~ treatment | pair_id + period_date,
  data = remittances_time_series,
  cluster = ~ us_state + mx_municipality_id
)

did_twfe_muni_quarter_fe <- feols(
  log1p_remittances ~ treatment | pair_id + municipality_period_id,
  data = remittances_time_series,
  cluster = ~ us_state + mx_municipality_id
)

remittances_no_carolinas <- remittances_time_series[
  !us_state %chin% carolina_control_states
]

remittances_top50_no_carolinas <- remittances_no_carolinas[
  mx_municipality_id %chin% top_50_municipalities
]

did_twfe_muni_quarter_fe_no_carolinas <- feols(
  log1p_remittances ~ treatment | pair_id + municipality_period_id,
  data = remittances_no_carolinas,
  cluster = ~ us_state + mx_municipality_id
)

did_twfe_top50_muni_quarter_fe_no_carolinas <- feols(
  log1p_remittances ~ treatment | pair_id + municipality_period_id,
  data = remittances_top50_no_carolinas,
  cluster = ~ us_state + mx_municipality_id
)

static_twfe_results <- rbindlist(
  list(
    data.table(
      specification = "Pair + quarter fixed effects",
      sample = "All origin states",
      fixed_effects = "US state-municipality pair; calendar quarter",
      clusters = "US state; Mexican municipality",
      tidy(did_twfe_quarter_fe, conf.int = TRUE)
    ),
    data.table(
      specification = "Pair + municipality-quarter fixed effects",
      sample = "All origin states",
      fixed_effects = "US state-municipality pair; Mexican municipality-quarter",
      clusters = "US state; Mexican municipality",
      tidy(did_twfe_muni_quarter_fe, conf.int = TRUE)
    ),
    data.table(
      specification = "Pair + municipality-quarter fixed effects",
      sample = "North Carolina and South Carolina excluded",
      fixed_effects = "US state-municipality pair; Mexican municipality-quarter",
      clusters = "US state; Mexican municipality",
      tidy(did_twfe_muni_quarter_fe_no_carolinas, conf.int = TRUE)
    ),
    data.table(
      specification = "Pair + municipality-quarter fixed effects",
      sample = "Top 50 Florida-linked municipalities; NC/SC excluded",
      fixed_effects = "US state-municipality pair; Mexican municipality-quarter",
      clusters = "US state; Mexican municipality",
      tidy(did_twfe_top50_muni_quarter_fe_no_carolinas, conf.int = TRUE)
    )
  ),
  fill = TRUE
)

static_twfe_results <- static_twfe_results[
  term == "treatment"
]
static_twfe_results[
  ,
  `:=`(
    estimate_pct = 100 * (exp(estimate) - 1),
    conf.low_pct = 100 * (exp(conf.low) - 1),
    conf.high_pct = 100 * (exp(conf.high) - 1)
  )
]

fwrite(static_twfe_results, file.path(table_dir, "twfe_static_results.csv"))

modelsummary(
  list(
    "All states: quarter FE" = did_twfe_quarter_fe,
    "All states: municipality-quarter FE" = did_twfe_muni_quarter_fe,
    "Clean controls" = did_twfe_muni_quarter_fe_no_carolinas,
    "Top 50 clean controls" = did_twfe_top50_muni_quarter_fe_no_carolinas
  ),
  coef_map = c("treatment" = "Florida x post-Ian"),
  statistic = "({std.error})",
  stars = TRUE,
  gof_omit = "AIC|BIC|Log.Lik.|RMSE",
  notes = c(
    "Outcome is log(1 + remittances in million USD).",
    "Standard errors are clustered by origin state and Mexican municipality.",
    "Clean controls exclude North Carolina and South Carolina. Top 50 restricts Mexican destinations to the largest pre-Ian Florida-linked municipalities."
  ),
  output = file.path(table_dir, "twfe_static_results.tex")
)

# TWFE event-study specifications ----

event_axis_breaks <- seq(min(event_window), max(event_window), by = 4L)
pretrend_window <- event_window[event_window < event_ref_quarter]
pretrend_keep_pattern <- paste0(
  paste0("relative_quarter::", pretrend_window, ":florida"),
  collapse = "|"
)

quarter_label_from_index <- function(quarter_index) {
  quarter <- ((quarter_index - 1L) %% 4L) + 1L
  year <- (quarter_index - quarter) %/% 4L
  paste0(year, " Q", quarter)
}

theme_event_study <- function(base_size = 11) {
  theme_classic(base_size = base_size) +
    theme(
      plot.title.position = "plot",
      plot.caption.position = "plot",
      plot.caption = element_text(
        hjust = 0,
        color = "grey35",
        size = rel(0.82),
        lineheight = 1.05
      ),
      plot.margin = margin(8, 12, 8, 8),
      panel.grid.major.y = element_line(color = "grey88", linewidth = 0.25),
      panel.grid.minor = element_blank(),
      axis.line = element_line(color = "grey30", linewidth = 0.3),
      axis.ticks = element_line(color = "grey30", linewidth = 0.3),
      legend.position = "top",
      legend.title = element_blank(),
      strip.background = element_rect(
        fill = "grey94",
        color = "grey75",
        linewidth = 0.3
      ),
      strip.text = element_text(face = "bold", size = rel(0.85))
    )
}

extract_event_study <- function(
    model,
    study,
    subgroup,
    sample,
    fixed_effects,
    clusters) {
  estimates <- as.data.table(tidy(model, conf.int = TRUE))
  estimates <- estimates[grepl("^relative_quarter::", term)]
  estimates[
    ,
    relative_quarter := as.integer(
      sub("^relative_quarter::(-?[0-9]+):florida$", "\\1", term)
    )
  ]
  estimates <- estimates[relative_quarter %in% event_window]
  estimates[
    ,
    `:=`(
      study = study,
      subgroup = subgroup,
      sample = sample,
      fixed_effects = fixed_effects,
      clusters = clusters,
      calendar_quarter = quarter_label_from_index(
        post_quarter_index + relative_quarter
      ),
      is_reference_period = FALSE,
      estimate_pct = 100 * (exp(estimate) - 1),
      conf.low_pct = 100 * (exp(conf.low) - 1),
      conf.high_pct = 100 * (exp(conf.high) - 1)
    )
  ]

  reference_row <- data.table(
    study = study,
    subgroup = subgroup,
    sample = sample,
    fixed_effects = fixed_effects,
    clusters = clusters,
    term = "Reference quarter",
    estimate = 0,
    std.error = NA_real_,
    statistic = NA_real_,
    p.value = NA_real_,
    conf.low = 0,
    conf.high = 0,
    relative_quarter = event_ref_quarter,
    calendar_quarter = quarter_label_from_index(
      post_quarter_index + event_ref_quarter
    ),
    is_reference_period = TRUE,
    estimate_pct = 0,
    conf.low_pct = 0,
    conf.high_pct = 0
  )

  estimates <- rbindlist(list(estimates, reference_row), fill = TRUE)
  setorder(estimates, study, subgroup, relative_quarter)
  setcolorder(
    estimates,
    c(
      "study", "subgroup", "sample", "fixed_effects", "clusters",
      "relative_quarter", "calendar_quarter", "is_reference_period",
      "term", "estimate", "std.error", "statistic", "p.value",
      "conf.low", "conf.high", "estimate_pct", "conf.low_pct",
      "conf.high_pct"
    )
  )
  estimates
}

extract_pretrend_test <- function(
    model,
    study,
    subgroup,
    sample,
    fixed_effects,
    clusters) {
  test <- tryCatch(
    wald(model, keep = pretrend_keep_pattern, print = FALSE),
    error = function(e) {
      structure(list(error_message = conditionMessage(e)), class = "event_wald_error")
    }
  )

  if (inherits(test, "event_wald_error")) {
    return(
      data.table(
        study = study,
        subgroup = subgroup,
        sample = sample,
        fixed_effects = fixed_effects,
        clusters = clusters,
        tested_relative_quarters = paste(pretrend_window, collapse = ", "),
        statistic = NA_real_,
        p.value = NA_real_,
        df1 = NA_real_,
        df2 = NA_real_,
        error_message = test$error_message
      )
    )
  }

  data.table(
    study = study,
    subgroup = subgroup,
    sample = sample,
    fixed_effects = fixed_effects,
    clusters = clusters,
    tested_relative_quarters = paste(pretrend_window, collapse = ", "),
    statistic = test$stat,
    p.value = test$p,
    df1 = test$df1,
    df2 = test$df2,
    error_message = NA_character_
  )
}

plot_event_study_facets <- function(
    plot_data,
    facet_var,
    title,
    subtitle,
    caption,
    filename,
    width,
    height,
    ncol,
    base_size = 11,
    free_y = FALSE) {
  plot_data <- copy(plot_data)
  plot_data[
    ,
    facet_label := get(facet_var)
  ]

  event_plot <- ggplot(
    plot_data,
    aes(x = relative_quarter, y = estimate_pct)
  ) +
    geom_hline(yintercept = 0, color = "grey35", linewidth = 0.35) +
    geom_vline(
      xintercept = -0.5,
      linetype = "dashed",
      color = "grey35",
      linewidth = 0.4
    ) +
    geom_linerange(
      aes(ymin = conf.low_pct, ymax = conf.high_pct),
      color = "#4A5568",
      linewidth = 0.35,
      alpha = 0.8
    ) +
    geom_line(
      color = "#1F4E79",
      linewidth = 0.45,
      alpha = 0.9
    ) +
    geom_point(
      aes(shape = is_reference_period),
      color = "#1F2937",
      fill = "white",
      size = 1.7,
      stroke = 0.6
    ) +
    facet_wrap(
      ~ facet_label,
      ncol = ncol,
      scales = if (free_y) "free_y" else "fixed",
      labeller = label_wrap_gen(width = 34)
    ) +
    scale_shape_manual(values = c("FALSE" = 16, "TRUE" = 21), guide = "none") +
    scale_x_continuous(breaks = event_axis_breaks) +
    scale_y_continuous(labels = label_number(accuracy = 0.1, suffix = "%")) +
    labs(
      title = title,
      subtitle = subtitle,
      x = "Quarters relative to 2022 Q4",
      y = "Effect on log(1 + remittances), percent",
      caption = str_wrap(caption, width = 145)
    ) +
    theme_event_study(base_size = base_size)

  ggsave(
    filename = file.path(plot_dir, filename),
    plot = event_plot,
    width = width,
    height = height,
    dpi = 300
  )

  event_plot
}

plot_event_study_single <- function(
    plot_data,
    title,
    subtitle,
    caption,
    filename,
    width = 8.5,
    height = 5.5,
    base_size = 11) {
  plot_data <- copy(plot_data)

  event_plot <- ggplot(
    plot_data,
    aes(x = relative_quarter, y = estimate_pct)
  ) +
    geom_hline(yintercept = 0, color = "grey35", linewidth = 0.35) +
    geom_vline(
      xintercept = -0.5,
      linetype = "dashed",
      color = "grey35",
      linewidth = 0.4
    ) +
    geom_linerange(
      aes(ymin = conf.low_pct, ymax = conf.high_pct),
      color = "#5F6C80",
      linewidth = 0.45,
      alpha = 0.9
    ) +
    geom_line(color = "#1F4E79", linewidth = 0.65) +
    geom_point(
      aes(shape = is_reference_period),
      color = "#1F2937",
      fill = "white",
      size = 2.1,
      stroke = 0.7
    ) +
    scale_shape_manual(values = c("FALSE" = 16, "TRUE" = 21), guide = "none") +
    scale_x_continuous(breaks = event_axis_breaks) +
    scale_y_continuous(labels = label_number(accuracy = 0.1, suffix = "%")) +
    labs(
      title = title,
      subtitle = subtitle,
      x = "Quarters relative to 2022 Q4",
      y = "Effect on log(1 + remittances), percent",
      caption = str_wrap(caption, width = 118)
    ) +
    theme_event_study(base_size = base_size)

  ggsave(
    filename = file.path(plot_dir, filename),
    plot = event_plot,
    width = width,
    height = height,
    dpi = 300
  )

  event_plot
}

event_plot_caption <- paste(
  "Notes: Points report TWFE event-study coefficients transformed as",
  "100 x (exp(beta) - 1), with 95% confidence intervals.",
  "The omitted quarter is 2022 Q3; the dashed line marks the first post-Ian quarter."
)

did_es_quarter_fe <- feols(
  log1p_remittances ~ i(relative_quarter, florida, ref = event_ref_quarter) |
    pair_id + period_date,
  data = remittances_time_series,
  cluster = ~ us_state + mx_municipality_id
)

did_es_muni_quarter_fe <- feols(
  log1p_remittances ~ i(relative_quarter, florida, ref = event_ref_quarter) |
    pair_id + municipality_period_id,
  data = remittances_time_series,
  cluster = ~ us_state + mx_municipality_id
)

did_es_muni_quarter_fe_no_carolinas <- feols(
  log1p_remittances ~ i(relative_quarter, florida, ref = event_ref_quarter) |
    pair_id + municipality_period_id,
  data = remittances_no_carolinas,
  cluster = ~ us_state + mx_municipality_id
)

did_es_top50_muni_quarter_fe_no_carolinas <- feols(
  log1p_remittances ~ i(relative_quarter, florida, ref = event_ref_quarter) |
    pair_id + municipality_period_id,
  data = remittances_top50_no_carolinas,
  cluster = ~ us_state + mx_municipality_id
)

aggregate_event_study_results <- rbindlist(
  list(
    extract_event_study(
      did_es_quarter_fe,
      study = "Aggregate TWFE event study",
      subgroup = "Pair + quarter FE",
      sample = "All origin states",
      fixed_effects = "US state-municipality pair; calendar quarter",
      clusters = "US state; Mexican municipality"
    ),
    extract_event_study(
      did_es_muni_quarter_fe,
      study = "Aggregate TWFE event study",
      subgroup = "Pair + municipality-quarter FE",
      sample = "All origin states",
      fixed_effects = "US state-municipality pair; Mexican municipality-quarter",
      clusters = "US state; Mexican municipality"
    ),
    extract_event_study(
      did_es_muni_quarter_fe_no_carolinas,
      study = "Aggregate TWFE event study",
      subgroup = "Preferred, excluding NC/SC",
      sample = "North Carolina and South Carolina excluded",
      fixed_effects = "US state-municipality pair; Mexican municipality-quarter",
      clusters = "US state; Mexican municipality"
    )
  ),
  fill = TRUE
)

aggregate_spec_levels <- c(
  "Pair + quarter FE",
  "Pair + municipality-quarter FE",
  "Preferred, excluding NC/SC"
)

aggregate_pretrend_tests <- rbindlist(
  list(
    extract_pretrend_test(
      did_es_quarter_fe,
      study = "Aggregate TWFE event study",
      subgroup = "Pair + quarter FE",
      sample = "All origin states",
      fixed_effects = "US state-municipality pair; calendar quarter",
      clusters = "US state; Mexican municipality"
    ),
    extract_pretrend_test(
      did_es_muni_quarter_fe,
      study = "Aggregate TWFE event study",
      subgroup = "Pair + municipality-quarter FE",
      sample = "All origin states",
      fixed_effects = "US state-municipality pair; Mexican municipality-quarter",
      clusters = "US state; Mexican municipality"
    ),
    extract_pretrend_test(
      did_es_muni_quarter_fe_no_carolinas,
      study = "Aggregate TWFE event study",
      subgroup = "Preferred, excluding NC/SC",
      sample = "North Carolina and South Carolina excluded",
      fixed_effects = "US state-municipality pair; Mexican municipality-quarter",
      clusters = "US state; Mexican municipality"
    )
  ),
  fill = TRUE
)

aggregate_event_study_results[
  ,
  subgroup := factor(subgroup, levels = aggregate_spec_levels)
]
aggregate_pretrend_tests[
  ,
  subgroup := factor(subgroup, levels = aggregate_spec_levels)
]

fwrite(
  aggregate_event_study_results,
  file.path(table_dir, "twfe_event_study_aggregate_results.csv")
)
fwrite(
  aggregate_pretrend_tests,
  file.path(table_dir, "twfe_event_study_aggregate_pretrend_tests.csv")
)

aggregate_event_study_preferred <- aggregate_event_study_results[
  subgroup == "Preferred, excluding NC/SC"
]

plot_aggregate_event_study <- plot_event_study_single(
  plot_data = aggregate_event_study_preferred,
  title = "Aggregate TWFE event-study estimates",
  subtitle = "Preferred specification: pair and municipality-quarter fixed effects, excluding NC/SC controls.",
  caption = event_plot_caption,
  filename = "twfe_event_study_aggregate.png",
  width = 8.5,
  height = 5.5,
  base_size = 11
)

plot_aggregate_event_study_specifications <- plot_event_study_facets(
  plot_data = aggregate_event_study_results,
  facet_var = "subgroup",
  title = "Aggregate TWFE event-study specification comparison",
  subtitle = "Same aggregate sample shown by alternative fixed-effect and control-state choices.",
  caption = event_plot_caption,
  filename = "twfe_event_study_aggregate_specification_comparison.png",
  width = 8.5,
  height = 9,
  ncol = 1,
  base_size = 11
)

top50_aggregate_event_study_results <- extract_event_study(
  did_es_top50_muni_quarter_fe_no_carolinas,
  study = "Top 50 pooled TWFE event study",
  subgroup = "Top 50 pooled",
  sample = "Top 50 Florida-linked municipalities; NC/SC excluded",
  fixed_effects = "US state-municipality pair; Mexican municipality-quarter",
  clusters = "US state; Mexican municipality"
)

top50_aggregate_pretrend_tests <- extract_pretrend_test(
  did_es_top50_muni_quarter_fe_no_carolinas,
  study = "Top 50 pooled TWFE event study",
  subgroup = "Top 50 pooled",
  sample = "Top 50 Florida-linked municipalities; NC/SC excluded",
  fixed_effects = "US state-municipality pair; Mexican municipality-quarter",
  clusters = "US state; Mexican municipality"
)

fwrite(
  top50_aggregate_event_study_results,
  file.path(table_dir, "twfe_event_study_top_50_aggregate_results.csv")
)
fwrite(
  top50_aggregate_pretrend_tests,
  file.path(table_dir, "twfe_event_study_top_50_aggregate_pretrend_tests.csv")
)

plot_top50_aggregate_event_study <- plot_event_study_single(
  plot_data = top50_aggregate_event_study_results,
  title = "Top 50 Florida-linked municipalities: pooled TWFE event-study estimates",
  subtitle = "Preferred specification restricted to high-exposure Mexican destinations, excluding NC/SC controls.",
  caption = event_plot_caption,
  filename = "twfe_event_study_top_50_aggregate.png",
  width = 8.5,
  height = 5.5,
  base_size = 11
)

regional_event_study_list <- vector("list", length(region_levels))
regional_pretrend_list <- vector("list", length(region_levels))

for (region_index in seq_along(region_levels)) {
  region_name <- region_levels[[region_index]]
  regional_data <- remittances_no_carolinas[mx_region == region_name]
  regional_model <- feols(
    log1p_remittances ~ i(
      relative_quarter,
      florida,
      ref = event_ref_quarter
    ) | pair_id + municipality_period_id,
    data = regional_data,
    cluster = ~ us_state + mx_municipality_id
  )

  regional_event_study_list[[region_index]] <- extract_event_study(
    regional_model,
    study = "Regional TWFE event study",
    subgroup = region_name,
    sample = "North Carolina and South Carolina excluded",
    fixed_effects = "US state-municipality pair; Mexican municipality-quarter",
    clusters = "US state; Mexican municipality"
  )

  regional_pretrend_list[[region_index]] <- extract_pretrend_test(
    regional_model,
    study = "Regional TWFE event study",
    subgroup = region_name,
    sample = "North Carolina and South Carolina excluded",
    fixed_effects = "US state-municipality pair; Mexican municipality-quarter",
    clusters = "US state; Mexican municipality"
  )
}

regional_event_study_results <- rbindlist(regional_event_study_list, fill = TRUE)
regional_pretrend_tests <- rbindlist(regional_pretrend_list, fill = TRUE)

regional_event_study_results[
  ,
  subgroup := factor(subgroup, levels = region_levels)
]
regional_pretrend_tests[
  ,
  subgroup := factor(subgroup, levels = region_levels)
]

fwrite(
  regional_event_study_results,
  file.path(table_dir, "twfe_event_study_regional_results.csv")
)
fwrite(
  regional_pretrend_tests,
  file.path(table_dir, "twfe_event_study_regional_pretrend_tests.csv")
)

plot_regional_event_study <- plot_event_study_facets(
  plot_data = regional_event_study_results,
  facet_var = "subgroup",
  title = "Regional TWFE event-study estimates",
  subtitle = "Preferred specification estimated separately by Mexican destination region.",
  caption = event_plot_caption,
  filename = "twfe_event_study_regions.png",
  width = 12,
  height = 7,
  ncol = 3,
  base_size = 10
)
