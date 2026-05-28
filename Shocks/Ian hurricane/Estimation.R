#######################################################################################
### --------- ESTIMATION OF THE IMPACT OF HURRICANE IAN ON REMITTANCES ------------ ###
#######################################################################################

# DISCLAIMER:
# This script estimates the effect of Hurricane Ian on Florida remittances using
# three methods and three post-treatment horizons:
#   - Full available post-treatment period
#   - First 4 post-treatment quarters
#   - First 6 post-treatment quarters
#
# The outcome is log remittances. Estimates are log-point effects.
# Percent effects are computed as 100 * (exp(beta) - 1).

# INDEX:
# 1) Data preparation and descriptive plot
#    1.1) Load libraries and define output folder
#    1.2) Load and clean Banxico remittance data
#    1.3) Define treatment, shock date, relative quarters, and log outcome
#    1.4) Plot Florida vs control states
#
# 2) Helper functions for horizon-specific estimation
#    2.1) Horizon sample constructor: full post, 4 post periods, 6 post periods
#    2.2) TWFE estimator with unit and period fixed effects
#    2.3) Synthetic Control estimator with placebo confidence interval
#    2.4) Synthetic Difference-in-Differences estimator with placebo standard error
#
# 3) Main horizon comparison
#    3.1) Estimate TWFE, SC, and SDID for all post-treatment periods
#    3.2) Estimate TWFE, SC, and SDID for first 4 post-treatment periods
#    3.3) Estimate TWFE, SC, and SDID for first 6 post-treatment periods
#    3.4) Export final comparison table as CSV
#
# 4) Diagnostic plots for the full post-treatment sample
#    4.1) TWFE event-study plot
#    4.2) Synthetic Control path, gap, placebo, and placebo-band plots
#    4.3) SDID treated/synthetic path, gap, and dynamic ATT plots

# -------------------------------------------------------------------------------------
# 1. DATA PREPARATION AND DESCRIPTIVE PLOT
# -------------------------------------------------------------------------------------

# 1.1 Load libraries and define output folder ------------------------------------------

library(tidyverse)
library(readxl)
library(fixest)
library(ggplot2)
library(Synth)
library(purrr)
library(tibble)
library(synthdid)

estimation_output_dir <- file.path("Shocks", "Ian hurricane", "outputs", "Estimation_outputs")
dir.create(estimation_output_dir, showWarnings = FALSE, recursive = TRUE)

sdid_placebo_replications <- 200

# 1.2 Load and clean Banxico remittance data -------------------------------------------

remit <- read.csv("1_network_estimation\\3_banxico_cleaning\\output\\banxico_origin_state_remittances_2013q1_2024q4.csv")

remit <- remit %>%
  filter(us_state != "No Identificado") %>%
  filter(us_state != "Puerto Rico") %>%
  mutate(remittances = remittances_musd * 1000000) %>%
  select(-remittances_musd) %>%
  select(-c(year))

# 1.3 Define treatment, shock date, relative quarters, and log outcome -----------------

twfe_remit_dataset <- remit %>%
  mutate(
    group = case_when(
      us_state == "Florida" ~ "Treated",
      us_state != "Florida" ~ "Control",
      TRUE ~ "Unused"
    ),
    period_date = as.Date(period_date)
  )

shock_date <- as.Date("2022-10-01")
unique_quarters <- sort(unique(twfe_remit_dataset$period_date))
shock_index <- which(unique_quarters == shock_date)

if (length(shock_index) != 1) {
  stop("shock_date is not found exactly once in the period_date variable.")
}

final_panel <- twfe_remit_dataset %>%
  mutate(
    rel_quarter = match(period_date, unique_quarters) - shock_index,
    post_period_number = if_else(period_date >= shock_date, rel_quarter + 1L, NA_integer_),
    log_remittances = log(remittances)
  ) %>%
  filter(rel_quarter >= -28 & rel_quarter <= 28)

reg_data_full <- final_panel %>%
  mutate(
    unit_id = us_state,
    is_treated = ifelse(group == "Treated", 1, 0),
    is_post = ifelse(period_date >= shock_date, 1, 0),
    treated_post = is_treated * is_post,
    trend = as.numeric(factor(period_date, levels = sort(unique(period_date))))
  )

# 1.4 Plot Florida vs control states ---------------------------------------------------

twfe_group_plot <- ggplot(final_panel, aes(x = period_date, y = log_remittances, group = us_state, color = group)) +
  geom_line(linewidth = 1, linetype = "solid") +
  geom_point(size = 1.5) +
  geom_vline(xintercept = shock_date, color = "black", linetype = "dashed", linewidth = 0.5) +
  theme_minimal(base_size = 14) +
  theme(
    legend.position = "bottom",
    panel.grid.minor = element_blank(),
    plot.title = element_text(face = "bold")
  ) +
  labs(
    title = "(log) Total Remittances: Florida vs Control States",
    x = "Quarter",
    y = "Total Quarterly Inflows (Log Scale)",
    color = "Group:"
  )

ggsave(
  filename = file.path(estimation_output_dir, "01_twfe_treated_vs_controls.png"),
  plot = twfe_group_plot,
  width = 9,
  height = 5,
  dpi = 300
)

# -------------------------------------------------------------------------------------
# 2. HELPER FUNCTIONS FOR HORIZON-SPECIFIC ESTIMATION
# -------------------------------------------------------------------------------------

# 2.1 Horizon sample constructor -------------------------------------------------------

make_horizon_data <- function(data, max_post_periods = NULL) {
  if (is.null(max_post_periods)) {
    data %>% arrange(unit_id, period_date)
  } else {
    data %>%
      filter(period_date < shock_date | post_period_number <= max_post_periods) %>%
      arrange(unit_id, period_date)
  }
}

# 2.2 TWFE estimator -------------------------------------------------------------------

estimate_twfe <- function(data, horizon_label) {
  twfe_model <- feols(
    log_remittances ~ treated_post | unit_id + period_date,
    data = data,
    cluster = ~unit_id
  )

  beta <- coef(twfe_model)["treated_post"]
  se <- se(twfe_model)["treated_post"]
  ci <- confint(twfe_model, parm = "treated_post", level = 0.95)

  tibble(
    horizon = horizon_label,
    method = "TWFE",
    estimate_log_points = as.numeric(beta),
    standard_error = as.numeric(se),
    ci_lower = as.numeric(ci[1]),
    ci_upper = as.numeric(ci[2]),
    p_value = as.numeric(pvalue(twfe_model)["treated_post"]),
    percent_effect = 100 * (exp(as.numeric(beta)) - 1),
    percent_ci_lower = 100 * (exp(as.numeric(ci[1])) - 1),
    percent_ci_upper = 100 * (exp(as.numeric(ci[2])) - 1)
  )
}

# 2.3 Synthetic Control estimator with placebo confidence interval ---------------------

estimate_sc <- function(data, horizon_label) {
  synth_data <- data %>%
    mutate(
      state_id = as.numeric(factor(unit_id)),
      time_id = as.numeric(factor(period_date, levels = sort(unique(period_date))))
    ) %>%
    arrange(state_id, time_id)

  florida_id <- synth_data %>% filter(unit_id == "Florida") %>% pull(state_id) %>% unique()
  control_ids <- synth_data %>% filter(unit_id != "Florida") %>% pull(state_id) %>% unique()
  pre_period <- synth_data %>% filter(period_date < shock_date) %>% pull(time_id) %>% unique()
  all_period <- sort(unique(synth_data$time_id))
  all_dates <- sort(unique(synth_data$period_date))
  all_state_ids <- sort(unique(synth_data$state_id))
  state_lookup <- synth_data %>% distinct(state_id, unit_id)

  run_synth_one <- function(treated_state_id) {
    control_ids_placebo <- synth_data %>%
      filter(state_id != treated_state_id, unit_id != "Florida") %>%
      pull(state_id) %>%
      unique()

    dp <- dataprep(
      foo = synth_data,
      predictors = c("log_remittances"),
      predictors.op = "mean",
      dependent = "log_remittances",
      unit.variable = "state_id",
      unit.names.variable = "unit_id",
      time.variable = "time_id",
      treatment.identifier = treated_state_id,
      controls.identifier = control_ids_placebo,
      time.predictors.prior = pre_period,
      time.optimize.ssr = pre_period,
      time.plot = all_period
    )

    so <- synth(dp, verbose = FALSE)
    Y1 <- dp$Y1plot
    Y0 <- dp$Y0plot %*% so$solution.w

    tibble(
      treated_state_id = treated_state_id,
      time_id = all_period,
      period_date = all_dates,
      gap = as.numeric(Y1 - Y0)
    )
  }

  placebo_gaps <- map_dfr(all_state_ids, possibly(run_synth_one, otherwise = NULL)) %>%
    left_join(state_lookup, by = c("treated_state_id" = "state_id"))

  placebo_effects <- placebo_gaps %>%
    filter(period_date >= shock_date) %>%
    group_by(unit_id) %>%
    summarise(avg_post_gap = mean(gap, na.rm = TRUE), .groups = "drop")

  florida_effect <- placebo_effects %>% filter(unit_id == "Florida") %>% pull(avg_post_gap)

  placebo_ci <- placebo_effects %>%
    filter(unit_id != "Florida") %>%
    summarise(
      ci_lower = quantile(avg_post_gap, 0.025, na.rm = TRUE),
      ci_upper = quantile(avg_post_gap, 0.975, na.rm = TRUE)
    )

  placebo_p_value <- mean(abs(placebo_effects$avg_post_gap) >= abs(florida_effect), na.rm = TRUE)

  tibble(
    horizon = horizon_label,
    method = "Synthetic Control",
    estimate_log_points = as.numeric(florida_effect),
    standard_error = NA_real_,
    ci_lower = as.numeric(placebo_ci$ci_lower),
    ci_upper = as.numeric(placebo_ci$ci_upper),
    p_value = as.numeric(placebo_p_value),
    percent_effect = 100 * (exp(as.numeric(florida_effect)) - 1),
    percent_ci_lower = 100 * (exp(as.numeric(placebo_ci$ci_lower)) - 1),
    percent_ci_upper = 100 * (exp(as.numeric(placebo_ci$ci_upper)) - 1)
  )
}

# 2.4 Synthetic Difference-in-Differences estimator with placebo standard error ---------

estimate_sdid <- function(data, horizon_label) {
  sdid_data <- data %>%
    mutate(
      state_id = as.numeric(factor(unit_id)),
      time_id = as.numeric(factor(period_date, levels = sort(unique(period_date)))),
      treated = ifelse(unit_id == "Florida" & period_date >= shock_date, 1, 0)
    ) %>%
    arrange(state_id, time_id)

  panel_sdid <- panel.matrices(
    sdid_data,
    unit = "state_id",
    time = "time_id",
    outcome = "log_remittances",
    treatment = "treated"
  )

  sdid_est <- synthdid_estimate(
    Y = panel_sdid$Y,
    N0 = panel_sdid$N0,
    T0 = panel_sdid$T0
  )

  set.seed(2024)
  # Placebo draws are taken from the SDID control block, so Florida is not used as a placebo control.
  sdid_se <- sqrt(vcov(sdid_est, method = "placebo", replications = sdid_placebo_replications))
  beta <- as.numeric(sdid_est)
  ci_lower <- beta - 1.96 * as.numeric(sdid_se)
  ci_upper <- beta + 1.96 * as.numeric(sdid_se)
  p_value <- 2 * pnorm(abs(beta / as.numeric(sdid_se)), lower.tail = FALSE)

  tibble(
    horizon = horizon_label,
    method = "Synthetic Difference-in-Differences",
    estimate_log_points = beta,
    standard_error = as.numeric(sdid_se),
    ci_lower = ci_lower,
    ci_upper = ci_upper,
    p_value = p_value,
    percent_effect = 100 * (exp(beta) - 1),
    percent_ci_lower = 100 * (exp(ci_lower) - 1),
    percent_ci_upper = 100 * (exp(ci_upper) - 1)
  )
}

# -------------------------------------------------------------------------------------
# 3. MAIN HORIZON COMPARISON
# -------------------------------------------------------------------------------------

horizon_specs <- tibble(
  horizon = c("All post-treatment periods", "First 4 post-treatment periods", "First 6 post-treatment periods"),
  max_post_periods = list(NULL, 4L, 6L)
)

estimate_all_methods_for_horizon <- function(horizon_label, max_post_periods) {
  horizon_data <- make_horizon_data(reg_data_full, max_post_periods)

  bind_rows(
    estimate_twfe(horizon_data, horizon_label),
    estimate_sc(horizon_data, horizon_label),
    estimate_sdid(horizon_data, horizon_label)
  )
}

final_estimation_table <- pmap_dfr(
  horizon_specs,
  ~ estimate_all_methods_for_horizon(..1, ..2)
) %>%
  mutate(
    estimate_log_points = round(estimate_log_points, 4),
    standard_error = round(standard_error, 4),
    ci_lower = round(ci_lower, 4),
    ci_upper = round(ci_upper, 4),
    p_value = round(p_value, 4),
    percent_effect = round(percent_effect, 2),
    percent_ci_lower = round(percent_ci_lower, 2),
    percent_ci_upper = round(percent_ci_upper, 2)
  )

print(final_estimation_table)

write.csv(
  final_estimation_table,
  file.path(estimation_output_dir, "final_twfe_sc_sdid_horizon_comparison.csv"),
  row.names = FALSE
)

# -------------------------------------------------------------------------------------
# 4. DIAGNOSTIC PLOTS FOR THE FULL POST-TREATMENT SAMPLE
# -------------------------------------------------------------------------------------

# 4.1 TWFE event-study plot ------------------------------------------------------------

twfe_dynamic_all_post <- feols(
  log_remittances ~ i(period_date, is_treated, ref = as.Date("2022-07-01")) | unit_id + period_date,
  data = reg_data_full,
  cluster = ~unit_id
)

png(file.path(estimation_output_dir, "02_twfe_event_study_all_post.png"), width = 2400, height = 1500, res = 300)
iplot(
  twfe_dynamic_all_post,
  main = "Event Study: Impact of Hurricane Ian on Florida Remittances",
  xlab = "Quarter",
  ylab = "Log Difference in Remittances",
  ylim = c(-2, 2)
)
dev.off()

# 4.2 Synthetic Control path and gap plots ---------------------------------------------

synth_data_plot <- reg_data_full %>%
  mutate(
    state_id = as.numeric(factor(unit_id)),
    time_id = as.numeric(factor(period_date, levels = sort(unique(period_date))))
  )

florida_id_plot <- synth_data_plot %>% filter(unit_id == "Florida") %>% pull(state_id) %>% unique()
control_ids_plot <- synth_data_plot %>% filter(unit_id != "Florida") %>% pull(state_id) %>% unique()
pre_period_plot <- synth_data_plot %>% filter(period_date < shock_date) %>% pull(time_id) %>% unique()
all_period_plot <- sort(unique(synth_data_plot$time_id))
all_dates_plot <- sort(unique(synth_data_plot$period_date))
all_state_ids_plot <- sort(unique(synth_data_plot$state_id))
state_lookup_plot <- synth_data_plot %>% distinct(state_id, unit_id)

dataprep_plot <- dataprep(
  foo = synth_data_plot,
  predictors = c("log_remittances"),
  predictors.op = "mean",
  dependent = "log_remittances",
  unit.variable = "state_id",
  unit.names.variable = "unit_id",
  time.variable = "time_id",
  treatment.identifier = florida_id_plot,
  controls.identifier = control_ids_plot,
  time.predictors.prior = pre_period_plot,
  time.optimize.ssr = pre_period_plot,
  time.plot = all_period_plot
)

synth_plot <- synth(dataprep_plot)

run_synth_placebo_plot <- function(treated_state_id) {
  treated_unit_name <- state_lookup_plot %>%
    filter(state_id == treated_state_id) %>%
    pull(unit_id)

  control_ids_placebo_plot <- synth_data_plot %>%
    filter(
      state_id != treated_state_id,
      unit_id != "Florida" | treated_unit_name == "Florida"
    ) %>%
    pull(state_id) %>%
    unique()

  dp <- dataprep(
    foo = synth_data_plot,
    predictors = c("log_remittances"),
    predictors.op = "mean",
    dependent = "log_remittances",
    unit.variable = "state_id",
    unit.names.variable = "unit_id",
    time.variable = "time_id",
    treatment.identifier = treated_state_id,
    controls.identifier = control_ids_placebo_plot,
    time.predictors.prior = pre_period_plot,
    time.optimize.ssr = pre_period_plot,
    time.plot = all_period_plot
  )

  so <- synth(dp, verbose = FALSE)

  tibble(
    treated_state_id = treated_state_id,
    time_id = all_period_plot,
    period_date = all_dates_plot,
    gap = as.numeric(dp$Y1plot - dp$Y0plot %*% so$solution.w)
  )
}

sc_placebo_gaps_plot <- map_dfr(
  all_state_ids_plot,
  possibly(run_synth_placebo_plot, otherwise = NULL)
) %>%
  left_join(state_lookup_plot, by = c("treated_state_id" = "state_id"))

sc_florida_gap_plot <- sc_placebo_gaps_plot %>%
  filter(unit_id == "Florida")

sc_placebo_band_plot <- sc_placebo_gaps_plot %>%
  filter(unit_id != "Florida") %>%
  group_by(period_date) %>%
  summarise(
    ci_lower = quantile(gap, 0.025, na.rm = TRUE),
    ci_upper = quantile(gap, 0.975, na.rm = TRUE),
    .groups = "drop"
  )

png(file.path(estimation_output_dir, "03_sc_full_post_path.png"), width = 2400, height = 1500, res = 300)
path.plot(
  synth.res = synth_plot,
  dataprep.res = dataprep_plot,
  Ylab = "Log remittances",
  Xlab = "Quarter",
  Legend = c("Florida", "Synthetic Florida"),
  Main = "SC: Florida vs Synthetic Florida, Full Post Period"
)
abline(v = max(pre_period_plot), lty = 2)
dev.off()

png(file.path(estimation_output_dir, "04_sc_full_post_gap.png"), width = 2400, height = 1500, res = 300)
gaps.plot(
  synth.res = synth_plot,
  dataprep.res = dataprep_plot,
  Ylab = "Florida - Synthetic Florida",
  Xlab = "Quarter",
  Main = "SC Gap, Full Post Period"
)
abline(v = max(pre_period_plot), lty = 2)
dev.off()

sc_gap_band_plot <- ggplot() +
  geom_ribbon(
    data = sc_placebo_band_plot,
    aes(x = period_date, ymin = ci_lower, ymax = ci_upper),
    fill = "grey75",
    alpha = 0.45
  ) +
  geom_hline(yintercept = 0, color = "grey45", linewidth = 0.35) +
  geom_vline(xintercept = shock_date, color = "black", linetype = "dashed", linewidth = 0.5) +
  geom_line(
    data = sc_florida_gap_plot,
    aes(x = period_date, y = gap),
    color = "#B2182B",
    linewidth = 0.9
  ) +
  theme_minimal(base_size = 13) +
  theme(panel.grid.minor = element_blank()) +
  labs(
    title = "SC Gap with Placebo Quantile Bands, Full Post Period",
    x = "Quarter",
    y = "Log-point gap"
  )

ggsave(
  filename = file.path(estimation_output_dir, "04b_sc_full_post_gap_placebo_bands.png"),
  plot = sc_gap_band_plot,
  width = 9,
  height = 5,
  dpi = 300
)

sc_placebo_gap_plot <- ggplot(sc_placebo_gaps_plot, aes(x = period_date, y = gap, group = unit_id)) +
  geom_hline(yintercept = 0, color = "grey45", linewidth = 0.35) +
  geom_vline(xintercept = shock_date, color = "black", linetype = "dashed", linewidth = 0.5) +
  geom_line(data = filter(sc_placebo_gaps_plot, unit_id != "Florida"), color = "grey70", linewidth = 0.35, alpha = 0.65) +
  geom_line(data = filter(sc_placebo_gaps_plot, unit_id == "Florida"), color = "#B2182B", linewidth = 1) +
  theme_minimal(base_size = 13) +
  theme(panel.grid.minor = element_blank()) +
  labs(
    title = "SC Placebo Gaps, Full Post Period",
    x = "Quarter",
    y = "Log-point gap"
  )

ggsave(
  filename = file.path(estimation_output_dir, "04c_sc_full_post_placebo_gaps.png"),
  plot = sc_placebo_gap_plot,
  width = 9,
  height = 5,
  dpi = 300
)

# 4.3 SDID treated/synthetic path and gap plots ----------------------------------------

sdid_data_plot <- reg_data_full %>%
  mutate(
    state_id = as.numeric(factor(unit_id)),
    time_id = as.numeric(factor(period_date, levels = sort(unique(period_date)))),
    treated = ifelse(unit_id == "Florida" & period_date >= shock_date, 1, 0)
  ) %>%
  arrange(state_id, time_id)

panel_sdid_plot <- panel.matrices(
  sdid_data_plot,
  unit = "state_id",
  time = "time_id",
  outcome = "log_remittances",
  treatment = "treated"
)

sdid_est_plot <- synthdid_estimate(
  Y = panel_sdid_plot$Y,
  N0 = panel_sdid_plot$N0,
  T0 = panel_sdid_plot$T0
)

set.seed(2024)
# Placebo draws are taken from the SDID control block, so Florida is not used as a placebo control.
sdid_placebo_se_plot <- as.numeric(sqrt(vcov(
  sdid_est_plot,
  method = "placebo",
  replications = sdid_placebo_replications
)))

sdid_weights <- attr(sdid_est_plot, "weights")
sdid_omega <- as.numeric(sdid_weights$omega)
sdid_lambda <- as.numeric(sdid_weights$lambda)

sdid_time_lookup <- sdid_data_plot %>% distinct(time_id, period_date) %>% arrange(time_id)
sdid_treated_rows <- (panel_sdid_plot$N0 + 1):nrow(panel_sdid_plot$Y)
sdid_control_matrix <- panel_sdid_plot$Y[seq_len(panel_sdid_plot$N0), , drop = FALSE]

sdid_paths <- tibble(
  period_date = sdid_time_lookup$period_date,
  post = period_date >= shock_date,
  florida = as.numeric(colMeans(panel_sdid_plot$Y[sdid_treated_rows, , drop = FALSE])),
  synthetic_florida = as.numeric(sdid_omega %*% sdid_control_matrix)
) %>%
  mutate(gap = florida - synthetic_florida)

sdid_pre_adjustment <- sum(sdid_lambda * sdid_paths$gap[seq_len(panel_sdid_plot$T0)])

sdid_paths <- sdid_paths %>%
  mutate(
    dynamic_att = gap - sdid_pre_adjustment,
    ci_lower = dynamic_att - 1.96 * sdid_placebo_se_plot,
    ci_upper = dynamic_att + 1.96 * sdid_placebo_se_plot
  )

sdid_path_plot <- ggplot(sdid_paths, aes(x = period_date)) +
  geom_vline(xintercept = shock_date, color = "black", linetype = "dashed", linewidth = 0.5) +
  geom_line(aes(y = florida, color = "Florida"), linewidth = 0.9) +
  geom_line(aes(y = synthetic_florida, color = "Synthetic Florida"), linewidth = 0.9) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "bottom", panel.grid.minor = element_blank()) +
  labs(
    title = "SDID: Florida vs Synthetic Florida, Full Post Period",
    x = "Quarter",
    y = "Log remittances",
    color = NULL
  )

ggsave(
  filename = file.path(estimation_output_dir, "05_sdid_full_post_florida_vs_synthetic.png"),
  plot = sdid_path_plot,
  width = 9,
  height = 5,
  dpi = 300
)

sdid_gap_plot <- ggplot(sdid_paths, aes(x = period_date, y = gap)) +
  geom_hline(yintercept = 0, color = "grey45", linewidth = 0.35) +
  geom_vline(xintercept = shock_date, color = "black", linetype = "dashed", linewidth = 0.5) +
  geom_line(linewidth = 0.8) +
  geom_point(aes(shape = post), size = 1.7) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "bottom", panel.grid.minor = element_blank()) +
  labs(
    title = "SDID Gap: Florida minus Synthetic Florida, Full Post Period",
    x = "Quarter",
    y = "Log-point gap",
    shape = "Post-Ian"
  )

ggsave(
  filename = file.path(estimation_output_dir, "06_sdid_full_post_gap_path.png"),
  plot = sdid_gap_plot,
  width = 9,
  height = 5,
  dpi = 300
)

sdid_dynamic_att_plot <- ggplot(sdid_paths, aes(x = period_date, y = dynamic_att)) +
  geom_ribbon(aes(ymin = ci_lower, ymax = ci_upper), fill = "#92C5DE", alpha = 0.35) +
  geom_hline(yintercept = 0, color = "grey45", linewidth = 0.35) +
  geom_vline(xintercept = shock_date, color = "black", linetype = "dashed", linewidth = 0.5) +
  geom_line(color = "#2166AC", linewidth = 0.9) +
  geom_point(aes(shape = post), size = 1.7, color = "#2166AC") +
  theme_minimal(base_size = 13) +
  theme(legend.position = "bottom", panel.grid.minor = element_blank()) +
  labs(
    title = "SDID Dynamic ATT with Placebo 95% CI, Full Post Period",
    x = "Quarter",
    y = "Log-point ATT",
    shape = "Post-Ian"
  )

ggsave(
  filename = file.path(estimation_output_dir, "06b_sdid_dynamic_att_placebo_ci.png"),
  plot = sdid_dynamic_att_plot,
  width = 9,
  height = 5,
  dpi = 300
)

# 4.4 Diagnostic plots for restricted post-treatment samples ---------------------------

plot_twfe_horizon_diagnostics <- function(data, horizon_title, file_suffix) {
  twfe_dynamic <- feols(
    log_remittances ~ i(period_date, is_treated, ref = as.Date("2022-07-01")) | unit_id + period_date,
    data = data,
    cluster = ~unit_id
  )

  png(file.path(estimation_output_dir, paste0("twfe_event_study_", file_suffix, ".png")), width = 2400, height = 1500, res = 300)
  iplot(
    twfe_dynamic,
    main = paste("Event Study: Impact of Hurricane Ian on Florida Remittances,", horizon_title),
    xlab = "Quarter",
    ylab = "Log Difference in Remittances",
    ylim = c(-2, 2)
  )
  dev.off()
}

plot_sc_horizon_diagnostics <- function(data, horizon_title, file_suffix) {
  synth_data <- data %>%
    mutate(
      state_id = as.numeric(factor(unit_id)),
      time_id = as.numeric(factor(period_date, levels = sort(unique(period_date))))
    ) %>%
    arrange(state_id, time_id)

  florida_id <- synth_data %>% filter(unit_id == "Florida") %>% pull(state_id) %>% unique()
  control_ids <- synth_data %>% filter(unit_id != "Florida") %>% pull(state_id) %>% unique()
  pre_period <- synth_data %>% filter(period_date < shock_date) %>% pull(time_id) %>% unique()
  all_period <- sort(unique(synth_data$time_id))
  all_dates <- sort(unique(synth_data$period_date))
  all_state_ids <- sort(unique(synth_data$state_id))
  state_lookup <- synth_data %>% distinct(state_id, unit_id)

  dataprep_florida <- dataprep(
    foo = synth_data,
    predictors = c("log_remittances"),
    predictors.op = "mean",
    dependent = "log_remittances",
    unit.variable = "state_id",
    unit.names.variable = "unit_id",
    time.variable = "time_id",
    treatment.identifier = florida_id,
    controls.identifier = control_ids,
    time.predictors.prior = pre_period,
    time.optimize.ssr = pre_period,
    time.plot = all_period
  )

  synth_florida <- synth(dataprep_florida, verbose = FALSE)

  run_synth_placebo <- function(treated_state_id) {
    treated_unit_name <- state_lookup %>%
      filter(state_id == treated_state_id) %>%
      pull(unit_id)

    control_ids_placebo <- synth_data %>%
      filter(
        state_id != treated_state_id,
        unit_id != "Florida" | treated_unit_name == "Florida"
      ) %>%
      pull(state_id) %>%
      unique()

    dp <- dataprep(
      foo = synth_data,
      predictors = c("log_remittances"),
      predictors.op = "mean",
      dependent = "log_remittances",
      unit.variable = "state_id",
      unit.names.variable = "unit_id",
      time.variable = "time_id",
      treatment.identifier = treated_state_id,
      controls.identifier = control_ids_placebo,
      time.predictors.prior = pre_period,
      time.optimize.ssr = pre_period,
      time.plot = all_period
    )

    so <- synth(dp, verbose = FALSE)

    tibble(
      treated_state_id = treated_state_id,
      time_id = all_period,
      period_date = all_dates,
      gap = as.numeric(dp$Y1plot - dp$Y0plot %*% so$solution.w)
    )
  }

  placebo_gaps <- map_dfr(all_state_ids, possibly(run_synth_placebo, otherwise = NULL)) %>%
    left_join(state_lookup, by = c("treated_state_id" = "state_id"))

  florida_gap <- placebo_gaps %>% filter(unit_id == "Florida")

  placebo_band <- placebo_gaps %>%
    filter(unit_id != "Florida") %>%
    group_by(period_date) %>%
    summarise(
      ci_lower = quantile(gap, 0.025, na.rm = TRUE),
      ci_upper = quantile(gap, 0.975, na.rm = TRUE),
      .groups = "drop"
    )

  png(file.path(estimation_output_dir, paste0("sc_path_", file_suffix, ".png")), width = 2400, height = 1500, res = 300)
  path.plot(
    synth.res = synth_florida,
    dataprep.res = dataprep_florida,
    Ylab = "Log remittances",
    Xlab = "Quarter",
    Legend = c("Florida", "Synthetic Florida"),
    Main = paste("SC: Florida vs Synthetic Florida,", horizon_title)
  )
  abline(v = max(pre_period), lty = 2)
  dev.off()

  png(file.path(estimation_output_dir, paste0("sc_gap_", file_suffix, ".png")), width = 2400, height = 1500, res = 300)
  gaps.plot(
    synth.res = synth_florida,
    dataprep.res = dataprep_florida,
    Ylab = "Florida - Synthetic Florida",
    Xlab = "Quarter",
    Main = paste("SC Gap,", horizon_title)
  )
  abline(v = max(pre_period), lty = 2)
  dev.off()

  sc_gap_band <- ggplot() +
    geom_ribbon(
      data = placebo_band,
      aes(x = period_date, ymin = ci_lower, ymax = ci_upper),
      fill = "grey75",
      alpha = 0.45
    ) +
    geom_hline(yintercept = 0, color = "grey45", linewidth = 0.35) +
    geom_vline(xintercept = shock_date, color = "black", linetype = "dashed", linewidth = 0.5) +
    geom_line(data = florida_gap, aes(x = period_date, y = gap), color = "#B2182B", linewidth = 0.9) +
    theme_minimal(base_size = 13) +
    theme(panel.grid.minor = element_blank()) +
    labs(
      title = paste("SC Gap with Placebo Quantile Bands,", horizon_title),
      x = "Quarter",
      y = "Log-point gap"
    )

  ggsave(
    filename = file.path(estimation_output_dir, paste0("sc_gap_placebo_bands_", file_suffix, ".png")),
    plot = sc_gap_band,
    width = 9,
    height = 5,
    dpi = 300
  )

  sc_placebos <- ggplot(placebo_gaps, aes(x = period_date, y = gap, group = unit_id)) +
    geom_hline(yintercept = 0, color = "grey45", linewidth = 0.35) +
    geom_vline(xintercept = shock_date, color = "black", linetype = "dashed", linewidth = 0.5) +
    geom_line(data = filter(placebo_gaps, unit_id != "Florida"), color = "grey70", linewidth = 0.35, alpha = 0.65) +
    geom_line(data = filter(placebo_gaps, unit_id == "Florida"), color = "#B2182B", linewidth = 1) +
    theme_minimal(base_size = 13) +
    theme(panel.grid.minor = element_blank()) +
    labs(
      title = paste("SC Placebo Gaps,", horizon_title),
      x = "Quarter",
      y = "Log-point gap"
    )

  ggsave(
    filename = file.path(estimation_output_dir, paste0("sc_placebo_gaps_", file_suffix, ".png")),
    plot = sc_placebos,
    width = 9,
    height = 5,
    dpi = 300
  )
}

plot_sdid_horizon_diagnostics <- function(data, horizon_title, file_suffix) {
  sdid_data <- data %>%
    mutate(
      state_id = as.numeric(factor(unit_id)),
      time_id = as.numeric(factor(period_date, levels = sort(unique(period_date)))),
      treated = ifelse(unit_id == "Florida" & period_date >= shock_date, 1, 0)
    ) %>%
    arrange(state_id, time_id)

  panel_sdid <- panel.matrices(
    sdid_data,
    unit = "state_id",
    time = "time_id",
    outcome = "log_remittances",
    treatment = "treated"
  )

  sdid_est <- synthdid_estimate(
    Y = panel_sdid$Y,
    N0 = panel_sdid$N0,
    T0 = panel_sdid$T0
  )

  set.seed(2024)
  # Placebo draws are taken from the SDID control block, so Florida is not used as a placebo control.
  sdid_placebo_se <- as.numeric(sqrt(vcov(
    sdid_est,
    method = "placebo",
    replications = sdid_placebo_replications
  )))

  sdid_weights <- attr(sdid_est, "weights")
  sdid_omega <- as.numeric(sdid_weights$omega)
  sdid_lambda <- as.numeric(sdid_weights$lambda)

  time_lookup <- sdid_data %>% distinct(time_id, period_date) %>% arrange(time_id)
  treated_rows <- (panel_sdid$N0 + 1):nrow(panel_sdid$Y)
  control_matrix <- panel_sdid$Y[seq_len(panel_sdid$N0), , drop = FALSE]

  sdid_paths_horizon <- tibble(
    period_date = time_lookup$period_date,
    post = period_date >= shock_date,
    florida = as.numeric(colMeans(panel_sdid$Y[treated_rows, , drop = FALSE])),
    synthetic_florida = as.numeric(sdid_omega %*% control_matrix)
  ) %>%
    mutate(gap = florida - synthetic_florida)

  pre_adjustment <- sum(sdid_lambda * sdid_paths_horizon$gap[seq_len(panel_sdid$T0)])

  sdid_paths_horizon <- sdid_paths_horizon %>%
    mutate(
      dynamic_att = gap - pre_adjustment,
      ci_lower = dynamic_att - 1.96 * sdid_placebo_se,
      ci_upper = dynamic_att + 1.96 * sdid_placebo_se
    )

  sdid_path <- ggplot(sdid_paths_horizon, aes(x = period_date)) +
    geom_vline(xintercept = shock_date, color = "black", linetype = "dashed", linewidth = 0.5) +
    geom_line(aes(y = florida, color = "Florida"), linewidth = 0.9) +
    geom_line(aes(y = synthetic_florida, color = "Synthetic Florida"), linewidth = 0.9) +
    theme_minimal(base_size = 13) +
    theme(legend.position = "bottom", panel.grid.minor = element_blank()) +
    labs(
      title = paste("SDID: Florida vs Synthetic Florida,", horizon_title),
      x = "Quarter",
      y = "Log remittances",
      color = NULL
    )

  ggsave(
    filename = file.path(estimation_output_dir, paste0("sdid_florida_vs_synthetic_", file_suffix, ".png")),
    plot = sdid_path,
    width = 9,
    height = 5,
    dpi = 300
  )

  sdid_gap <- ggplot(sdid_paths_horizon, aes(x = period_date, y = gap)) +
    geom_hline(yintercept = 0, color = "grey45", linewidth = 0.35) +
    geom_vline(xintercept = shock_date, color = "black", linetype = "dashed", linewidth = 0.5) +
    geom_line(linewidth = 0.8) +
    geom_point(aes(shape = post), size = 1.7) +
    theme_minimal(base_size = 13) +
    theme(legend.position = "bottom", panel.grid.minor = element_blank()) +
    labs(
      title = paste("SDID Gap: Florida minus Synthetic Florida,", horizon_title),
      x = "Quarter",
      y = "Log-point gap",
      shape = "Post-Ian"
    )

  ggsave(
    filename = file.path(estimation_output_dir, paste0("sdid_gap_path_", file_suffix, ".png")),
    plot = sdid_gap,
    width = 9,
    height = 5,
    dpi = 300
  )

  sdid_dynamic_att <- ggplot(sdid_paths_horizon, aes(x = period_date, y = dynamic_att)) +
    geom_ribbon(aes(ymin = ci_lower, ymax = ci_upper), fill = "#92C5DE", alpha = 0.35) +
    geom_hline(yintercept = 0, color = "grey45", linewidth = 0.35) +
    geom_vline(xintercept = shock_date, color = "black", linetype = "dashed", linewidth = 0.5) +
    geom_line(color = "#2166AC", linewidth = 0.9) +
    geom_point(aes(shape = post), size = 1.7, color = "#2166AC") +
    theme_minimal(base_size = 13) +
    theme(legend.position = "bottom", panel.grid.minor = element_blank()) +
    labs(
      title = paste("SDID Dynamic ATT with Placebo 95% CI,", horizon_title),
      x = "Quarter",
      y = "Log-point ATT",
      shape = "Post-Ian"
    )

  ggsave(
    filename = file.path(estimation_output_dir, paste0("sdid_dynamic_att_placebo_ci_", file_suffix, ".png")),
    plot = sdid_dynamic_att,
    width = 9,
    height = 5,
    dpi = 300
  )
}

restricted_horizon_plot_specs <- horizon_specs %>%
  filter(!map_lgl(max_post_periods, is.null)) %>%
  mutate(
    file_suffix = paste0("first_", unlist(max_post_periods), "_post_periods"),
    horizon_title = paste0("First ", unlist(max_post_periods), " Post-Treatment Periods")
  )

pwalk(
  restricted_horizon_plot_specs,
  function(horizon, max_post_periods, file_suffix, horizon_title) {
    horizon_data <- make_horizon_data(reg_data_full, max_post_periods)

    plot_twfe_horizon_diagnostics(horizon_data, horizon_title, file_suffix)
    plot_sc_horizon_diagnostics(horizon_data, horizon_title, file_suffix)
    plot_sdid_horizon_diagnostics(horizon_data, horizon_title, file_suffix)
  }
)

# -------------------------------------------------------------------------------------
# 5. SYNTHETIC CONTROL ROBUSTNESS: PRE-RMSPE VALIDITY AND FILTERED PLACEBOS
# -------------------------------------------------------------------------------------

estimate_sc_rmspe_robustness <- function(horizon_label, max_post_periods) {
  rmspe_data <- make_horizon_data(reg_data_full, max_post_periods)

  synth_data_rmspe <- rmspe_data %>%
    mutate(
      state_id = as.numeric(factor(unit_id)),
      time_id = as.numeric(factor(period_date, levels = sort(unique(period_date))))
    ) %>%
    arrange(state_id, time_id)

  pre_period_rmspe <- synth_data_rmspe %>%
    filter(period_date < shock_date) %>%
    pull(time_id) %>%
    unique()

  all_period_rmspe <- sort(unique(synth_data_rmspe$time_id))
  all_dates_rmspe <- sort(unique(synth_data_rmspe$period_date))
  all_state_ids_rmspe <- sort(unique(synth_data_rmspe$state_id))
  state_lookup_rmspe <- synth_data_rmspe %>% distinct(state_id, unit_id)

  run_synth_for_rmspe <- function(treated_state_id) {
    treated_unit_name <- state_lookup_rmspe %>%
      filter(state_id == treated_state_id) %>%
      pull(unit_id)

    control_ids_rmspe <- synth_data_rmspe %>%
      filter(
        state_id != treated_state_id,
        unit_id != "Florida" | treated_unit_name == "Florida"
      ) %>%
      pull(state_id) %>%
      unique()

    dp <- dataprep(
      foo = synth_data_rmspe,
      predictors = c("log_remittances"),
      predictors.op = "mean",
      dependent = "log_remittances",
      unit.variable = "state_id",
      unit.names.variable = "unit_id",
      time.variable = "time_id",
      treatment.identifier = treated_state_id,
      controls.identifier = control_ids_rmspe,
      time.predictors.prior = pre_period_rmspe,
      time.optimize.ssr = pre_period_rmspe,
      time.plot = all_period_rmspe
    )

    so <- synth(dp, verbose = FALSE)

    tibble(
      horizon = horizon_label,
      treated_state_id = treated_state_id,
      time_id = all_period_rmspe,
      period_date = all_dates_rmspe,
      gap = as.numeric(dp$Y1plot - dp$Y0plot %*% so$solution.w)
    )
  }

  placebo_gaps <- map_dfr(
    all_state_ids_rmspe,
    possibly(run_synth_for_rmspe, otherwise = NULL)
  ) %>%
    left_join(state_lookup_rmspe, by = c("treated_state_id" = "state_id"))

  rmspe_results <- placebo_gaps %>%
    group_by(horizon, unit_id) %>%
    summarise(
      pre_rmspe = sqrt(mean(gap[period_date < shock_date]^2, na.rm = TRUE)),
      post_rmspe = sqrt(mean(gap[period_date >= shock_date]^2, na.rm = TRUE)),
      rmspe_ratio = post_rmspe / pre_rmspe,
      avg_post_gap = mean(gap[period_date >= shock_date], na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(horizon, desc(rmspe_ratio))

  florida_rmspe <- rmspe_results %>%
    filter(unit_id == "Florida")

  good_placebo_states <- rmspe_results %>%
    filter(unit_id != "Florida", pre_rmspe <= 2 * florida_rmspe$pre_rmspe) %>%
    pull(unit_id)

  filtered_placebo_effects <- rmspe_results %>%
    filter(unit_id %in% good_placebo_states)

  filtered_placebo_ci <- filtered_placebo_effects %>%
    summarise(
      ci_lower = quantile(avg_post_gap, 0.025, na.rm = TRUE),
      ci_upper = quantile(avg_post_gap, 0.975, na.rm = TRUE)
    )

  filtered_placebo_p_value <- mean(
    abs(filtered_placebo_effects$avg_post_gap) >= abs(florida_rmspe$avg_post_gap),
    na.rm = TRUE
  )

  summary <- tibble(
    horizon = horizon_label,
    method = "Synthetic Control",
    florida_avg_post_gap = florida_rmspe$avg_post_gap,
    florida_percent_effect = 100 * (exp(florida_rmspe$avg_post_gap) - 1),
    florida_pre_rmspe = florida_rmspe$pre_rmspe,
    florida_post_rmspe = florida_rmspe$post_rmspe,
    florida_rmspe_ratio = florida_rmspe$rmspe_ratio,
    retained_placebo_states = length(good_placebo_states),
    filtered_placebo_p_value = filtered_placebo_p_value,
    filtered_ci_lower = filtered_placebo_ci$ci_lower,
    filtered_ci_upper = filtered_placebo_ci$ci_upper,
    filtered_percent_ci_lower = 100 * (exp(filtered_placebo_ci$ci_lower) - 1),
    filtered_percent_ci_upper = 100 * (exp(filtered_placebo_ci$ci_upper) - 1)
  )

  list(
    gaps = placebo_gaps,
    diagnostics = rmspe_results,
    filtered_placebos = filtered_placebo_effects,
    summary = summary
  )
}

sc_rmspe_robustness_by_horizon <- pmap(
  horizon_specs,
  ~ estimate_sc_rmspe_robustness(..1, ..2)
)

sc_rmspe_placebo_gaps_all_horizons <- map_dfr(sc_rmspe_robustness_by_horizon, "gaps")
sc_rmspe_diagnostics_all_horizons <- map_dfr(sc_rmspe_robustness_by_horizon, "diagnostics")
sc_filtered_placebo_effects_all_horizons <- map_dfr(sc_rmspe_robustness_by_horizon, "filtered_placebos")
sc_rmspe_robustness_summary <- map_dfr(sc_rmspe_robustness_by_horizon, "summary") %>%
  mutate(
    across(
      where(is.numeric),
      ~ round(.x, 4)
    )
  )

print(sc_rmspe_robustness_summary)

write.csv(
  sc_rmspe_placebo_gaps_all_horizons,
  file.path(estimation_output_dir, "sc_rmspe_placebo_gaps_all_horizons.csv"),
  row.names = FALSE
)

write.csv(
  sc_rmspe_diagnostics_all_horizons,
  file.path(estimation_output_dir, "sc_rmspe_placebo_diagnostics_all_horizons.csv"),
  row.names = FALSE
)

write.csv(
  sc_filtered_placebo_effects_all_horizons,
  file.path(estimation_output_dir, "sc_filtered_placebo_effects_pre_rmspe_2x_all_horizons.csv"),
  row.names = FALSE
)

write.csv(
  sc_rmspe_robustness_summary,
  file.path(estimation_output_dir, "sc_rmspe_robustness_summary_all_horizons.csv"),
  row.names = FALSE
)
