# Hurricane Ian and US-to-Mexico remittances
#
# Clean analysis file:
# - removes stale Ian outputs at the start of each run;
# - uses log(1 + remittances_usd);
# - excludes the two largest pre-Ian Florida receiver municipalities;
# - reports TWFE, corridor, matched-corridor, DRDID, and BJS imputation diagnostics;
# - estimates shift-share exposure and synthetic difference-in-differences designs;
# - reports HonestDiD sensitivity bounds for the TWFE event study.

suppressPackageStartupMessages({
  library(data.table)
  library(fixest)
  library(ggplot2)
})

options(scipen = 999)
setFixest_notes(FALSE)

# Paths -----------------------------------------------------------------------

analysis_dir <- "Shocks/Ian hurricane"
data_file <- "1_network_estimation/4_remittance_calibration/output/calibrated_remittance_flows_master_2013q1_2024q4_usd.csv"

output_dir <- file.path(analysis_dir, "outputs")
plot_dir <- file.path(output_dir, "plots")
table_dir <- file.path(output_dir, "tables")

if (dir.exists(output_dir)) {
  unlink(output_dir, recursive = TRUE, force = TRUE)
}
dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(table_dir, showWarnings = FALSE, recursive = TRUE)

# Parameters ------------------------------------------------------------------

hit_q <- (2022L - 2013L) * 4L + 4L
event_window <- -8L:8L
event_ref <- -1L
pretrend_window <- setdiff(event_window[event_window < 0L], event_ref)
post_event_window <- event_window[event_window >= 0L]
honest_mbar_grid <- c(0, 0.5, 1, 1.5, 2)
corridor_controls_per_receiver <- 5L

# Load and prepare data --------------------------------------------------------

df <- fread(data_file)

df[
  ,
  `:=`(
    pair_id = paste(us_state, mx_state, mx_municipality, sep = "_"),
    receiver_id = paste(mx_state, mx_municipality, sep = " - "),
    is_treated = as.integer(us_state == "Florida"),
    q_num = (year - 2013L) * 4L + quarter,
    log1p_remittances = log1p(remittances_usd)
  )
]

df[
  ,
  `:=`(
    time_to_treat = q_num - hit_q,
    is_post = as.integer(q_num >= hit_q),
    treated_post = as.integer(us_state == "Florida" & q_num >= hit_q)
  )
]

excluded_receivers <- df[
  q_num < hit_q & us_state == "Florida",
  .(pre_florida_usd = sum(remittances_usd, na.rm = TRUE)),
  by = .(receiver_id, mx_state, mx_municipality)
][order(-pre_florida_usd)][1:2]

fwrite(excluded_receivers, file.path(table_dir, "excluded_top_receivers.csv"))

df <- df[!receiver_id %chin% excluded_receivers$receiver_id]
df[, receiver_period_id := .GRP, by = .(receiver_id, year_quarter)]
df_window <- df[time_to_treat %in% event_window]

# Helpers ---------------------------------------------------------------------

tidy_fixest <- function(model) {
  estimates <- as.data.table(coeftable(model), keep.rownames = "term")
  setnames(
    estimates,
    names(estimates),
    c("term", "estimate", "std.error", "statistic", "p.value")
  )

  intervals <- as.data.table(confint(model), keep.rownames = "term")
  setnames(intervals, names(intervals), c("term", "conf.low", "conf.high"))

  merge(estimates, intervals, by = "term", all.x = TRUE, sort = FALSE)
}

quarter_label <- function(time_to_treat) {
  q_num <- hit_q + time_to_treat
  quarter <- ((q_num - 1L) %% 4L) + 1L
  year <- 2013L + (q_num - 1L) %/% 4L
  paste0(year, " Q", quarter)
}

theme_event <- function() {
  theme_minimal(base_size = 12) +
    theme(
      legend.position = "top",
      panel.grid.minor = element_blank(),
      plot.title.position = "plot"
    )
}

extract_event_results <- function(model, interaction_var = "is_treated") {
  event_results <- tidy_fixest(model)
  event_results <- event_results[grepl("^time_to_treat::", term)]
  term_pattern <- paste0(
    "^time_to_treat::(-?[0-9]+):",
    interaction_var,
    "$"
  )
  event_results[
    ,
    time_to_treat := as.integer(
      sub(term_pattern, "\\1", term)
    )
  ]
  event_results[, calendar_quarter := quarter_label(time_to_treat)]
  event_results[, is_reference_period := FALSE]
  event_results[
    ,
    `:=`(
      effect_pct = 100 * (exp(estimate) - 1),
      conf.low_pct = 100 * (exp(conf.low) - 1),
      conf.high_pct = 100 * (exp(conf.high) - 1)
    )
  ]

  reference_row <- data.table(
    term = "Reference period",
    estimate = 0,
    std.error = NA_real_,
    statistic = NA_real_,
    p.value = NA_real_,
    conf.low = 0,
    conf.high = 0,
    time_to_treat = event_ref,
    calendar_quarter = quarter_label(event_ref),
    is_reference_period = TRUE,
    effect_pct = 0,
    conf.low_pct = 0,
    conf.high_pct = 0
  )

  event_results <- rbindlist(list(event_results, reference_row), fill = TRUE)
  setorder(event_results, time_to_treat)
  event_results
}

run_pretrend_test <- function(model, model_label, interaction_var = "is_treated") {
  keep_pattern <- paste0(
    paste0("time_to_treat::", pretrend_window, ":", interaction_var),
    collapse = "|"
  )
  test <- tryCatch(
    wald(model, keep = keep_pattern, print = FALSE),
    error = function(e) {
      structure(list(error_message = conditionMessage(e)), class = "wald_error")
    }
  )

  if (inherits(test, "wald_error")) {
    return(data.table(
      model = model_label,
      tested_relative_quarters = paste(pretrend_window, collapse = ", "),
      tested_coef_count = NA_integer_,
      statistic = NA_real_,
      p.value = NA_real_,
      df1 = NA_real_,
      df2 = NA_real_,
      error_message = test$error_message
    ))
  }

  data.table(
    model = model_label,
    tested_relative_quarters = paste(pretrend_window, collapse = ", "),
    tested_coef_count = length(pretrend_window),
    statistic = test$stat,
    p.value = test$p,
    df1 = test$df1,
    df2 = test$df2,
    error_message = NA_character_
  )
}

plot_event_study <- function(plot_data, filename, title, subtitle, y_label) {
  plot_data <- copy(plot_data)
  if (!"is_reference_period" %in% names(plot_data)) {
    plot_data[, is_reference_period := time_to_treat == event_ref]
  }

  event_plot <- ggplot(plot_data, aes(x = time_to_treat, y = estimate)) +
    geom_hline(yintercept = 0, color = "grey45", linewidth = 0.35) +
    geom_vline(xintercept = -0.5, linetype = "dashed", color = "grey35") +
    geom_linerange(
      aes(ymin = conf.low, ymax = conf.high),
      color = "#4B5563",
      linewidth = 0.4
    ) +
    geom_line(color = "#2563EB", linewidth = 0.55) +
    geom_point(
      aes(shape = is_reference_period),
      color = "#111827",
      fill = "white",
      size = 1.9,
      stroke = 0.65
    ) +
    scale_shape_manual(values = c("FALSE" = 16, "TRUE" = 21), guide = "none") +
    scale_x_continuous(breaks = seq(min(event_window), max(event_window), by = 2L)) +
    labs(
      title = title,
      subtitle = subtitle,
      x = "Quarters relative to Hurricane Ian (0 = 2022 Q4)",
      y = y_label
    ) +
    theme_event()

  ggsave(
    filename = file.path(plot_dir, filename),
    plot = event_plot,
    width = 9,
    height = 5,
    dpi = 300
  )

  event_plot
}

# TWFE event study -------------------------------------------------------------

twfe_static <- feols(
  log1p_remittances ~ treated_post | pair_id + year_quarter,
  data = df_window,
  cluster = ~ pair_id
)

twfe_static_results <- tidy_fixest(twfe_static)
twfe_static_results[
  term == "treated_post",
  `:=`(
    effect_pct = 100 * (exp(estimate) - 1),
    conf.low_pct = 100 * (exp(conf.low) - 1),
    conf.high_pct = 100 * (exp(conf.high) - 1)
  )
]
fwrite(twfe_static_results, file.path(table_dir, "twfe_static_log_results.csv"))

twfe_event <- feols(
  log1p_remittances ~ i(time_to_treat, is_treated, ref = event_ref) |
    pair_id + year_quarter,
  data = df_window,
  cluster = ~ pair_id
)

twfe_event_results <- extract_event_results(twfe_event)
fwrite(twfe_event_results, file.path(table_dir, "twfe_event_study_log_results.csv"))

twfe_pretrend_results <- run_pretrend_test(
  twfe_event,
  model_label = "TWFE log event study, pair and year-quarter FE"
)
fwrite(twfe_pretrend_results, file.path(table_dir, "twfe_event_study_pretrend_test.csv"))

plot_event_study(
  twfe_event_results,
  filename = "twfe_event_study_log.png",
  title = "TWFE event study",
  subtitle = "Outcome is log(1 + remittances_usd); top two pre-Ian Florida receiver municipalities excluded.",
  y_label = "Effect on log(1 + remittances_usd)"
)

# Corridor-level designs -------------------------------------------------------

corridor_twfe_static <- feols(
  log1p_remittances ~ treated_post | pair_id + receiver_period_id,
  data = df_window,
  cluster = ~ pair_id
)

corridor_twfe_static_results <- tidy_fixest(corridor_twfe_static)
corridor_twfe_static_results[
  term == "treated_post",
  `:=`(
    effect_pct = 100 * (exp(estimate) - 1),
    conf.low_pct = 100 * (exp(conf.low) - 1),
    conf.high_pct = 100 * (exp(conf.high) - 1)
  )
]
fwrite(
  corridor_twfe_static_results,
  file.path(table_dir, "corridor_twfe_static_log_results.csv")
)

corridor_twfe_event <- feols(
  log1p_remittances ~ i(time_to_treat, is_treated, ref = event_ref) |
    pair_id + receiver_period_id,
  data = df_window,
  cluster = ~ pair_id
)

corridor_twfe_event_results <- extract_event_results(corridor_twfe_event)
fwrite(
  corridor_twfe_event_results,
  file.path(table_dir, "corridor_twfe_event_study_log_results.csv")
)

corridor_twfe_pretrend_results <- run_pretrend_test(
  corridor_twfe_event,
  model_label = "Corridor TWFE, pair and receiver-quarter FE"
)
fwrite(
  corridor_twfe_pretrend_results,
  file.path(table_dir, "corridor_twfe_event_study_pretrend_test.csv")
)

plot_event_study(
  corridor_twfe_event_results,
  filename = "corridor_twfe_event_study_log.png",
  title = "Corridor TWFE event study",
  subtitle = "Compares Florida corridors to other origins serving the same Mexican receiver-quarter.",
  y_label = "Effect on log(1 + remittances_usd)"
)

florida_pre_path <- df[
  us_state == "Florida" & q_num < hit_q,
  .(receiver_id, q_num, florida_log = log1p_remittances)
]
control_pre_path <- df[
  us_state != "Florida" & q_num < hit_q,
  .(receiver_id, us_state, q_num, control_log = log1p_remittances)
]

corridor_match_quality <- control_pre_path[
  florida_pre_path,
  on = .(receiver_id, q_num),
  nomatch = 0
][
  ,
  .(
    pre_rmse = sqrt(mean((control_log - florida_log)^2, na.rm = TRUE)),
    pre_mae = mean(abs(control_log - florida_log), na.rm = TRUE),
    pre_corr = suppressWarnings(cor(control_log, florida_log, use = "complete.obs"))
  ),
  by = .(receiver_id, us_state)
]
setorder(corridor_match_quality, receiver_id, pre_rmse)
corridor_match_quality[, match_rank := seq_len(.N), by = receiver_id]

selected_corridor_controls <- corridor_match_quality[
  match_rank <= corridor_controls_per_receiver
]
fwrite(
  selected_corridor_controls,
  file.path(table_dir, "selected_corridor_controls.csv")
)

matched_corridor_pairs <- unique(rbindlist(
  list(
    df[us_state == "Florida", .(receiver_id, us_state)],
    selected_corridor_controls[, .(receiver_id, us_state)]
  )
))

matched_corridor_window <- df_window[
  matched_corridor_pairs,
  on = .(receiver_id, us_state),
  nomatch = 0
]

matched_corridor_summary <- data.table(
  treated_corridors = matched_corridor_window[is_treated == 1L, uniqueN(pair_id)],
  control_corridors = matched_corridor_window[is_treated == 0L, uniqueN(pair_id)],
  controls_per_receiver = corridor_controls_per_receiver,
  observations = nrow(matched_corridor_window)
)
fwrite(
  matched_corridor_summary,
  file.path(table_dir, "matched_corridor_summary.csv")
)

matched_corridor_event <- feols(
  log1p_remittances ~ i(time_to_treat, is_treated, ref = event_ref) |
    pair_id + receiver_period_id,
  data = matched_corridor_window,
  cluster = ~ pair_id
)

matched_corridor_event_results <- extract_event_results(matched_corridor_event)
fwrite(
  matched_corridor_event_results,
  file.path(table_dir, "matched_corridor_event_study_log_results.csv")
)

matched_corridor_pretrend_results <- run_pretrend_test(
  matched_corridor_event,
  model_label = "Matched-corridor TWFE, pair and receiver-quarter FE"
)
fwrite(
  matched_corridor_pretrend_results,
  file.path(table_dir, "matched_corridor_event_study_pretrend_test.csv")
)

plot_event_study(
  matched_corridor_event_results,
  filename = "matched_corridor_event_study_log.png",
  title = "Matched-corridor event study",
  subtitle = paste0(
    "For each Florida receiver, controls are the ",
    corridor_controls_per_receiver,
    " closest non-Florida corridors by pre-Ian log path."
  ),
  y_label = "Effect on log(1 + remittances_usd)"
)

# Doubly robust DiD event-study path ------------------------------------------

safe_slope <- function(y, x) {
  complete <- is.finite(y) & is.finite(x)
  if (sum(complete) < 2L || uniqueN(x[complete]) < 2L) {
    return(0)
  }
  as.numeric(coef(lm(y[complete] ~ x[complete]))[[2]])
}

run_drdid_event_study <- function(panel) {
  if (!requireNamespace("DRDID", quietly = TRUE)) {
    return(list(
      event_results = data.table(error_message = "DRDID is not installed."),
      pretrend_results = data.table(error_message = "DRDID is not installed.")
    ))
  }

  covariate_panel <- panel[q_num < hit_q + min(event_window)]
  pair_covariates <- covariate_panel[
    ,
    .(
      pre_mean_log = mean(log1p_remittances, na.rm = TRUE),
      pre_sd_log = sd(log1p_remittances, na.rm = TRUE),
      pre_zero_share = mean(remittances_usd <= 0, na.rm = TRUE),
      pre_slope_log = safe_slope(log1p_remittances, q_num)
    ),
    by = .(pair_id, is_treated)
  ]
  pair_covariates[is.na(pre_sd_log), pre_sd_log := 0]

  event_rows <- vector("list", length(setdiff(event_window, event_ref)))
  inf_list <- list()
  event_index <- 0L

  for (rel_q in setdiff(event_window, event_ref)) {
    event_index <- event_index + 1L
    q0 <- hit_q + event_ref
    q1 <- hit_q + rel_q

    two_period <- panel[
      q_num %in% c(q0, q1),
      .(pair_id, q_num, log1p_remittances)
    ]
    two_period <- dcast(
      two_period,
      pair_id ~ q_num,
      value.var = "log1p_remittances"
    )
    setnames(two_period, c(as.character(q0), as.character(q1)), c("y0", "y1"))
    two_period <- merge(pair_covariates, two_period, by = "pair_id")
    setorder(two_period, pair_id)

    covariates <- as.matrix(cbind(
      intercept = 1,
      two_period[, .(pre_mean_log, pre_sd_log, pre_zero_share, pre_slope_log)]
    ))

    estimate <- tryCatch(
      DRDID::drdid_panel(
        y1 = two_period$y1,
        y0 = two_period$y0,
        D = two_period$is_treated,
        covariates = covariates,
        boot = FALSE,
        inffunc = TRUE
      ),
      error = function(e) {
        structure(list(error_message = conditionMessage(e)), class = "drdid_error")
      }
    )

    if (inherits(estimate, "drdid_error")) {
      event_rows[[event_index]] <- data.table(
        time_to_treat = rel_q,
        calendar_quarter = quarter_label(rel_q),
        estimate = NA_real_,
        std.error = NA_real_,
        statistic = NA_real_,
        p.value = NA_real_,
        conf.low = NA_real_,
        conf.high = NA_real_,
        effect_pct = NA_real_,
        error_message = estimate$error_message
      )
    } else {
      event_rows[[event_index]] <- data.table(
        time_to_treat = rel_q,
        calendar_quarter = quarter_label(rel_q),
        estimate = estimate$ATT,
        std.error = estimate$se,
        statistic = estimate$ATT / estimate$se,
        p.value = 2 * pnorm(-abs(estimate$ATT / estimate$se)),
        conf.low = estimate$lci,
        conf.high = estimate$uci,
        effect_pct = 100 * (exp(estimate$ATT) - 1),
        error_message = NA_character_
      )
      if (rel_q %in% pretrend_window) {
        inf_list[[as.character(rel_q)]] <- data.table(
          pair_id = two_period$pair_id,
          time_to_treat = rel_q,
          inf = as.numeric(estimate$att.inf.func)
        )
      }
    }
  }

  event_results <- rbindlist(event_rows, fill = TRUE)
  reference_row <- data.table(
    time_to_treat = event_ref,
    calendar_quarter = quarter_label(event_ref),
    estimate = 0,
    std.error = NA_real_,
    statistic = NA_real_,
    p.value = NA_real_,
    conf.low = 0,
    conf.high = 0,
    effect_pct = 0,
    error_message = NA_character_
  )
  event_results <- rbindlist(list(event_results, reference_row), fill = TRUE)
  setorder(event_results, time_to_treat)

  if (length(inf_list) == length(pretrend_window)) {
    inf_wide <- Reduce(
      function(x, y) merge(x, y, by = "pair_id"),
      lapply(names(inf_list), function(rel_q) {
        out <- inf_list[[rel_q]]
        setnames(out, "inf", paste0("inf_", rel_q))
        out[, time_to_treat := NULL]
        out
      })
    )
    inf_cols <- paste0("inf_", pretrend_window)
    inf_matrix <- as.matrix(inf_wide[, ..inf_cols])
    inf_matrix <- sweep(inf_matrix, 2, colMeans(inf_matrix), FUN = "-")
    covariance <- crossprod(inf_matrix) / (nrow(inf_matrix)^2)
    pre_estimates <- event_results[
      time_to_treat %in% pretrend_window,
      estimate
    ]
    joint_stat <- tryCatch(
      as.numeric(t(pre_estimates) %*% solve(covariance, pre_estimates)),
      error = function(e) NA_real_
    )
    pretrend_results <- data.table(
      model = "DRDID event-study path, pair-level conditional DiD",
      tested_relative_quarters = paste(pretrend_window, collapse = ", "),
      tested_coef_count = length(pretrend_window),
      statistic = joint_stat,
      p.value = if (is.na(joint_stat)) NA_real_ else pchisq(joint_stat, df = length(pretrend_window), lower.tail = FALSE),
      df1 = length(pretrend_window),
      df2 = NA_real_,
      error_message = NA_character_
    )
  } else {
    pretrend_results <- data.table(
      model = "DRDID event-study path, pair-level conditional DiD",
      tested_relative_quarters = paste(pretrend_window, collapse = ", "),
      tested_coef_count = length(inf_list),
      statistic = NA_real_,
      p.value = NA_real_,
      df1 = NA_real_,
      df2 = NA_real_,
      error_message = "Could not construct all pre-period influence functions."
    )
  }

  list(event_results = event_results, pretrend_results = pretrend_results)
}

drdid_outputs <- run_drdid_event_study(df)
fwrite(
  drdid_outputs$event_results,
  file.path(table_dir, "drdid_event_study_log_results.csv")
)
fwrite(
  drdid_outputs$pretrend_results,
  file.path(table_dir, "drdid_event_study_pretrend_test.csv")
)

plot_event_study(
  drdid_outputs$event_results,
  filename = "drdid_event_study_log.png",
  title = "Doubly robust DiD event-study path",
  subtitle = "Quarter-specific DRDID estimates relative to 2022 Q3, adjusted for pre-window corridor covariates.",
  y_label = "Effect on log(1 + remittances_usd)"
)

# BJS imputation DiD -----------------------------------------------------------

bjs_untreated_model <- feols(
  log1p_remittances ~ 1 | pair_id + receiver_period_id,
  data = df[is_treated == 0L | q_num < hit_q]
)

bjs_window <- copy(df_window)
bjs_window[, untreated_prediction := as.numeric(predict(bjs_untreated_model, newdata = bjs_window))]
bjs_window[, imputed_effect := log1p_remittances - untreated_prediction]

bjs_treated_effects <- bjs_window[
  is_treated == 1L,
  .(
    estimate = mean(imputed_effect, na.rm = TRUE),
    std.error = sd(imputed_effect, na.rm = TRUE) / sqrt(.N),
    treated_corridors = .N
  ),
  by = .(time_to_treat)
]
bjs_treated_effects[
  ,
  `:=`(
    calendar_quarter = quarter_label(time_to_treat),
    statistic = estimate / std.error,
    p.value = 2 * pnorm(-abs(estimate / std.error)),
    conf.low = estimate - 1.96 * std.error,
    conf.high = estimate + 1.96 * std.error,
    effect_pct = 100 * (exp(estimate) - 1),
    is_reference_period = FALSE
  )
]
bjs_treated_effects[
  ,
  `:=`(
    conf.low_pct = 100 * (exp(conf.low) - 1),
    conf.high_pct = 100 * (exp(conf.high) - 1)
  )
]
setorder(bjs_treated_effects, time_to_treat)
fwrite(
  bjs_treated_effects,
  file.path(table_dir, "bjs_imputation_event_study_log_results.csv")
)

bjs_post_pair_effects <- bjs_window[
  is_treated == 1L & time_to_treat %in% post_event_window,
  .(pair_post_effect = mean(imputed_effect, na.rm = TRUE)),
  by = pair_id
]

bjs_static_results <- bjs_post_pair_effects[
  ,
  .(
    term = "Average post imputed effect",
    estimate = mean(pair_post_effect, na.rm = TRUE),
    std.error = sd(pair_post_effect, na.rm = TRUE) / sqrt(.N),
    treated_corridors = .N
  )
]
bjs_static_results[
  ,
  `:=`(
    statistic = estimate / std.error,
    p.value = 2 * pnorm(-abs(estimate / std.error)),
    conf.low = estimate - 1.96 * std.error,
    conf.high = estimate + 1.96 * std.error,
    effect_pct = 100 * (exp(estimate) - 1)
  )
]
bjs_static_results[
  ,
  `:=`(
    conf.low_pct = 100 * (exp(conf.low) - 1),
    conf.high_pct = 100 * (exp(conf.high) - 1)
  )
]
fwrite(
  bjs_static_results,
  file.path(table_dir, "bjs_imputation_static_log_results.csv")
)

bjs_pre_wide <- dcast(
  bjs_window[
    is_treated == 1L & time_to_treat %in% pretrend_window,
    .(pair_id, time_to_treat, imputed_effect)
  ],
  pair_id ~ time_to_treat,
  value.var = "imputed_effect"
)
bjs_pre_cols <- as.character(pretrend_window)
bjs_pre_matrix <- as.matrix(bjs_pre_wide[, ..bjs_pre_cols])
bjs_pre_matrix <- sweep(bjs_pre_matrix, 2, colMeans(bjs_pre_matrix), FUN = "-")
bjs_pre_covariance <- crossprod(bjs_pre_matrix) / (nrow(bjs_pre_matrix)^2)
bjs_pre_estimates <- bjs_treated_effects[
  time_to_treat %in% pretrend_window,
  estimate
]
bjs_pretrend_stat <- tryCatch(
  as.numeric(t(bjs_pre_estimates) %*% solve(bjs_pre_covariance, bjs_pre_estimates)),
  error = function(e) NA_real_
)
bjs_pretrend_results <- data.table(
  model = "BJS imputation, pair and receiver-quarter FE",
  tested_relative_quarters = paste(pretrend_window, collapse = ", "),
  tested_coef_count = length(pretrend_window),
  statistic = bjs_pretrend_stat,
  p.value = if (is.na(bjs_pretrend_stat)) NA_real_ else pchisq(
    bjs_pretrend_stat,
    df = length(pretrend_window),
    lower.tail = FALSE
  ),
  df1 = length(pretrend_window),
  df2 = NA_real_,
  error_message = NA_character_
)
fwrite(
  bjs_pretrend_results,
  file.path(table_dir, "bjs_imputation_pretrend_test.csv")
)

plot_event_study(
  bjs_treated_effects,
  filename = "bjs_imputation_event_study_log.png",
  title = "BJS imputation event study",
  subtitle = "Untreated model fit on non-Florida observations and Florida pre-Ian observations; FE are corridor and receiver-quarter.",
  y_label = "Imputed effect on log(1 + remittances_usd)"
)

# Shift-share/Bartik exposure design ------------------------------------------

receiver_exposure <- df[
  q_num < hit_q,
  .(
    pre_florida_usd = sum(remittances_usd[us_state == "Florida"], na.rm = TRUE),
    pre_all_usd = sum(remittances_usd, na.rm = TRUE)
  ),
  by = .(receiver_id, mx_state, mx_municipality)
]
receiver_exposure[
  ,
  `:=`(
    florida_exposure = fifelse(pre_all_usd > 0, pre_florida_usd / pre_all_usd, 0),
    florida_exposure_10pp = fifelse(pre_all_usd > 0, pre_florida_usd / pre_all_usd / 0.1, 0)
  )
]
fwrite(
  receiver_exposure[order(-florida_exposure)],
  file.path(table_dir, "shiftshare_receiver_exposure.csv")
)

receiver_quarter <- df[
  ,
  .(remittances_usd = sum(remittances_usd, na.rm = TRUE)),
  by = .(receiver_id, mx_state, mx_municipality, q_num, year_quarter, time_to_treat, is_post)
]
receiver_quarter <- merge(
  receiver_quarter,
  receiver_exposure[
    ,
    .(receiver_id, florida_exposure, florida_exposure_10pp)
  ],
  by = "receiver_id"
)
receiver_quarter[
  ,
  `:=`(
    log_total_remittances = log1p(remittances_usd),
    exposure_post_10pp = florida_exposure_10pp * is_post
  )
]
receiver_quarter_window <- receiver_quarter[time_to_treat %in% event_window]

shiftshare_static <- feols(
  log_total_remittances ~ exposure_post_10pp |
    receiver_id + mx_state^year_quarter,
  data = receiver_quarter_window,
  cluster = ~ receiver_id
)

shiftshare_static_results <- tidy_fixest(shiftshare_static)
shiftshare_static_results[
  term == "exposure_post_10pp",
  `:=`(
    effect_pct = 100 * (exp(estimate) - 1),
    conf.low_pct = 100 * (exp(conf.low) - 1),
    conf.high_pct = 100 * (exp(conf.high) - 1)
  )
]
fwrite(
  shiftshare_static_results,
  file.path(table_dir, "shiftshare_static_log_results.csv")
)

shiftshare_event <- feols(
  log_total_remittances ~ i(time_to_treat, florida_exposure_10pp, ref = event_ref) |
    receiver_id + mx_state^year_quarter,
  data = receiver_quarter_window,
  cluster = ~ receiver_id
)

shiftshare_event_results <- extract_event_results(
  shiftshare_event,
  interaction_var = "florida_exposure_10pp"
)
fwrite(
  shiftshare_event_results,
  file.path(table_dir, "shiftshare_event_study_log_results.csv")
)

shiftshare_pretrend_results <- run_pretrend_test(
  shiftshare_event,
  model_label = "Shift-share exposure event study, receiver and Mexican-state-quarter FE",
  interaction_var = "florida_exposure_10pp"
)
fwrite(
  shiftshare_pretrend_results,
  file.path(table_dir, "shiftshare_event_study_pretrend_test.csv")
)

plot_event_study(
  shiftshare_event_results,
  filename = "shiftshare_event_study_log.png",
  title = "Shift-share exposure event study",
  subtitle = "Effect of a 10pp higher pre-Ian Florida remittance share, with receiver and Mexican-state-quarter FE.",
  y_label = "Log-point effect per 10pp higher Florida exposure"
)

# HonestDiD sensitivity bounds -------------------------------------------------

run_honestdid <- function(model) {
  if (!requireNamespace("HonestDiD", quietly = TRUE)) {
    return(data.table(error_message = "HonestDiD is not installed."))
  }

  relative_quarters <- c(pretrend_window, post_event_window)
  coef_terms <- paste0("time_to_treat::", relative_quarters, ":is_treated")
  missing_terms <- setdiff(coef_terms, names(coef(model)))

  if (length(missing_terms) > 0L) {
    return(data.table(
      error_message = paste("Missing event-study coefficients:", paste(missing_terms, collapse = ", "))
    ))
  }

  betahat <- coef(model)[coef_terms]
  sigma <- vcov(model)[coef_terms, coef_terms]
  num_pre <- length(pretrend_window)
  num_post <- length(post_event_window)
  l_vec <- rep(1 / num_post, num_post)
  honest_warnings <- character()

  result <- tryCatch(
    withCallingHandlers(
      as.data.table(
        HonestDiD::createSensitivityResults_relativeMagnitudes(
          betahat = betahat,
          sigma = sigma,
          numPrePeriods = num_pre,
          numPostPeriods = num_post,
          Mbarvec = honest_mbar_grid,
          l_vec = l_vec,
          gridPoints = 1000
        )
      ),
      warning = function(w) {
        honest_warnings <<- c(honest_warnings, conditionMessage(w))
        invokeRestart("muffleWarning")
      }
    ),
    error = function(e) {
      data.table(error_message = conditionMessage(e))
    }
  )

  result[
    ,
    `:=`(
      estimand = paste0("Average post-treatment effect across quarters 0 to ", max(post_event_window)),
      warning_message = if (length(honest_warnings) == 0L) {
        NA_character_
      } else {
        paste(unique(honest_warnings), collapse = " | ")
      }
    )
  ]
  result
}

honestdid_bounds <- run_honestdid(twfe_event)
fwrite(honestdid_bounds, file.path(table_dir, "honestdid_relative_magnitude_bounds.csv"))

if (!"error_message" %in% names(honestdid_bounds)) {
  honest_plot <- ggplot(honestdid_bounds, aes(x = Mbar)) +
    geom_hline(yintercept = 0, color = "grey45", linewidth = 0.35) +
    geom_linerange(aes(ymin = lb, ymax = ub), color = "#7C2D12", linewidth = 0.8) +
    geom_point(aes(y = (lb + ub) / 2), color = "#7C2D12", size = 2) +
    labs(
      title = "HonestDiD sensitivity bounds",
      subtitle = "Relative-magnitude bounds for the average TWFE post-Ian effect.",
      x = "Mbar: allowed post-period violation relative to pre-period violations",
      y = "Robust confidence interval, log points"
    ) +
    theme_event()

  ggsave(
    filename = file.path(plot_dir, "honestdid_relative_magnitude_bounds.png"),
    plot = honest_plot,
    width = 8,
    height = 4.8,
    dpi = 300
  )
}

# Synthetic difference-in-differences style design -----------------------------

fit_simplex_weights <- function(X, target, ridge = 1e-6) {
  X <- as.matrix(X)
  target <- as.numeric(target)
  n_features <- nrow(X)
  n_weights <- ncol(X)

  X_centered <- sweep(X, 2, colMeans(X), FUN = "-")
  target_centered <- target - mean(target)

  if (requireNamespace("osqp", quietly = TRUE) && requireNamespace("Matrix", quietly = TRUE)) {
    P <- 2 * (crossprod(X_centered) / n_features + ridge * diag(n_weights))
    q <- as.numeric(-2 * crossprod(X_centered, target_centered) / n_features)
    A <- rbind(
      Matrix::Matrix(rep(1, n_weights), nrow = 1, sparse = TRUE),
      Matrix::Diagonal(n_weights)
    )
    l <- c(1, rep(0, n_weights))
    u <- c(1, rep(Inf, n_weights))

    fit <- tryCatch(
      osqp::solve_osqp(
        P = methods::as(P, "dgCMatrix"),
        q = q,
        A = methods::as(A, "dgCMatrix"),
        l = l,
        u = u,
        pars = osqp::osqpSettings(verbose = FALSE, eps_abs = 1e-8, eps_rel = 1e-8)
      ),
      error = function(e) NULL
    )

    if (!is.null(fit) && fit$info$status %in% c("solved", "solved inaccurate")) {
      weights <- pmax(as.numeric(fit$x), 0)
      if (sum(weights) > 0) {
        weights <- weights / sum(weights)
      } else {
        weights <- rep(1 / n_weights, n_weights)
      }
      intercept <- mean(target - as.numeric(X %*% weights))
      objective <- mean((target - intercept - as.numeric(X %*% weights))^2)
      return(list(
        weights = weights,
        intercept = intercept,
        objective = objective,
        solver = paste("osqp", fit$info$status)
      ))
    }
  }

  softmax <- function(theta) {
    shifted <- theta - max(theta)
    exp_shifted <- exp(shifted)
    exp_shifted / sum(exp_shifted)
  }
  objective <- function(theta) {
    weights <- softmax(theta)
    intercept <- mean(target - as.numeric(X %*% weights))
    mean((target - intercept - as.numeric(X %*% weights))^2) + ridge * sum(weights^2)
  }
  opt <- optim(
    par = rep(0, n_weights),
    fn = objective,
    method = "BFGS",
    control = list(maxit = 1000, reltol = 1e-10)
  )
  weights <- softmax(opt$par)
  intercept <- mean(target - as.numeric(X %*% weights))

  list(
    weights = weights,
    intercept = intercept,
    objective = objective(opt$par),
    solver = paste("optim convergence", opt$convergence)
  )
}

fit_sdid <- function(Y, q_index, treated_state, control_states, hit_q) {
  treated_index <- match(treated_state, rownames(Y))
  control_indices <- match(control_states, rownames(Y))
  pre_cols <- which(q_index < hit_q)
  post_cols <- which(q_index >= hit_q)

  treated_pre <- Y[treated_index, pre_cols]
  control_pre <- Y[control_indices, pre_cols, drop = FALSE]
  control_post_average <- rowMeans(Y[control_indices, post_cols, drop = FALSE])

  unit_fit <- fit_simplex_weights(
    X = t(control_pre),
    target = treated_pre
  )
  time_fit <- fit_simplex_weights(
    X = control_pre,
    target = control_post_average
  )

  omega <- unit_fit$weights
  lambda <- time_fit$weights
  synthetic_path <- as.numeric(omega %*% Y[control_indices, , drop = FALSE]) +
    unit_fit$intercept
  treated_path <- as.numeric(Y[treated_index, ])
  gap <- treated_path - synthetic_path
  baseline_gap <- sum(lambda * gap[pre_cols])
  event_effect <- gap - baseline_gap
  tau <- mean(event_effect[post_cols])

  list(
    tau = tau,
    baseline_gap = baseline_gap,
    treated_state = treated_state,
    control_states = control_states,
    q_index = q_index,
    treated_path = treated_path,
    synthetic_path = synthetic_path,
    gap = gap,
    event_effect = event_effect,
    omega = omega,
    lambda = lambda,
    unit_intercept = unit_fit$intercept,
    unit_objective = unit_fit$objective,
    unit_solver = unit_fit$solver,
    time_intercept = time_fit$intercept,
    time_objective = time_fit$objective,
    time_solver = time_fit$solver
  )
}

state_quarter <- df[
  ,
  .(remittances_usd = sum(remittances_usd, na.rm = TRUE)),
  by = .(us_state, q_num, year_quarter, time_to_treat)
]
state_quarter[, log_total_remittances := log1p(remittances_usd)]
state_quarter[, q_col := paste0("q", q_num)]

q_index <- sort(unique(state_quarter$q_num))
q_cols <- paste0("q", q_index)
state_wide <- dcast(
  state_quarter,
  us_state ~ q_col,
  value.var = "log_total_remittances"
)
setorder(state_wide, us_state)

Y <- as.matrix(state_wide[, ..q_cols])
rownames(Y) <- state_wide$us_state

if (anyNA(Y)) {
  stop("State-quarter matrix has missing values; SDID requires a balanced panel.")
}

donor_states <- setdiff(rownames(Y), "Florida")
sdid_fit <- fit_sdid(
  Y = Y,
  q_index = q_index,
  treated_state = "Florida",
  control_states = donor_states,
  hit_q = hit_q
)

sdid_summary <- data.table(
  estimator = "Synthetic difference-in-differences style",
  treated_state = "Florida",
  outcome = "log(1 + total remittances_usd)",
  top_receiver_exclusions = paste(excluded_receivers$receiver_id, collapse = "; "),
  donor_states = length(donor_states),
  pre_periods = sum(q_index < hit_q),
  post_periods = sum(q_index >= hit_q),
  estimate_log_points = sdid_fit$tau,
  estimate_pct = 100 * (exp(sdid_fit$tau) - 1),
  baseline_gap = sdid_fit$baseline_gap,
  unit_weight_solver = sdid_fit$unit_solver,
  unit_weight_objective = sdid_fit$unit_objective,
  time_weight_solver = sdid_fit$time_solver,
  time_weight_objective = sdid_fit$time_objective,
  synthdid_package_available = requireNamespace("synthdid", quietly = TRUE)
)
fwrite(sdid_summary, file.path(table_dir, "sdid_summary.csv"))

sdid_unit_weights <- data.table(
  us_state = donor_states,
  unit_weight = sdid_fit$omega
)[order(-unit_weight)]
fwrite(sdid_unit_weights, file.path(table_dir, "sdid_unit_weights.csv"))

sdid_time_weights <- data.table(
  q_num = q_index[q_index < hit_q],
  time_to_treat = q_index[q_index < hit_q] - hit_q,
  calendar_quarter = quarter_label(q_index[q_index < hit_q] - hit_q),
  time_weight = sdid_fit$lambda
)[order(time_to_treat)]
fwrite(sdid_time_weights, file.path(table_dir, "sdid_time_weights.csv"))

sdid_paths <- data.table(
  q_num = q_index,
  time_to_treat = q_index - hit_q,
  calendar_quarter = quarter_label(q_index - hit_q),
  florida_log_total = sdid_fit$treated_path,
  synthetic_log_total = sdid_fit$synthetic_path,
  gap = sdid_fit$gap,
  event_effect = sdid_fit$event_effect,
  post_ian = q_index >= hit_q
)
fwrite(sdid_paths, file.path(table_dir, "sdid_paths.csv"))

sdid_event_results <- sdid_paths[
  time_to_treat %in% event_window,
  .(
    time_to_treat,
    calendar_quarter,
    estimate = event_effect,
    effect_pct = 100 * (exp(event_effect) - 1),
    post_ian
  )
]
fwrite(sdid_event_results, file.path(table_dir, "sdid_event_study_log_results.csv"))

placebo_results <- rbindlist(
  lapply(
    donor_states,
    function(placebo_state) {
      placebo_controls <- setdiff(donor_states, placebo_state)
      placebo_fit <- fit_sdid(
        Y = Y,
        q_index = q_index,
        treated_state = placebo_state,
        control_states = placebo_controls,
        hit_q = hit_q
      )
      data.table(
        placebo_state = placebo_state,
        estimate_log_points = placebo_fit$tau,
        estimate_pct = 100 * (exp(placebo_fit$tau) - 1)
      )
    }
  )
)
placebo_results[
  ,
  `:=`(
    florida_estimate_log_points = sdid_fit$tau,
    abs_placebo_ge_florida = abs(estimate_log_points) >= abs(sdid_fit$tau)
  )
]
placebo_p_value <- (sum(placebo_results$abs_placebo_ge_florida) + 1) /
  (nrow(placebo_results) + 1)
placebo_results[, placebo_p_value := placebo_p_value]
fwrite(placebo_results, file.path(table_dir, "sdid_placebo_results.csv"))

sdid_design_notes <- data.table(
  item = c(
    "Paper",
    "Implementation",
    "Outcome",
    "Unit weights",
    "Time weights",
    "Event-study path",
    "Inference"
  ),
  value = c(
    "Arkhangelsky et al. (2021), Synthetic Difference-in-Differences",
    "Self-contained SDID-style estimator because synthdid is unavailable for this R version",
    "State-quarter log(1 + total remittances_usd), with top two pre-Ian Florida receiver municipalities excluded",
    "Simplex weights on donor states chosen to match Florida's pre-Ian path up to an intercept",
    "Simplex weights on pre-Ian quarters chosen to match donor states' post-Ian averages up to an intercept",
    "Florida minus synthetic-control gap, centered by the SDID time-weighted pre-Ian gap",
    "Donor-state placebo distribution; HonestDiD is reported separately for TWFE event-study sensitivity"
  )
)
fwrite(sdid_design_notes, file.path(table_dir, "sdid_design_notes.csv"))

sdid_plot_data <- melt(
  sdid_paths[time_to_treat %between% c(min(event_window), max(event_window))],
  id.vars = c("time_to_treat", "calendar_quarter", "post_ian"),
  measure.vars = c("florida_log_total", "synthetic_log_total"),
  variable.name = "series",
  value.name = "log_total_remittances"
)
sdid_plot_data[
  ,
  series := fifelse(
    series == "florida_log_total",
    "Florida",
    "Synthetic control"
  )
]

sdid_path_plot <- ggplot(
  sdid_plot_data,
  aes(x = time_to_treat, y = log_total_remittances, color = series)
) +
  geom_vline(xintercept = -0.5, linetype = "dashed", color = "grey35") +
  geom_line(linewidth = 0.8) +
  geom_point(size = 1.5) +
  scale_color_manual(values = c("Florida" = "#B91C1C", "Synthetic control" = "#1D4ED8")) +
  scale_x_continuous(breaks = seq(min(event_window), max(event_window), by = 2L)) +
  labs(
    title = "SDID path comparison",
    subtitle = "Florida versus synthetic control in state-quarter log total remittances.",
    x = "Quarters relative to Hurricane Ian (0 = 2022 Q4)",
    y = "log(1 + total remittances_usd)",
    color = NULL
  ) +
  theme_event()

ggsave(
  filename = file.path(plot_dir, "sdid_florida_vs_synthetic_path.png"),
  plot = sdid_path_plot,
  width = 9,
  height = 5,
  dpi = 300
)

sdid_event_plot <- ggplot(
  sdid_event_results,
  aes(x = time_to_treat, y = estimate)
) +
  geom_hline(yintercept = 0, color = "grey45", linewidth = 0.35) +
  geom_vline(xintercept = -0.5, linetype = "dashed", color = "grey35") +
  geom_line(color = "#0F766E", linewidth = 0.65) +
  geom_point(color = "#134E4A", size = 1.9) +
  scale_x_continuous(breaks = seq(min(event_window), max(event_window), by = 2L)) +
  labs(
    title = "SDID event-study gaps",
    subtitle = "Florida minus synthetic control, centered by the SDID time-weighted pre-Ian gap.",
    x = "Quarters relative to Hurricane Ian (0 = 2022 Q4)",
    y = "Effect on log(1 + total remittances_usd)"
  ) +
  theme_event()

ggsave(
  filename = file.path(plot_dir, "sdid_event_study_log.png"),
  plot = sdid_event_plot,
  width = 9,
  height = 5,
  dpi = 300
)

placebo_plot <- ggplot(placebo_results, aes(x = estimate_log_points)) +
  geom_histogram(bins = 18, fill = "#CBD5E1", color = "white") +
  geom_vline(xintercept = sdid_fit$tau, color = "#B91C1C", linewidth = 0.9) +
  geom_vline(xintercept = 0, color = "grey45", linewidth = 0.35) +
  labs(
    title = "SDID placebo distribution",
    subtitle = paste0("Vertical red line is Florida; placebo p-value = ", signif(placebo_p_value, 3), "."),
    x = "Placebo estimate, log points",
    y = "Donor states"
  ) +
  theme_event()

ggsave(
  filename = file.path(plot_dir, "sdid_placebo_distribution.png"),
  plot = placebo_plot,
  width = 8,
  height = 4.8,
  dpi = 300
)

avg_post_estimate <- function(event_results) {
  event_results[
    time_to_treat %in% post_event_window,
    mean(estimate, na.rm = TRUE)
  ]
}

identification_comparison <- rbindlist(
  list(
    data.table(
      design = "TWFE, all corridors",
      identifying_variation = "Florida corridors vs all non-Florida corridors",
      estimate_type = "Static post coefficient, log points",
      estimate = twfe_static_results[term == "treated_post", estimate],
      estimate_pct = twfe_static_results[term == "treated_post", effect_pct],
      event_avg_post_estimate = avg_post_estimate(twfe_event_results),
      pretrend_p_value = twfe_pretrend_results$p.value,
      passes_pretrend_10pct = twfe_pretrend_results$p.value > 0.1
    ),
    data.table(
      design = "Corridor TWFE",
      identifying_variation = "Florida corridors vs non-Florida corridors to same receiver-quarter",
      estimate_type = "Static post coefficient, log points",
      estimate = corridor_twfe_static_results[term == "treated_post", estimate],
      estimate_pct = corridor_twfe_static_results[term == "treated_post", effect_pct],
      event_avg_post_estimate = avg_post_estimate(corridor_twfe_event_results),
      pretrend_p_value = corridor_twfe_pretrend_results$p.value,
      passes_pretrend_10pct = corridor_twfe_pretrend_results$p.value > 0.1
    ),
    data.table(
      design = "Matched-corridor TWFE",
      identifying_variation = "Florida corridors vs nearest pre-Ian same-receiver donor corridors",
      estimate_type = "Average post event coefficient, log points",
      estimate = avg_post_estimate(matched_corridor_event_results),
      estimate_pct = 100 * (exp(avg_post_estimate(matched_corridor_event_results)) - 1),
      event_avg_post_estimate = avg_post_estimate(matched_corridor_event_results),
      pretrend_p_value = matched_corridor_pretrend_results$p.value,
      passes_pretrend_10pct = matched_corridor_pretrend_results$p.value > 0.1
    ),
    data.table(
      design = "DRDID event path",
      identifying_variation = "Florida corridors vs controls conditional on pre-window covariates",
      estimate_type = "Average post event coefficient, log points",
      estimate = avg_post_estimate(drdid_outputs$event_results),
      estimate_pct = 100 * (exp(avg_post_estimate(drdid_outputs$event_results)) - 1),
      event_avg_post_estimate = avg_post_estimate(drdid_outputs$event_results),
      pretrend_p_value = drdid_outputs$pretrend_results$p.value,
      passes_pretrend_10pct = drdid_outputs$pretrend_results$p.value > 0.1
    ),
    data.table(
      design = "BJS imputation",
      identifying_variation = "Untreated potential outcomes imputed from non-Florida and Florida-pre observations",
      estimate_type = "Average post imputed effect, log points",
      estimate = bjs_static_results$estimate,
      estimate_pct = bjs_static_results$effect_pct,
      event_avg_post_estimate = avg_post_estimate(bjs_treated_effects),
      pretrend_p_value = bjs_pretrend_results$p.value,
      passes_pretrend_10pct = bjs_pretrend_results$p.value > 0.1
    ),
    data.table(
      design = "Shift-share exposure",
      identifying_variation = "Mexican receivers with higher vs lower pre-Ian Florida remittance exposure",
      estimate_type = "Static post coefficient per 10pp Florida exposure",
      estimate = shiftshare_static_results[term == "exposure_post_10pp", estimate],
      estimate_pct = shiftshare_static_results[term == "exposure_post_10pp", effect_pct],
      event_avg_post_estimate = avg_post_estimate(shiftshare_event_results),
      pretrend_p_value = shiftshare_pretrend_results$p.value,
      passes_pretrend_10pct = shiftshare_pretrend_results$p.value > 0.1
    ),
    data.table(
      design = "SDID, state aggregate",
      identifying_variation = "Florida vs synthetic donor-state combination",
      estimate_type = "Average post SDID gap, log points",
      estimate = sdid_summary$estimate_log_points,
      estimate_pct = sdid_summary$estimate_pct,
      event_avg_post_estimate = avg_post_estimate(sdid_event_results),
      pretrend_p_value = unique(placebo_results$placebo_p_value),
      passes_pretrend_10pct = NA
    )
  ),
  fill = TRUE
)
fwrite(
  identification_comparison,
  file.path(table_dir, "identification_design_comparison.csv")
)

# Console summary --------------------------------------------------------------

print("--- Excluded receiver municipalities ---")
print(excluded_receivers)

print("--- TWFE static log result ---")
print(twfe_static_results)

print("--- TWFE joint pretrend test ---")
print(twfe_pretrend_results)

print("--- Corridor TWFE joint pretrend test ---")
print(corridor_twfe_pretrend_results)

print("--- Matched-corridor joint pretrend test ---")
print(matched_corridor_pretrend_results)

print("--- DRDID joint pretrend test ---")
print(drdid_outputs$pretrend_results)

print("--- BJS imputation joint pretrend test ---")
print(bjs_pretrend_results)

print("--- Shift-share joint pretrend test ---")
print(shiftshare_pretrend_results)

print("--- Identification design comparison ---")
print(identification_comparison)

print("--- HonestDiD relative-magnitude bounds ---")
print(honestdid_bounds)

print("--- SDID summary ---")
print(sdid_summary)

print("--- SDID placebo p-value ---")
print(placebo_p_value)
