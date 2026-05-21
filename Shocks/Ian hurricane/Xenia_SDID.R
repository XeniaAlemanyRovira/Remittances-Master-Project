#!/usr/bin/env Rscript

# Hurricane Ian synthetic control with lagged remittance predictors ------------
# Design:
# - Treated unit: Florida.
# - Donor pool: all other US states.
# - Outcome: state-quarter log(1 + total remittances_usd).
# - Unit weights: simplex weights on donor states chosen to match Florida's
#   pre-Ian lagged outcome values.

suppressPackageStartupMessages({
  library(data.table)
  library(ggplot2)
})

options(scipen = 999)

# Paths -----------------------------------------------------------------------

get_script_path <- function() {
  cmd_args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", cmd_args, value = TRUE)

  if (length(file_arg) > 0) {
    return(normalizePath(sub("^--file=", "", file_arg[1]), winslash = "/", mustWork = TRUE))
  }

  frames <- sys.frames()
  for (frame in rev(frames)) {
    if (!is.null(frame$ofile)) {
      return(normalizePath(frame$ofile, winslash = "/", mustWork = TRUE))
    }
  }

  NA_character_
}

find_repo_root <- function(start_dir) {
  current_dir <- normalizePath(start_dir, winslash = "/", mustWork = TRUE)

  repeat {
    if (
      file.exists(file.path(current_dir, "Thesis.Rproj")) ||
      dir.exists(file.path(current_dir, ".git"))
    ) {
      return(current_dir)
    }

    parent_dir <- dirname(current_dir)
    if (identical(parent_dir, current_dir)) {
      stop("Could not find the project root from: ", start_dir)
    }
    current_dir <- parent_dir
  }
}

script_path <- get_script_path()
project_dir <- if (!is.na(script_path)) {
  find_repo_root(dirname(script_path))
} else {
  find_repo_root(getwd())
}

path_in_repo <- function(...) {
  normalizePath(file.path(project_dir, ...), winslash = "/", mustWork = FALSE)
}

analysis_dir <- path_in_repo("Shocks", "Ian hurricane")
data_file <- path_in_repo(
  "1_network_estimation",
  "4_remittance_calibration",
  "output",
  "calibrated_remittance_flows_master_2013q1_2024q4_usd.csv"
)

output_dir <- file.path(analysis_dir, "outputs", "xenia_lag_synthetic_control")
plot_dir <- file.path(output_dir, "plots")
table_dir <- file.path(output_dir, "tables")
dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(table_dir, showWarnings = FALSE, recursive = TRUE)

# Parameters ------------------------------------------------------------------

treated_state <- "Florida"
hit_q <- (2022L - 2013L) * 4L + 4L
event_window <- -8L:8L
lag_window <- 12L
post_event_window <- event_window[event_window >= 0L]

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

# Weight solver ----------------------------------------------------------------

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
      weights <- if (sum(weights) > 0) weights / sum(weights) else rep(1 / n_weights, n_weights)
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

fit_lag_synthetic_control <- function(Y, q_index, treated_state, control_states, hit_q, lag_window) {
  treated_index <- match(treated_state, rownames(Y))
  control_indices <- match(control_states, rownames(Y))
  lag_q <- tail(q_index[q_index < hit_q], lag_window)
  lag_cols <- match(lag_q, q_index)
  post_cols <- which(q_index >= hit_q)

  treated_lags <- Y[treated_index, lag_cols]
  control_lags <- Y[control_indices, lag_cols, drop = FALSE]

  unit_fit <- fit_simplex_weights(
    X = t(control_lags),
    target = treated_lags
  )

  omega <- unit_fit$weights
  synthetic_path <- as.numeric(omega %*% Y[control_indices, , drop = FALSE]) +
    unit_fit$intercept
  treated_path <- as.numeric(Y[treated_index, ])
  gap <- treated_path - synthetic_path
  baseline_gap <- mean(gap[lag_cols], na.rm = TRUE)
  centered_gap <- gap - baseline_gap
  tau <- mean(centered_gap[post_cols], na.rm = TRUE)

  list(
    tau = tau,
    baseline_gap = baseline_gap,
    lag_q = lag_q,
    lag_cols = lag_cols,
    q_index = q_index,
    treated_state = treated_state,
    control_states = control_states,
    treated_path = treated_path,
    synthetic_path = synthetic_path,
    gap = gap,
    centered_gap = centered_gap,
    omega = omega,
    unit_intercept = unit_fit$intercept,
    unit_objective = unit_fit$objective,
    unit_solver = unit_fit$solver
  )
}

# Load and prepare data --------------------------------------------------------

df <- fread(data_file)
df[
  ,
  `:=`(
    receiver_id = paste(mx_state, mx_municipality, sep = " - "),
    q_num = (year - 2013L) * 4L + quarter,
    time_to_treat = (year - 2013L) * 4L + quarter - hit_q
  )
]

excluded_receivers <- df[
  q_num < hit_q & us_state == treated_state,
  .(pre_florida_usd = sum(remittances_usd, na.rm = TRUE)),
  by = .(receiver_id, mx_state, mx_municipality)
][order(-pre_florida_usd)][1:2]

df <- df[!receiver_id %chin% excluded_receivers$receiver_id]
fwrite(excluded_receivers, file.path(table_dir, "excluded_top_florida_receivers.csv"))

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
  stop("State-quarter matrix has missing values; synthetic control requires a balanced panel.")
}

donor_states <- setdiff(rownames(Y), treated_state)

# Main synthetic control -------------------------------------------------------

sc_fit <- fit_lag_synthetic_control(
  Y = Y,
  q_index = q_index,
  treated_state = treated_state,
  control_states = donor_states,
  hit_q = hit_q,
  lag_window = lag_window
)

sc_summary <- data.table(
  estimator = "Lagged-outcome synthetic control",
  treated_state = treated_state,
  outcome = "log(1 + total remittances_usd)",
  weight_predictors = paste0("Last ", length(sc_fit$lag_q), " pre-Ian quarterly lag values"),
  lag_predictor_quarters = paste(quarter_label(sc_fit$lag_q - hit_q), collapse = "; "),
  top_receiver_exclusions = paste(excluded_receivers$receiver_id, collapse = "; "),
  donor_states = length(donor_states),
  pre_periods = sum(q_index < hit_q),
  post_periods = sum(q_index >= hit_q),
  estimate_log_points = sc_fit$tau,
  estimate_pct = 100 * (exp(sc_fit$tau) - 1),
  baseline_gap = sc_fit$baseline_gap,
  unit_weight_solver = sc_fit$unit_solver,
  unit_weight_objective = sc_fit$unit_objective,
  unit_intercept = sc_fit$unit_intercept
)
fwrite(sc_summary, file.path(table_dir, "xenia_lag_synthetic_control_summary.csv"))

sc_unit_weights <- data.table(
  us_state = donor_states,
  unit_weight = sc_fit$omega
)[order(-unit_weight)]
fwrite(sc_unit_weights, file.path(table_dir, "xenia_lag_synthetic_control_weights.csv"))

sc_paths <- data.table(
  q_num = q_index,
  time_to_treat = q_index - hit_q,
  calendar_quarter = quarter_label(q_index - hit_q),
  florida_log_total = sc_fit$treated_path,
  synthetic_log_total = sc_fit$synthetic_path,
  gap = sc_fit$gap,
  centered_gap = sc_fit$centered_gap,
  used_for_weights = q_index %in% sc_fit$lag_q,
  post_ian = q_index >= hit_q
)
fwrite(sc_paths, file.path(table_dir, "xenia_lag_synthetic_control_paths.csv"))

sc_event_results <- sc_paths[
  time_to_treat %in% event_window,
  .(
    time_to_treat,
    calendar_quarter,
    estimate = centered_gap,
    effect_pct = 100 * (exp(centered_gap) - 1),
    used_for_weights,
    post_ian
  )
]
fwrite(sc_event_results, file.path(table_dir, "xenia_lag_synthetic_control_event_results.csv"))

# Donor-state placebo distribution and confidence intervals -------------------

placebo_fits <- lapply(
  donor_states,
  function(placebo_state) {
    placebo_controls <- setdiff(rownames(Y), c(treated_state, placebo_state))
    placebo_fit <- fit_lag_synthetic_control(
      Y = Y,
      q_index = q_index,
      treated_state = placebo_state,
      control_states = placebo_controls,
      hit_q = hit_q,
      lag_window = lag_window
    )

    list(placebo_state = placebo_state, fit = placebo_fit)
  }
)

placebo_results <- rbindlist(
  lapply(placebo_fits, function(item) {
    data.table(
      placebo_state = item$placebo_state,
      estimate_log_points = item$fit$tau,
      estimate_pct = 100 * (exp(item$fit$tau) - 1),
      lag_fit_objective = item$fit$unit_objective
    )
  })
)
placebo_results[
  ,
  `:=`(
    florida_estimate_log_points = sc_fit$tau,
    abs_placebo_ge_florida = abs(estimate_log_points) >= abs(sc_fit$tau)
  )
]
placebo_p_value <- (sum(placebo_results$abs_placebo_ge_florida) + 1) /
  (nrow(placebo_results) + 1)
placebo_results[, placebo_p_value := placebo_p_value]
fwrite(placebo_results, file.path(table_dir, "xenia_lag_synthetic_control_placebos.csv"))

placebo_event_paths <- rbindlist(
  lapply(placebo_fits, function(item) {
    data.table(
      placebo_state = item$placebo_state,
      q_num = q_index,
      time_to_treat = q_index - hit_q,
      calendar_quarter = quarter_label(q_index - hit_q),
      placebo_centered_gap = item$fit$centered_gap,
      used_for_weights = q_index %in% item$fit$lag_q,
      post_ian = q_index >= hit_q
    )
  })
)

fwrite(
  placebo_event_paths,
  file.path(table_dir, "xenia_lag_synthetic_control_placebo_event_paths.csv")
)

tau_error_quantile_90 <- quantile(abs(placebo_results$estimate_log_points), 0.90, na.rm = TRUE)
tau_error_quantile_95 <- quantile(abs(placebo_results$estimate_log_points), 0.95, na.rm = TRUE)

sc_average_effect_intervals <- data.table(
  estimator = "Lagged-outcome synthetic control",
  interval_method = "Donor-placebo absolute-error inversion",
  confidence_level = c(0.90, 0.95),
  estimate_log_points = sc_fit$tau,
  conf.low = c(sc_fit$tau - tau_error_quantile_90, sc_fit$tau - tau_error_quantile_95),
  conf.high = c(sc_fit$tau + tau_error_quantile_90, sc_fit$tau + tau_error_quantile_95)
)
sc_average_effect_intervals[
  ,
  `:=`(
    estimate_pct = 100 * (exp(estimate_log_points) - 1),
    conf.low_pct = 100 * (exp(conf.low) - 1),
    conf.high_pct = 100 * (exp(conf.high) - 1),
    placebo_p_value = placebo_p_value,
    placebo_states = nrow(placebo_results)
  )
]

fwrite(
  sc_average_effect_intervals,
  file.path(table_dir, "xenia_lag_synthetic_control_average_effect_intervals.csv")
)

pointwise_error_bands <- placebo_event_paths[
  time_to_treat %in% event_window,
  .(
    placebo_abs_q90 = quantile(abs(placebo_centered_gap), 0.90, na.rm = TRUE),
    placebo_abs_q95 = quantile(abs(placebo_centered_gap), 0.95, na.rm = TRUE),
    placebo_q025 = quantile(placebo_centered_gap, 0.025, na.rm = TRUE),
    placebo_q975 = quantile(placebo_centered_gap, 0.975, na.rm = TRUE)
  ),
  by = .(time_to_treat)
]

sc_event_results <- merge(sc_event_results, pointwise_error_bands, by = "time_to_treat", all.x = TRUE)
setorder(sc_event_results, time_to_treat)
sc_event_results[
  ,
  `:=`(
    conf.low_90 = estimate - placebo_abs_q90,
    conf.high_90 = estimate + placebo_abs_q90,
    conf.low_95 = estimate - placebo_abs_q95,
    conf.high_95 = estimate + placebo_abs_q95,
    conf.low_95_pct = 100 * (exp(estimate - placebo_abs_q95) - 1),
    conf.high_95_pct = 100 * (exp(estimate + placebo_abs_q95) - 1)
  )
]

fwrite(sc_event_results, file.path(table_dir, "xenia_lag_synthetic_control_event_results.csv"))

# Plots -----------------------------------------------------------------------

plot_data <- melt(
  sc_paths[time_to_treat %between% c(min(event_window), max(event_window))],
  id.vars = c("time_to_treat", "calendar_quarter", "post_ian", "used_for_weights"),
  measure.vars = c("florida_log_total", "synthetic_log_total"),
  variable.name = "series",
  value.name = "log_total_remittances"
)
plot_data[
  ,
  series := fifelse(series == "florida_log_total", treated_state, "Synthetic control")
]

path_plot <- ggplot(
  plot_data,
  aes(x = time_to_treat, y = log_total_remittances, color = series)
) +
  geom_vline(xintercept = -0.5, linetype = "dashed", color = "grey35") +
  geom_line(linewidth = 0.8) +
  geom_point(aes(shape = used_for_weights), size = 1.6) +
  scale_color_manual(values = c("Florida" = "#B91C1C", "Synthetic control" = "#1D4ED8")) +
  scale_shape_manual(values = c("FALSE" = 16, "TRUE" = 17), name = "Used for weights") +
  scale_x_continuous(breaks = seq(min(event_window), max(event_window), by = 2L)) +
  labs(
    title = "Lagged-outcome synthetic control",
    subtitle = paste0(
      treated_state,
      " versus donor-state synthetic control; weights match the last ",
      lag_window,
      " pre-Ian lag values."
    ),
    x = "Quarters relative to Hurricane Ian (0 = 2022 Q4)",
    y = "log(1 + total remittances_usd)",
    color = NULL
  ) +
  theme_event()

ggsave(
  filename = file.path(plot_dir, "xenia_lag_synthetic_control_path.png"),
  plot = path_plot,
  width = 9,
  height = 5,
  dpi = 300
)

event_plot <- ggplot(sc_event_results, aes(x = time_to_treat, y = estimate)) +
  geom_hline(yintercept = 0, color = "grey45", linewidth = 0.35) +
  geom_vline(xintercept = -0.5, linetype = "dashed", color = "grey35") +
  geom_ribbon(
    aes(ymin = conf.low_95, ymax = conf.high_95),
    fill = "#99F6E4",
    alpha = 0.35,
    color = NA
  ) +
  geom_ribbon(
    aes(ymin = conf.low_90, ymax = conf.high_90),
    fill = "#2DD4BF",
    alpha = 0.28,
    color = NA
  ) +
  geom_line(color = "#0F766E", linewidth = 0.65) +
  geom_point(aes(shape = used_for_weights), color = "#134E4A", size = 1.9) +
  scale_shape_manual(values = c("FALSE" = 16, "TRUE" = 17), name = "Used for weights") +
  scale_x_continuous(breaks = seq(min(event_window), max(event_window), by = 2L)) +
  labs(
    title = "Synthetic-control event gaps",
    subtitle = "Florida minus synthetic control; ribbons are donor-placebo 90% and 95% bands.",
    x = "Quarters relative to Hurricane Ian (0 = 2022 Q4)",
    y = "Effect on log(1 + total remittances_usd)"
  ) +
  theme_event()

ggsave(
  filename = file.path(plot_dir, "xenia_lag_synthetic_control_event_gaps.png"),
  plot = event_plot,
  width = 9,
  height = 5,
  dpi = 300
)

placebo_plot <- ggplot(placebo_results, aes(x = estimate_log_points)) +
  geom_histogram(bins = 18, fill = "#CBD5E1", color = "white") +
  geom_vline(xintercept = sc_fit$tau, color = "#B91C1C", linewidth = 0.9) +
  geom_vline(xintercept = 0, color = "grey45", linewidth = 0.35) +
  labs(
    title = "Synthetic-control placebo distribution",
    subtitle = paste0("Vertical red line is Florida; placebo p-value = ", signif(placebo_p_value, 3), "."),
    x = "Placebo estimate, log points",
    y = "Donor states"
  ) +
  theme_event()

ggsave(
  filename = file.path(plot_dir, "xenia_lag_synthetic_control_placebos.png"),
  plot = placebo_plot,
  width = 8,
  height = 4.8,
  dpi = 300
)

print(sc_summary)
print(sc_unit_weights[unit_weight > 0.001])
print(data.table(placebo_p_value = placebo_p_value))
