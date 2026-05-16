# Hurricane Ian remittances: outlier / influence diagnostics
#
# Exploratory analysis to test whether a small number of US control/donor
# states or Mexican receiver municipalities are driving the parallel-trends
# behaviour and the headline effects in Analysis.R.
#
# This script is self-contained and DOES NOT touch the Analysis.R outputs.
# It writes everything under outputs/outlier_diagnostics/.
#
# What it does:
#   1. Concentration: how lopsided are remittances across US states and
#      across Mexican receivers (shares, cumulative curves, HHI).
#   2. Pre-Ian path divergence of every non-Florida state vs Florida.
#   3. First-order influence of each corridor / receiver / state on the
#      TWFE treated_post coefficient (cheap global ranking).
#   4. Exact leave-one-US-state-out: refit TWFE (static + event-study
#      pretrend) and the SDID-style estimator dropping each control state.
#   5. Exact leave-top-K Florida-receiver-out: cumulative and one-at-a-time
#      removal of the largest pre-Ian Florida receivers.
#   6. Plots summarising 3-5.

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

diag_dir <- file.path(analysis_dir, "outputs", "outlier_diagnostics")
diag_tables <- file.path(diag_dir, "tables")
diag_plots <- file.path(diag_dir, "plots")
dir.create(diag_tables, showWarnings = FALSE, recursive = TRUE)
dir.create(diag_plots, showWarnings = FALSE, recursive = TRUE)

# Parameters (mirror Analysis.R) ----------------------------------------------

hit_q <- (2022L - 2013L) * 4L + 4L
event_window <- -8L:8L
event_ref <- -1L
pretrend_window <- setdiff(event_window[event_window < 0L], event_ref)
post_event_window <- event_window[event_window >= 0L]
top_k_receivers <- 12L          # how deep to probe Florida receivers
n_state_loo_states <- Inf       # leave-one-out over all non-FL states

# Load and prepare ------------------------------------------------------------

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

# Top-2 pre-Ian Florida receivers excluded by the main spec
excluded_receivers <- df[
  q_num < hit_q & us_state == "Florida",
  .(pre_florida_usd = sum(remittances_usd, na.rm = TRUE)),
  by = .(receiver_id, mx_state, mx_municipality)
][order(-pre_florida_usd)][1:2]

# df_full keeps every receiver; df_base mirrors the main spec exclusion.
df_full <- copy(df)
df_base <- df[!receiver_id %chin% excluded_receivers$receiver_id]

df_full_window <- df_full[time_to_treat %in% event_window]
df_base_window <- df_base[time_to_treat %in% event_window]

quarter_label <- function(time_to_treat) {
  q_num <- hit_q + time_to_treat
  quarter <- ((q_num - 1L) %% 4L) + 1L
  year <- 2013L + (q_num - 1L) %/% 4L
  paste0(year, " Q", quarter)
}

theme_diag <- function() {
  theme_minimal(base_size = 12) +
    theme(
      legend.position = "top",
      panel.grid.minor = element_blank(),
      plot.title.position = "plot"
    )
}

pretrend_pvalue <- function(model, interaction_var = "is_treated") {
  keep_pattern <- paste0(
    paste0("time_to_treat::", pretrend_window, ":", interaction_var),
    collapse = "|"
  )
  out <- tryCatch(
    wald(model, keep = keep_pattern, print = FALSE),
    error = function(e) NULL
  )
  if (is.null(out)) {
    return(list(p.value = NA_real_, statistic = NA_real_))
  }
  list(p.value = out$p, statistic = out$stat)
}

avg_post_coef <- function(model, interaction_var = "is_treated") {
  cf <- coef(model)
  terms <- paste0("time_to_treat::", post_event_window, ":", interaction_var)
  terms <- terms[terms %in% names(cf)]
  if (length(terms) == 0L) NA_real_ else mean(cf[terms])
}

# =============================================================================
# 1. Concentration diagnostics
# =============================================================================

# 1a. US state share of total remittances (full sample and pre-Ian)
state_share <- df_full[
  ,
  .(
    total_usd = sum(remittances_usd, na.rm = TRUE),
    pre_ian_usd = sum(remittances_usd[q_num < hit_q], na.rm = TRUE)
  ),
  by = us_state
][order(-total_usd)]
state_share[
  ,
  `:=`(
    total_share = total_usd / sum(total_usd),
    pre_ian_share = pre_ian_usd / sum(pre_ian_usd),
    cum_total_share = cumsum(total_usd) / sum(total_usd)
  )
]
state_hhi <- state_share[, sum(total_share^2)]
fwrite(state_share, file.path(diag_tables, "concentration_us_state.csv"))

# 1b. Florida receiver concentration (this is what the top-2 exclusion targets)
florida_receiver <- df_full[
  us_state == "Florida",
  .(
    florida_usd = sum(remittances_usd, na.rm = TRUE),
    pre_ian_florida_usd = sum(remittances_usd[q_num < hit_q], na.rm = TRUE)
  ),
  by = .(receiver_id, mx_state, mx_municipality)
][order(-pre_ian_florida_usd)]
florida_receiver[
  ,
  `:=`(
    pre_ian_share = pre_ian_florida_usd / sum(pre_ian_florida_usd),
    cum_pre_ian_share = cumsum(pre_ian_florida_usd) / sum(pre_ian_florida_usd),
    rank = seq_len(.N)
  )
]
florida_receiver_hhi <- florida_receiver[, sum(pre_ian_share^2)]
fwrite(
  florida_receiver[rank <= 50L],
  file.path(diag_tables, "concentration_florida_receivers_top50.csv")
)

concentration_summary <- data.table(
  metric = c(
    "US states (n)",
    "US state HHI (full sample)",
    "Top 1 US state share",
    "Top 3 US states share",
    "Florida pre-Ian receivers (n)",
    "Florida receiver HHI (pre-Ian)",
    "Top 1 FL receiver pre-Ian share",
    "Top 2 FL receivers pre-Ian share (excluded by main spec)",
    "Top 10 FL receivers pre-Ian share"
  ),
  value = c(
    state_share[, .N],
    round(state_hhi, 4),
    round(state_share[1, total_share], 4),
    round(state_share[1:3, sum(total_share)], 4),
    florida_receiver[, .N],
    round(florida_receiver_hhi, 4),
    round(florida_receiver[1, pre_ian_share], 4),
    round(florida_receiver[1:2, sum(pre_ian_share)], 4),
    round(florida_receiver[1:10, sum(pre_ian_share)], 4)
  )
)
fwrite(concentration_summary, file.path(diag_tables, "concentration_summary.csv"))

# =============================================================================
# 2. Pre-Ian path divergence of each non-Florida state vs Florida
#    (a divergent-pre-trend control state is a prime parallel-trends suspect)
# =============================================================================

state_q <- df_base[
  ,
  .(log_total = log1p(sum(remittances_usd, na.rm = TRUE))),
  by = .(us_state, q_num, time_to_treat)
]
fl_path <- state_q[us_state == "Florida", .(q_num, fl_log = log_total)]

state_divergence <- state_q[
  us_state != "Florida"
][
  fl_path, on = "q_num", nomatch = 0
][
  q_num < hit_q,
  .(
    pre_corr = suppressWarnings(cor(log_total, fl_log)),
    pre_rmse = sqrt(mean((log_total - fl_log)^2)),
    # gap-trend slope: how fast the FL-minus-state gap drifts pre-Ian
    pre_gap_slope = {
      g <- log_total - fl_log
      tt <- time_to_treat
      ok <- is.finite(g) & is.finite(tt)
      if (sum(ok) > 2L) as.numeric(coef(lm(g[ok] ~ tt[ok]))[2]) else NA_real_
    }
  ),
  by = us_state
]
state_divergence[, abs_pre_gap_slope := abs(pre_gap_slope)]
setorder(state_divergence, -abs_pre_gap_slope)
fwrite(state_divergence, file.path(diag_tables, "state_pretrend_divergence.csv"))

# =============================================================================
# 3. First-order influence on the TWFE treated_post coefficient
#    Residualise treated_post and the outcome on the two-way FE, then
#    decompose beta into per-corridor contributions. Cheap global ranking
#    of which corridors / receivers / states move the headline coefficient.
# =============================================================================

infl_dt <- df_base_window[, .(
  pair_id, receiver_id, us_state, year_quarter,
  y = log1p_remittances, x = as.numeric(treated_post)
)]

ytil <- resid(feols(y ~ 1 | pair_id + year_quarter, data = infl_dt))
xtil <- resid(feols(x ~ 1 | pair_id + year_quarter, data = infl_dt))
sxx <- sum(xtil^2)
beta_full <- sum(xtil * ytil) / sxx
e <- ytil - beta_full * xtil
infl_dt[, contrib := (xtil * e) / sxx]   # first-order d(beta) if obs removed

corridor_influence <- infl_dt[
  ,
  .(
    influence = sum(contrib),
    n_obs = .N,
    us_state = us_state[1],
    receiver_id = receiver_id[1]
  ),
  by = pair_id
]
corridor_influence[, abs_influence := abs(influence)]
setorder(corridor_influence, -abs_influence)
corridor_influence[, beta_full := beta_full]
fwrite(
  corridor_influence[1:100],
  file.path(diag_tables, "influence_top100_corridors.csv")
)

receiver_influence <- infl_dt[
  , .(influence = sum(contrib), n_obs = .N), by = receiver_id
]
receiver_influence[, abs_influence := abs(influence)]
setorder(receiver_influence, -abs_influence)
fwrite(
  receiver_influence[1:100],
  file.path(diag_tables, "influence_top100_receivers.csv")
)

state_influence <- infl_dt[
  , .(influence = sum(contrib), n_obs = .N), by = us_state
]
state_influence[, abs_influence := abs(influence)]
setorder(state_influence, -abs_influence)
state_influence[, beta_full := beta_full]
fwrite(state_influence, file.path(diag_tables, "influence_by_us_state.csv"))

# =============================================================================
# 4. Exact leave-one-US-state-out
#    Drop each non-Florida state from the control pool, refit the main
#    corridor TWFE (static + event-study pretrend) and the SDID estimator.
# =============================================================================

control_states <- sort(setdiff(unique(df_base_window$us_state), "Florida"))
if (is.finite(n_state_loo_states)) {
  control_states <- head(control_states, n_state_loo_states)
}

base_static <- feols(
  log1p_remittances ~ treated_post | pair_id + year_quarter,
  data = df_base_window, cluster = ~ pair_id
)
base_event <- feols(
  log1p_remittances ~ i(time_to_treat, is_treated, ref = event_ref) |
    pair_id + year_quarter,
  data = df_base_window, cluster = ~ pair_id
)
base_pt <- pretrend_pvalue(base_event)
base_row <- data.table(
  dropped_state = "(none) baseline",
  static_estimate = coef(base_static)["treated_post"],
  static_pct = 100 * (exp(coef(base_static)["treated_post"]) - 1),
  avg_post_estimate = avg_post_coef(base_event),
  pretrend_p = base_pt$p.value,
  passes_pretrend_10pct = base_pt$p.value > 0.1
)

state_loo <- rbindlist(lapply(control_states, function(st) {
  d <- df_base_window[us_state != st]
  m_s <- feols(
    log1p_remittances ~ treated_post | pair_id + year_quarter,
    data = d, cluster = ~ pair_id
  )
  m_e <- feols(
    log1p_remittances ~ i(time_to_treat, is_treated, ref = event_ref) |
      pair_id + year_quarter,
    data = d, cluster = ~ pair_id
  )
  pt <- pretrend_pvalue(m_e)
  data.table(
    dropped_state = st,
    static_estimate = coef(m_s)["treated_post"],
    static_pct = 100 * (exp(coef(m_s)["treated_post"]) - 1),
    avg_post_estimate = avg_post_coef(m_e),
    pretrend_p = pt$p.value,
    passes_pretrend_10pct = pt$p.value > 0.1
  )
}))

state_loo <- rbindlist(list(base_row, state_loo))
state_loo[
  ,
  `:=`(
    d_static_vs_base = static_estimate - base_row$static_estimate,
    d_pretrend_p_vs_base = pretrend_p - base_row$pretrend_p,
    flips_pretrend_decision = (pretrend_p > 0.1) != base_row$passes_pretrend_10pct
  )
]
setorder(state_loo, -flips_pretrend_decision, -pretrend_p)
fwrite(state_loo, file.path(diag_tables, "leave_one_state_out_twfe.csv"))

# --- SDID leave-one-donor-state-out -----------------------------------------

fit_simplex_weights <- function(X, target, ridge = 1e-6) {
  X <- as.matrix(X)
  target <- as.numeric(target)
  n_features <- nrow(X)
  n_weights <- ncol(X)
  X_centered <- sweep(X, 2, colMeans(X), FUN = "-")
  target_centered <- target - mean(target)

  if (requireNamespace("osqp", quietly = TRUE) &&
        requireNamespace("Matrix", quietly = TRUE)) {
    P <- 2 * (crossprod(X_centered) / n_features + ridge * diag(n_weights))
    q <- as.numeric(-2 * crossprod(X_centered, target_centered) / n_features)
    A <- rbind(
      Matrix::Matrix(rep(1, n_weights), nrow = 1, sparse = TRUE),
      Matrix::Diagonal(n_weights)
    )
    fit <- tryCatch(
      osqp::solve_osqp(
        P = methods::as(P, "dgCMatrix"), q = q,
        A = methods::as(A, "dgCMatrix"),
        l = c(1, rep(0, n_weights)), u = c(1, rep(Inf, n_weights)),
        pars = osqp::osqpSettings(verbose = FALSE, eps_abs = 1e-8, eps_rel = 1e-8)
      ),
      error = function(e) NULL
    )
    if (!is.null(fit) && fit$info$status %in% c("solved", "solved inaccurate")) {
      w <- pmax(as.numeric(fit$x), 0)
      w <- if (sum(w) > 0) w / sum(w) else rep(1 / n_weights, n_weights)
      intercept <- mean(target - as.numeric(X %*% w))
      return(list(weights = w, intercept = intercept))
    }
  }
  softmax <- function(theta) {
    s <- exp(theta - max(theta)); s / sum(s)
  }
  obj <- function(theta) {
    w <- softmax(theta)
    intercept <- mean(target - as.numeric(X %*% w))
    mean((target - intercept - as.numeric(X %*% w))^2) + ridge * sum(w^2)
  }
  opt <- optim(rep(0, n_weights), obj, method = "BFGS",
               control = list(maxit = 1000, reltol = 1e-10))
  w <- softmax(opt$par)
  list(weights = w, intercept = mean(target - as.numeric(X %*% w)))
}

fit_sdid_tau <- function(Y, q_index, treated_state, control_states, hit_q) {
  ti <- match(treated_state, rownames(Y))
  ci <- match(control_states, rownames(Y))
  pre <- which(q_index < hit_q)
  post <- which(q_index >= hit_q)
  unit_fit <- fit_simplex_weights(t(Y[ci, pre, drop = FALSE]), Y[ti, pre])
  time_fit <- fit_simplex_weights(
    Y[ci, pre, drop = FALSE], rowMeans(Y[ci, post, drop = FALSE])
  )
  omega <- unit_fit$weights
  lambda <- time_fit$weights
  synth <- as.numeric(omega %*% Y[ci, , drop = FALSE]) + unit_fit$intercept
  gap <- as.numeric(Y[ti, ]) - synth
  baseline_gap <- sum(lambda * gap[pre])
  mean((gap - baseline_gap)[post])
}

state_qq <- df_base[
  , .(log_total = log1p(sum(remittances_usd, na.rm = TRUE))),
  by = .(us_state, q_num)
]
q_index <- sort(unique(state_qq$q_num))
state_wide <- dcast(state_qq, us_state ~ q_num, value.var = "log_total")
setorder(state_wide, us_state)
Y <- as.matrix(state_wide[, -1])
rownames(Y) <- state_wide$us_state

sdid_loo <- NULL
if (!anyNA(Y)) {
  donor_states <- setdiff(rownames(Y), "Florida")
  base_tau <- fit_sdid_tau(Y, q_index, "Florida", donor_states, hit_q)
  sdid_loo <- rbindlist(c(
    list(data.table(
      dropped_state = "(none) baseline",
      sdid_tau = base_tau, sdid_pct = 100 * (exp(base_tau) - 1)
    )),
    lapply(donor_states, function(st) {
      tau <- fit_sdid_tau(
        Y, q_index, "Florida", setdiff(donor_states, st), hit_q
      )
      data.table(
        dropped_state = st, sdid_tau = tau,
        sdid_pct = 100 * (exp(tau) - 1)
      )
    })
  ))
  sdid_loo[, d_tau_vs_base := sdid_tau - base_tau]
  sdid_loo[, abs_d_tau_vs_base := abs(d_tau_vs_base)]
  setorder(sdid_loo, -abs_d_tau_vs_base)
  fwrite(sdid_loo, file.path(diag_tables, "leave_one_state_out_sdid.csv"))
}

# =============================================================================
# 5. Exact leave-top-K Florida-receiver-out
#    Cumulative removal (0, top 1, top 2 = main spec, ... top K) and
#    one-at-a-time removal of each of the top-K pre-Ian Florida receivers.
# =============================================================================

ranked_receivers <- florida_receiver[rank <= top_k_receivers, receiver_id]

fit_receiver_drop <- function(drop_ids, label, scenario) {
  d <- df_full_window[!receiver_id %chin% drop_ids]
  m_s <- feols(
    log1p_remittances ~ treated_post | pair_id + year_quarter,
    data = d, cluster = ~ pair_id
  )
  m_e <- feols(
    log1p_remittances ~ i(time_to_treat, is_treated, ref = event_ref) |
      pair_id + year_quarter,
    data = d, cluster = ~ pair_id
  )
  pt <- pretrend_pvalue(m_e)
  data.table(
    scenario = scenario,
    label = label,
    n_dropped = length(drop_ids),
    static_estimate = coef(m_s)["treated_post"],
    static_pct = 100 * (exp(coef(m_s)["treated_post"]) - 1),
    avg_post_estimate = avg_post_coef(m_e),
    pretrend_p = pt$p.value,
    passes_pretrend_10pct = pt$p.value > 0.1
  )
}

cumulative_receiver <- rbindlist(lapply(0:top_k_receivers, function(k) {
  drop_ids <- if (k == 0L) character(0) else ranked_receivers[1:k]
  lbl <- if (k == 0L) "keep all receivers" else
    paste0("drop top ", k, " (", ranked_receivers[k], ")")
  fit_receiver_drop(drop_ids, lbl, "cumulative")
}))

oneout_receiver <- rbindlist(lapply(ranked_receivers, function(rid) {
  fit_receiver_drop(rid, paste0("drop only ", rid), "leave-one-out")
}))

base_full <- cumulative_receiver[n_dropped == 0L]
receiver_sensitivity <- rbindlist(list(cumulative_receiver, oneout_receiver))
receiver_sensitivity[
  ,
  `:=`(
    d_static_vs_keepall = static_estimate - base_full$static_estimate,
    d_pretrend_p_vs_keepall = pretrend_p - base_full$pretrend_p
  )
]
fwrite(
  receiver_sensitivity,
  file.path(diag_tables, "leave_top_receivers_out_twfe.csv")
)

# =============================================================================
# 6. Plots
# =============================================================================

# 6a. US-state concentration (cumulative share curve)
state_share[, rank := seq_len(.N)]
gg_state_conc <- ggplot(state_share, aes(rank, cum_total_share)) +
  geom_line(color = "#2563EB", linewidth = 0.7) +
  geom_point(color = "#1E3A8A", size = 1.4) +
  geom_hline(yintercept = 0.9, linetype = "dashed", color = "grey45") +
  scale_y_continuous(limits = c(0, 1)) +
  labs(
    title = "US-state concentration of remittances",
    subtitle = paste0("HHI = ", round(state_hhi, 3),
                       "; dashed line = 90% of total volume"),
    x = "US state rank (by total remittances)",
    y = "Cumulative share of total remittances"
  ) +
  theme_diag()
ggsave(file.path(diag_plots, "concentration_us_state.png"),
       gg_state_conc, width = 8, height = 4.8, dpi = 300)

# 6b. Florida-receiver concentration
gg_recv_conc <- ggplot(
  florida_receiver[rank <= 60L], aes(rank, cum_pre_ian_share)
) +
  geom_line(color = "#B91C1C", linewidth = 0.7) +
  geom_point(color = "#7F1D1D", size = 1.3) +
  geom_vline(xintercept = 2.5, linetype = "dashed", color = "grey35") +
  scale_y_continuous(limits = c(0, 1)) +
  labs(
    title = "Florida receiver concentration (pre-Ian)",
    subtitle = paste0(
      "HHI = ", round(florida_receiver_hhi, 3),
      "; dashed line = main-spec top-2 exclusion cutoff"
    ),
    x = "Mexican receiver rank (by pre-Ian Florida USD)",
    y = "Cumulative share of pre-Ian Florida remittances"
  ) +
  theme_diag()
ggsave(file.path(diag_plots, "concentration_florida_receivers.png"),
       gg_recv_conc, width = 8, height = 4.8, dpi = 300)

# 6c. Leave-one-state-out tornado: pretrend p-value
loo_plot <- state_loo[dropped_state != "(none) baseline"]
loo_plot[, dropped_state := factor(
  dropped_state, levels = loo_plot[order(pretrend_p), dropped_state]
)]
gg_loo_pt <- ggplot(loo_plot, aes(pretrend_p, dropped_state)) +
  geom_vline(xintercept = base_row$pretrend_p, linetype = "dashed",
             color = "#B91C1C") +
  geom_vline(xintercept = 0.1, color = "grey45", linewidth = 0.35) +
  geom_point(aes(color = passes_pretrend_10pct), size = 1.8) +
  scale_color_manual(values = c("FALSE" = "#B91C1C", "TRUE" = "#15803D"),
                     name = "Pretrend p > 0.10") +
  labs(
    title = "Leave-one-US-state-out: TWFE event-study pretrend p-value",
    subtitle = paste0(
      "Red dashed = baseline p (", signif(base_row$pretrend_p, 3),
      "); grey = 0.10 threshold"
    ),
    x = "Joint pretrend p-value with that state dropped",
    y = NULL
  ) +
  theme_diag() +
  theme(axis.text.y = element_text(size = 7))
ggsave(file.path(diag_plots, "leave_one_state_out_pretrend.png"),
       gg_loo_pt, width = 8.5, height = 9, dpi = 300)

# 6d. Leave-one-state-out tornado: static estimate
gg_loo_est <- ggplot(
  loo_plot,
  aes(static_pct, reorder(dropped_state, static_pct))
) +
  geom_vline(xintercept = base_row$static_pct, linetype = "dashed",
             color = "#B91C1C") +
  geom_point(color = "#1D4ED8", size = 1.8) +
  labs(
    title = "Leave-one-US-state-out: static treated_post effect",
    subtitle = paste0("Red dashed = baseline (",
                       round(base_row$static_pct, 1), "%)"),
    x = "Static treated_post effect (%) with that state dropped",
    y = NULL
  ) +
  theme_diag() +
  theme(axis.text.y = element_text(size = 7))
ggsave(file.path(diag_plots, "leave_one_state_out_estimate.png"),
       gg_loo_est, width = 8.5, height = 9, dpi = 300)

# 6e. Cumulative receiver removal: static effect and pretrend p
cr <- cumulative_receiver[order(n_dropped)]
gg_cum <- ggplot(cr, aes(n_dropped, static_pct)) +
  geom_line(color = "#1D4ED8", linewidth = 0.7) +
  geom_point(color = "#1E3A8A", size = 2) +
  geom_vline(xintercept = 2, linetype = "dashed", color = "grey35") +
  labs(
    title = "Cumulative top-Florida-receiver removal",
    subtitle = "Static treated_post effect as the largest receivers are dropped; dashed = main-spec top-2",
    x = "Number of top pre-Ian Florida receivers dropped",
    y = "Static treated_post effect (%)"
  ) +
  theme_diag()
ggsave(file.path(diag_plots, "cumulative_receiver_removal_estimate.png"),
       gg_cum, width = 8, height = 4.8, dpi = 300)

gg_cum_pt <- ggplot(cr, aes(n_dropped, pretrend_p)) +
  geom_hline(yintercept = 0.1, color = "grey45", linewidth = 0.35) +
  geom_line(color = "#B45309", linewidth = 0.7) +
  geom_point(color = "#92400E", size = 2) +
  geom_vline(xintercept = 2, linetype = "dashed", color = "grey35") +
  labs(
    title = "Cumulative top-Florida-receiver removal",
    subtitle = "Joint pretrend p-value; grey = 0.10 threshold, dashed = main-spec top-2",
    x = "Number of top pre-Ian Florida receivers dropped",
    y = "Joint pretrend p-value"
  ) +
  theme_diag()
ggsave(file.path(diag_plots, "cumulative_receiver_removal_pretrend.png"),
       gg_cum_pt, width = 8, height = 4.8, dpi = 300)

# 6f. Influence ranking by US state
si <- copy(state_influence)
si[, sign := ifelse(influence >= 0, "raises beta", "lowers beta")]
gg_infl <- ggplot(
  si[1:min(25L, .N)],
  aes(influence, reorder(us_state, abs_influence), fill = sign)
) +
  geom_col() +
  scale_fill_manual(values = c("raises beta" = "#1D4ED8",
                               "lowers beta" = "#B91C1C"), name = NULL) +
  labs(
    title = "First-order influence on TWFE treated_post by US state",
    subtitle = paste0("beta_full = ", signif(beta_full, 4),
                       "; bar = approx d(beta) if that state removed"),
    x = "First-order influence on beta",
    y = NULL
  ) +
  theme_diag()
ggsave(file.path(diag_plots, "influence_by_us_state.png"),
       gg_infl, width = 8, height = 6, dpi = 300)

# =============================================================================
# 7. Console summary
# =============================================================================

cat("\n================ OUTLIER / INFLUENCE DIAGNOSTICS ================\n")

cat("\n--- Concentration summary ---\n")
print(concentration_summary)

cat("\n--- Top 10 Florida receivers (pre-Ian) ---\n")
print(florida_receiver[rank <= 10L,
  .(rank, receiver_id, pre_ian_share = round(pre_ian_share, 4),
    cum = round(cum_pre_ian_share, 4))])

cat("\n--- States with largest pre-Ian gap-trend slope vs Florida (top 10) ---\n")
print(state_divergence[1:10])

cat("\n--- Most influential US states on TWFE treated_post ---\n")
print(state_influence[1:10])

cat("\n--- Most influential single corridors ---\n")
print(corridor_influence[1:10, .(pair_id, influence, n_obs)])

cat("\n--- Leave-one-US-state-out: states that flip the pretrend decision ---\n")
flippers <- state_loo[flips_pretrend_decision == TRUE]
if (nrow(flippers) == 0L) {
  cat("None: no single control state flips the 10% pretrend decision.\n")
} else {
  print(flippers[, .(dropped_state, pretrend_p, static_pct,
                      d_pretrend_p_vs_base)])
}

cat("\n--- Leave-one-US-state-out: 8 largest pretrend-p movers ---\n")
print(head(state_loo[order(-abs(d_pretrend_p_vs_base))][
  dropped_state != "(none) baseline",
  .(dropped_state, pretrend_p, d_pretrend_p_vs_base, static_pct)], 8))

if (!is.null(sdid_loo)) {
  cat("\n--- Leave-one-donor-state-out: 8 largest SDID tau movers ---\n")
  print(head(sdid_loo[dropped_state != "(none) baseline"], 8))
}

cat("\n--- Cumulative top-Florida-receiver removal ---\n")
print(cumulative_receiver[, .(n_dropped, label, static_pct = round(static_pct, 2),
                              pretrend_p = round(pretrend_p, 4),
                              passes_pretrend_10pct)])

cat("\n--- Leave-one-receiver-out (top", top_k_receivers, "receivers) ---\n")
print(oneout_receiver[, .(label, static_pct = round(static_pct, 2),
                          pretrend_p = round(pretrend_p, 4),
                          passes_pretrend_10pct)])

cat("\nTables  ->", diag_tables, "\n")
cat("Plots   ->", diag_plots, "\n")
cat("================================================================\n")
