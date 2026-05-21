# Hurricane Ian remittances: PPML and positive-flows robustness + checks
#
# Follow-up to Outlier_diagnostics.R. The outlier probe showed the parallel
# trends failure is structural (pretrend p ~ 1e-184) and that static
# treated_post (-57%) is ~15x the event-study post average (-5.5%), pointing
# to log1p + zero-inflation manufacturing pre-trends rather than outliers.
#
# This script re-specifies the corridor TWFE three ways and reruns the
# decisive checks on each:
#   S0  log1p baseline                : feols  log1p(rem) ~ ... (reference)
#   S1  PPML                          : fepois rem ~ ...        (uses zeros)
#   S2  log positives, unbalanced     : feols  log(rem) ~ ...   (drop rem<=0)
#   S3  log positives, balanced panel : feols  log(rem) ~ ...   (corridors
#                                       strictly positive in every event qtr)
#
# Checks per spec:
#   - static treated_post estimate and % effect
#   - event-study coefficients + joint pretrend Wald p-value
#   - static vs average-post-coefficient discrepancy (the red flag)
#   - leave-one-US-state-out (full sweep for feols specs; 15 largest
#     control states for the slower PPML spec)
#   - sample sizes
#
# Self-contained; writes under outputs/outlier_diagnostics/ with ppml_/
# posflow_ prefixes. Does not touch Analysis.R outputs.

suppressPackageStartupMessages({
  library(data.table)
  library(fixest)
  library(ggplot2)
})

options(scipen = 999)
setFixest_notes(FALSE)

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

# Load and prepare (top-2 pre-Ian FL receivers excluded, as in main spec) -----

df <- fread(data_file)
df[
  ,
  `:=`(
    pair_id = paste(us_state, mx_state, mx_municipality, sep = "_"),
    receiver_id = paste(mx_state, mx_municipality, sep = " - "),
    is_treated = as.integer(us_state == "Florida"),
    q_num = (year - 2013L) * 4L + quarter
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
  by = receiver_id
][order(-pre_florida_usd)][1:2, receiver_id]
df <- df[!receiver_id %chin% excluded_receivers]

df_window <- df[time_to_treat %in% event_window]

# Strictly-positive balanced sample: corridors with rem > 0 in EVERY
# event-window quarter. Caveat (printed below): conditioning on post-period
# positivity is a selection-on-outcome; this spec is the intensive margin only.
n_window_q <- length(event_window)
pos_pairs <- df_window[
  remittances_usd > 0,
  .N,
  by = pair_id
][N == n_window_q, pair_id]

df_window[, log_rem := fifelse(remittances_usd > 0, log(remittances_usd), NA_real_)]
df_window[, log1p_rem := log1p(remittances_usd)]

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

theme_diag <- function() {
  theme_minimal(base_size = 12) +
    theme(legend.position = "top",
          panel.grid.minor = element_blank(),
          plot.title.position = "plot")
}

pretrend_pvalue <- function(model) {
  keep_pattern <- paste0(
    paste0("time_to_treat::", pretrend_window, ":is_treated"),
    collapse = "|"
  )
  out <- tryCatch(wald(model, keep = keep_pattern, print = FALSE),
                   error = function(e) NULL)
  if (is.null(out)) return(list(p.value = NA_real_, statistic = NA_real_))
  list(p.value = out$p, statistic = out$stat)
}

avg_post_coef <- function(model) {
  cf <- coef(model)
  terms <- paste0("time_to_treat::", post_event_window, ":is_treated")
  terms <- terms[terms %in% names(cf)]
  if (length(terms) == 0L) NA_real_ else mean(cf[terms])
}

# Fit one spec: returns static est, event coefs, pretrend p, sizes ------------
# estimator: "feols" or "fepois"; outcome is the LHS variable name; subset
# is a logical row filter on df_window.

fit_spec <- function(spec_id, label, estimator, outcome, subset_expr) {
  d <- df_window[eval(subset_expr)]
  est_fun <- if (estimator == "fepois") fepois else feols
  f_static <- as.formula(paste0(outcome, " ~ treated_post | pair_id + year_quarter"))
  f_event <- as.formula(paste0(
    outcome,
    " ~ i(time_to_treat, is_treated, ref = ", event_ref,
    ") | pair_id + year_quarter"
  ))

  m_s <- est_fun(f_static, data = d, cluster = ~ pair_id)
  m_e <- est_fun(f_event, data = d, cluster = ~ pair_id)
  pt <- pretrend_pvalue(m_e)

  b_static <- unname(coef(m_s)["treated_post"])
  b_postavg <- avg_post_coef(m_e)

  ev <- tidy_fixest(m_e)
  ev <- ev[grepl("^time_to_treat::", term)]
  ev[, time_to_treat := as.integer(sub(
    "^time_to_treat::(-?[0-9]+):is_treated$", "\\1", term
  ))]
  ev[, `:=`(
    calendar_quarter = quarter_label(time_to_treat),
    is_reference_period = FALSE,
    effect_pct = 100 * (exp(estimate) - 1),
    conf.low_pct = 100 * (exp(conf.low) - 1),
    conf.high_pct = 100 * (exp(conf.high) - 1)
  )]
  ev <- rbindlist(list(
    ev[, .(
      spec = spec_id, label = label, term, time_to_treat, calendar_quarter,
      is_reference_period, estimate, std.error, statistic, p.value,
      conf.low, conf.high, effect_pct, conf.low_pct, conf.high_pct
    )],
    data.table(spec = spec_id, label = label, term = "Reference period",
               time_to_treat = event_ref,
               calendar_quarter = quarter_label(event_ref),
               is_reference_period = TRUE,
               estimate = 0, std.error = NA_real_,
               statistic = NA_real_, p.value = NA_real_,
               conf.low = 0, conf.high = 0,
               effect_pct = 0, conf.low_pct = 0, conf.high_pct = 0)
  ), use.names = TRUE, fill = TRUE)
  setorder(ev, time_to_treat)

  list(
    summary = data.table(
      spec = spec_id,
      label = label,
      estimator = estimator,
      outcome = outcome,
      n_obs = nobs(m_s),
      n_corridors = length(unique(d$pair_id)),
      static_estimate = b_static,
      static_pct = 100 * (exp(b_static) - 1),
      avg_post_estimate = b_postavg,
      avg_post_pct = 100 * (exp(b_postavg) - 1),
      static_to_postavg_ratio = b_static / b_postavg,
      pretrend_p = pt$p.value,
      pretrend_stat = pt$statistic,
      passes_pretrend_10pct = isTRUE(pt$p.value > 0.1)
    ),
    event = ev
  )
}

specs <- list(
  list(id = "S0_log1p",    lab = "log1p(rem) baseline",
       est = "feols",  out = "log1p_rem", sub = quote(rep(TRUE, .N))),
  list(id = "S1_ppml",     lab = "PPML (levels, zeros kept)",
       est = "fepois", out = "remittances_usd", sub = quote(rep(TRUE, .N))),
  list(id = "S2_pos_unbal", lab = "log(rem), positives only (unbalanced)",
       est = "feols",  out = "log_rem", sub = quote(remittances_usd > 0)),
  list(id = "S3_pos_bal",  lab = "log(rem), strictly-positive balanced panel",
       est = "feols",  out = "log_rem", sub = quote(pair_id %chin% pos_pairs))
)

fits <- lapply(specs, function(s) {
  cat("Fitting", s$id, "-", s$lab, "...\n"); flush.console()
  fit_spec(s$id, s$lab, s$est, s$out, s$sub)
})

spec_summary <- rbindlist(lapply(fits, `[[`, "summary"))
event_all <- rbindlist(lapply(fits, `[[`, "event"))
fwrite(spec_summary, file.path(diag_tables, "ppml_posflow_spec_summary.csv"))
fwrite(event_all, file.path(diag_tables, "ppml_posflow_event_paths.csv"))

cat("\n--- Spec comparison (decisive checks) ---\n")
print(spec_summary[, .(
  spec, n_obs, n_corridors,
  static_pct = round(static_pct, 1),
  avg_post_pct = round(avg_post_pct, 1),
  static_postavg_ratio = round(static_to_postavg_ratio, 1),
  pretrend_p = signif(pretrend_p, 3),
  passes_pt = passes_pretrend_10pct
)])

# Event-study comparison plot -------------------------------------------------

ev_plot <- copy(event_all)
ev_plot[, spec := factor(spec, levels = sapply(specs, `[[`, "id"))]
gg_ev <- ggplot(ev_plot, aes(time_to_treat, estimate, color = label)) +
  geom_hline(yintercept = 0, color = "grey45", linewidth = 0.35) +
  geom_vline(xintercept = -0.5, linetype = "dashed", color = "grey35") +
  geom_errorbar(
    aes(ymin = conf.low, ymax = conf.high),
    width = 0.15,
    color = "#4B5563",
    linewidth = 0.35,
    alpha = 0.85
  ) +
  geom_line(linewidth = 0.6) +
  geom_point(aes(shape = is_reference_period), size = 1.4) +
  facet_wrap(~ label, scales = "free_y", ncol = 2) +
  scale_shape_manual(values = c("FALSE" = 16, "TRUE" = 21), guide = "none") +
  scale_x_continuous(breaks = seq(min(event_window), max(event_window), 2L)) +
  labs(
    title = "Event-study coefficients across specifications",
    subtitle = "Error bars are 95% confidence intervals. Pre-period (left of dashed line) should be flat at 0 under parallel trends.",
    x = "Quarters relative to Hurricane Ian (0 = 2022 Q4)",
    y = "Coefficient (log points / PPML semi-elasticity)",
    color = NULL
  ) +
  theme_diag() +
  theme(legend.position = "none")
ggsave(file.path(diag_plots, "ppml_posflow_event_comparison.png"),
       gg_ev, width = 11, height = 7, dpi = 300)

# Leave-one-US-state-out per spec --------------------------------------------
# Full sweep for the feols specs; for PPML restrict to the 15 largest
# control states (the only plausible single-state "outlier" drivers; the
# log1p probe already showed no single state flips the decision).

control_states <- sort(setdiff(unique(df_window$us_state), "Florida"))
state_volume <- df_window[
  us_state != "Florida",
  .(usd = sum(remittances_usd, na.rm = TRUE)),
  by = us_state
][order(-usd)]
ppml_loo_states <- head(state_volume$us_state, 15L)

loo_one_spec <- function(s, loo_states) {
  est_fun <- if (s$est == "fepois") fepois else feols
  f_static <- as.formula(paste0(s$out, " ~ treated_post | pair_id + year_quarter"))
  f_event <- as.formula(paste0(
    s$out, " ~ i(time_to_treat, is_treated, ref = ", event_ref,
    ") | pair_id + year_quarter"
  ))
  base_d <- df_window[eval(s$sub)]
  bm_s <- est_fun(f_static, data = base_d, cluster = ~ pair_id)
  bm_e <- est_fun(f_event, data = base_d, cluster = ~ pair_id)
  bpt <- pretrend_pvalue(bm_e)
  base_static_pct <- 100 * (exp(unname(coef(bm_s)["treated_post"])) - 1)
  base_pt <- bpt$p.value

  rows <- lapply(loo_states, function(st) {
    d <- base_d[us_state != st]
    m_s <- est_fun(f_static, data = d, cluster = ~ pair_id)
    m_e <- est_fun(f_event, data = d, cluster = ~ pair_id)
    pt <- pretrend_pvalue(m_e)
    data.table(
      spec = s$id,
      dropped_state = st,
      static_pct = 100 * (exp(unname(coef(m_s)["treated_post"])) - 1),
      pretrend_p = pt$p.value,
      passes_pretrend_10pct = isTRUE(pt$p.value > 0.1)
    )
  })
  out <- rbindlist(c(
    list(data.table(spec = s$id, dropped_state = "(none) baseline",
                     static_pct = base_static_pct, pretrend_p = base_pt,
                     passes_pretrend_10pct = isTRUE(base_pt > 0.1))),
    rows
  ))
  out[, `:=`(
    base_static_pct = base_static_pct,
    base_pretrend_p = base_pt,
    flips_pretrend_decision =
      passes_pretrend_10pct != isTRUE(base_pt > 0.1)
  )]
  out
}

loo_results <- rbindlist(lapply(specs, function(s) {
  ls <- if (s$est == "fepois") ppml_loo_states else control_states
  cat("Leave-one-state-out for", s$id, "(", length(ls), "states )...\n")
  flush.console()
  loo_one_spec(s, ls)
}))
fwrite(loo_results, file.path(diag_tables, "ppml_posflow_leave_one_state_out.csv"))

loo_digest <- loo_results[
  dropped_state != "(none) baseline",
  .(
    n_states = .N,
    static_pct_min = round(min(static_pct), 2),
    static_pct_max = round(max(static_pct), 2),
    pretrend_p_min = signif(min(pretrend_p), 3),
    pretrend_p_max = signif(max(pretrend_p), 3),
    n_states_flip_decision = sum(flips_pretrend_decision)
  ),
  by = spec
]
fwrite(loo_digest, file.path(diag_tables, "ppml_posflow_loo_digest.csv"))

# LOO pretrend plot (feols specs swept over all states) -----------------------

loo_plot_dt <- loo_results[
  spec %in% c("S0_log1p", "S2_pos_unbal", "S3_pos_bal") &
    dropped_state != "(none) baseline"
]
if (nrow(loo_plot_dt) > 0L) {
  gg_loo <- ggplot(loo_plot_dt,
                   aes(pretrend_p, dropped_state,
                       color = passes_pretrend_10pct)) +
    geom_vline(xintercept = 0.1, color = "grey45", linewidth = 0.35) +
    geom_point(size = 1.3) +
    facet_wrap(~ spec, scales = "free_x") +
    scale_color_manual(values = c("FALSE" = "#B91C1C", "TRUE" = "#15803D"),
                       name = "Pretrend p > 0.10") +
    labs(
      title = "Leave-one-US-state-out pretrend p-value, by specification",
      subtitle = "Grey line = 0.10 threshold. Each point is one control state removed.",
      x = "Joint pretrend p-value", y = NULL
    ) +
    theme_diag() +
    theme(axis.text.y = element_text(size = 5))
  ggsave(file.path(diag_plots, "ppml_posflow_loo_pretrend.png"),
         gg_loo, width = 12, height = 9, dpi = 300)
}

# Console summary -------------------------------------------------------------

cat("\n================ PPML / POSITIVE-FLOWS CHECKS ================\n")

cat("\nStrictly-positive balanced panel: ", length(pos_pairs),
    " corridors kept (of ", length(unique(df_window$pair_id)),
    "); selection-on-outcome caveat applies.\n", sep = "")

cat("\n--- Spec summary ---\n")
print(spec_summary[, .(
  spec, label, n_obs, n_corridors,
  static_pct = round(static_pct, 1),
  avg_post_pct = round(avg_post_pct, 1),
  pretrend_p = signif(pretrend_p, 3),
  passes_pt = passes_pretrend_10pct
)])

cat("\n--- Leave-one-state-out digest ---\n")
print(loo_digest)

cat("\n--- States that FLIP the pretrend decision (any spec) ---\n")
flips <- loo_results[flips_pretrend_decision == TRUE,
  .(spec, dropped_state, pretrend_p = signif(pretrend_p, 3),
    static_pct = round(static_pct, 2))]
if (nrow(flips) == 0L) {
  cat("None in any spec: no single control state flips the 10% decision.\n")
} else {
  print(flips)
}

cat("\nTables ->", diag_tables, "\nPlots  ->", diag_plots, "\n")
cat("=============================================================\n")
