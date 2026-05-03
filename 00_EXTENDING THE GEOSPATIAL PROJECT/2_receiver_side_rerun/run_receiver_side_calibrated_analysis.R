get_script_path <- function() {
  cmd_args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", cmd_args, value = TRUE)

  if (length(file_arg) > 0) {
    return(normalizePath(sub("^--file=", "", file_arg[1]), winslash = "/", mustWork = TRUE))
  }

  this_file <- tryCatch(
    normalizePath(sys.frame(1)$ofile, winslash = "/", mustWork = TRUE),
    error = function(...) NA_character_
  )

  if (!is.na(this_file)) {
    return(this_file)
  }

  normalizePath(
    file.path(
      getwd(),
      "00_EXTENDING THE GEOSPATIAL PROJECT",
      "2_receiver_side_rerun",
      "run_receiver_side_calibrated_analysis.R"
    ),
    winslash = "/",
    mustWork = FALSE
  )
}

safe_write_csv <- function(df, path) {
  readr::write_csv(df, path, na = "")
}

script_path <- get_script_path()
repo_root <- normalizePath(file.path(dirname(script_path), "..", ".."), winslash = "/", mustWork = TRUE)
extension_dir <- file.path(repo_root, "00_EXTENDING THE GEOSPATIAL PROJECT")
panel_dir <- file.path(extension_dir, "1_cleaned_panels")
input_dir <- file.path(extension_dir, "2_receiver_side_rerun", "input")
output_dir <- file.path(extension_dir, "2_receiver_side_rerun", "output")
dir.create(input_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

suppressPackageStartupMessages({
  library(tidyverse)
  library(fixest)
  library(did)
  library(readxl)
  library(broom)
})

message("Reading calibrated receiver-side panel...")
panel_path <- file.path(panel_dir, "panel_remittances_pga_calibrated.csv")
panel <- readr::read_csv(panel_path, show_col_types = FALSE)

missing_report <- panel %>%
  filter(is.na(remittances_musd)) %>%
  distinct(state, municipality)

safe_write_csv(
  missing_report,
  file.path(output_dir, "dropped_unmatched_receiver_municipalities.csv")
)

message("Dropping unmatched municipalities with missing calibrated remittances...")
panel <- panel %>%
  filter(!is.na(remittances_musd)) %>%
  mutate(event = na_if(event, ""))

message("Preparing panel exactly as in the original receiver-side specification...")
panel <- panel %>%
  rename(event_quarter = event) %>%
  mutate(
    muni_id = paste(state, municipality, sep = "_"),
    time_index = (year - min(year)) * 4 + quarter,
    asinhremit = asinh_remittances
  )

partitions <- list(
  "4-10" = c(4, 10),
  "10-20" = c(10, 20),
  "20+" = c(20, Inf)
)

PGA_THRESHOLD <- 4

muni_meta <- panel %>%
  filter(!is.na(event_quarter), mean_pga > PGA_THRESHOLD) %>%
  group_by(muni_id) %>%
  slice_max(mean_pga, n = 1, with_ties = FALSE) %>%
  transmute(
    muni_id,
    event = event_quarter,
    dose = mean_pga,
    G = time_index
  ) %>%
  ungroup()

panel <- panel %>%
  left_join(muni_meta, by = "muni_id")

panel_cs <- panel %>%
  mutate(
    G_cs = ifelse(is.na(event), 0L, as.integer(G)),
    muni_num = as.integer(factor(muni_id))
  )

run_cs_partition <- function(data, pga_lo, pga_hi, partition_label,
                             pre_window = -10, post_window = 10) {
  treated_in_partition <- data %>%
    filter(!is.na(event), dose > pga_lo, dose <= pga_hi) %>%
    distinct(muni_id)

  if (nrow(treated_in_partition) == 0) {
    return(NULL)
  }

  sample_df <- data %>%
    filter(muni_id %in% treated_in_partition$muni_id | G_cs == 0) %>%
    mutate(G_cs = ifelse(muni_id %in% treated_in_partition$muni_id, G_cs, 0L))

  n_treated <- n_distinct(sample_df$muni_id[sample_df$G_cs > 0])
  n_controls <- n_distinct(sample_df$muni_id[sample_df$G_cs == 0])
  n_cohorts <- n_distinct(sample_df$G_cs[sample_df$G_cs > 0])

  if (n_treated < 2) {
    return(NULL)
  }

  cs_out <- tryCatch(
    att_gt(
      yname = "asinhremit",
      tname = "time_index",
      idname = "muni_num",
      gname = "G_cs",
      data = sample_df,
      control_group = "nevertreated",
      est_method = "dr",
      clustervars = "muni_num",
      print_details = FALSE
    ),
    error = function(e) NULL
  )

  if (is.null(cs_out)) {
    return(NULL)
  }

  es <- tryCatch(
    aggte(cs_out, type = "dynamic", min_e = pre_window, max_e = post_window, na.rm = TRUE),
    error = function(e) NULL
  )

  agg <- NULL
  if (!is.null(es)) {
    post_idx <- which(es$egt >= 0 & es$egt <= post_window)
    if (length(post_idx) > 0) {
      post_atts <- es$att.egt[post_idx]
      post_att <- mean(post_atts, na.rm = TRUE)
      post_se <- sqrt(mean(es$se.egt[post_idx]^2, na.rm = TRUE) / length(post_idx))
      agg <- list(overall.att = post_att, overall.se = post_se)
    }
  }

  list(
    partition = partition_label,
    n_treated = n_treated,
    n_controls = n_controls,
    n_cohorts = n_cohorts,
    cs_out = cs_out,
    agg = agg,
    es = es
  )
}

message("Running baseline Callaway-Sant'Anna partitions...")
cs_results <- purrr::map(names(partitions), function(part_name) {
  bounds <- partitions[[part_name]]
  run_cs_partition(
    data = panel_cs,
    pga_lo = bounds[1],
    pga_hi = bounds[2],
    partition_label = part_name,
    pre_window = -10,
    post_window = 10
  )
}) %>%
  setNames(names(partitions)) %>%
  purrr::compact()

att_table <- purrr::map_dfr(cs_results, function(res) {
  if (is.null(res$agg)) {
    return(NULL)
  }
  tibble(
    partition = res$partition,
    n_cohorts = res$n_cohorts,
    n_treated = res$n_treated,
    n_controls = res$n_controls,
    att = res$agg$overall.att,
    se = res$agg$overall.se,
    ci_lo = res$agg$overall.att - 1.96 * res$agg$overall.se,
    ci_hi = res$agg$overall.att + 1.96 * res$agg$overall.se,
    p_val = 2 * pnorm(-abs(res$agg$overall.att / res$agg$overall.se))
  )
}) %>%
  mutate(
    sig = case_when(
      p_val < 0.01 ~ "***",
      p_val < 0.05 ~ "**",
      p_val < 0.10 ~ "*",
      TRUE ~ ""
    ),
    partition = factor(partition, levels = c("4-10", "10-20", "20+"))
  ) %>%
  arrange(partition)

safe_write_csv(att_table, file.path(output_dir, "receiver_att_table_calibrated.csv"))

dynamic_baseline <- purrr::map_dfr(cs_results, function(res) {
  if (is.null(res$es)) {
    return(NULL)
  }
  tibble(
    partition = res$partition,
    egt = res$es$egt,
    att = res$es$att.egt,
    se = res$es$se.egt,
    ci_lo = res$es$att.egt - 1.96 * res$es$se.egt,
    ci_hi = res$es$att.egt + 1.96 * res$es$se.egt
  )
})
safe_write_csv(dynamic_baseline, file.path(output_dir, "receiver_event_study_baseline_calibrated.csv"))

message("Running low-dose-controls robustness specification...")
panel_cs_ld <- panel %>%
  mutate(
    G_cs = case_when(
      is.na(event) ~ 0L,
      dose > 4 & dose <= 10 ~ 0L,
      TRUE ~ as.integer(G)
    ),
    muni_num = as.integer(factor(muni_id))
  )

partitions_ld <- list(
  "10-20" = c(10, 20),
  "20+" = c(20, Inf)
)

run_cs_partition_ld <- function(data, pga_lo, pga_hi, partition_label,
                                pre_window = -10, post_window = 10) {
  treated_in_partition <- data %>%
    filter(!is.na(event), dose > pga_lo, dose <= pga_hi) %>%
    distinct(muni_id)

  if (nrow(treated_in_partition) == 0) {
    return(NULL)
  }

  other_high_dose <- data %>%
    filter(!is.na(event), dose > 10, !(muni_id %in% treated_in_partition$muni_id)) %>%
    distinct(muni_id)

  sample_df <- data %>%
    filter(muni_id %in% treated_in_partition$muni_id | G_cs == 0) %>%
    filter(!(muni_id %in% other_high_dose$muni_id)) %>%
    mutate(G_cs = ifelse(muni_id %in% treated_in_partition$muni_id, G_cs, 0L))

  n_treated <- n_distinct(sample_df$muni_id[sample_df$G_cs > 0])
  n_controls <- n_distinct(sample_df$muni_id[sample_df$G_cs == 0])
  n_cohorts <- n_distinct(sample_df$G_cs[sample_df$G_cs > 0])

  if (n_treated < 2) {
    return(NULL)
  }

  cs_out <- tryCatch(
    att_gt(
      yname = "asinhremit",
      tname = "time_index",
      idname = "muni_num",
      gname = "G_cs",
      data = sample_df,
      control_group = "nevertreated",
      est_method = "dr",
      clustervars = "muni_num",
      print_details = FALSE
    ),
    error = function(e) NULL
  )

  if (is.null(cs_out)) {
    return(NULL)
  }

  es <- tryCatch(
    aggte(cs_out, type = "dynamic", min_e = pre_window, max_e = post_window, na.rm = TRUE),
    error = function(e) NULL
  )

  agg <- NULL
  if (!is.null(es)) {
    post_idx <- which(es$egt >= 0 & es$egt <= post_window)
    if (length(post_idx) > 0) {
      post_atts <- es$att.egt[post_idx]
      post_att <- mean(post_atts, na.rm = TRUE)
      post_se <- sqrt(mean(es$se.egt[post_idx]^2, na.rm = TRUE) / length(post_idx))
      agg <- list(overall.att = post_att, overall.se = post_se)
    }
  }

  list(
    partition = partition_label,
    n_treated = n_treated,
    n_controls = n_controls,
    n_cohorts = n_cohorts,
    cs_out = cs_out,
    agg = agg,
    es = es
  )
}

cs_results_ld <- purrr::map(names(partitions_ld), function(part_name) {
  bounds <- partitions_ld[[part_name]]
  run_cs_partition_ld(
    data = panel_cs_ld,
    pga_lo = bounds[1],
    pga_hi = bounds[2],
    partition_label = part_name,
    pre_window = -10,
    post_window = 10
  )
}) %>%
  setNames(names(partitions_ld)) %>%
  purrr::compact()

att_table_ld <- purrr::map_dfr(cs_results_ld, function(res) {
  if (is.null(res$agg)) {
    return(NULL)
  }
  tibble(
    partition = res$partition,
    n_cohorts = res$n_cohorts,
    n_treated = res$n_treated,
    n_controls = res$n_controls,
    att = res$agg$overall.att,
    se = res$agg$overall.se,
    ci_lo = res$agg$overall.att - 1.96 * res$agg$overall.se,
    ci_hi = res$agg$overall.att + 1.96 * res$agg$overall.se,
    p_val = 2 * pnorm(-abs(res$agg$overall.att / res$agg$overall.se))
  )
}) %>%
  mutate(
    sig = case_when(
      p_val < 0.01 ~ "***",
      p_val < 0.05 ~ "**",
      p_val < 0.10 ~ "*",
      TRUE ~ ""
    ),
    partition = factor(partition, levels = c("10-20", "20+"))
  ) %>%
  arrange(partition)

safe_write_csv(att_table_ld, file.path(output_dir, "receiver_att_table_low_dose_controls_calibrated.csv"))

dynamic_ld <- purrr::map_dfr(cs_results_ld, function(res) {
  if (is.null(res$es)) {
    return(NULL)
  }
  tibble(
    partition = res$partition,
    egt = res$es$egt,
    att = res$es$att.egt,
    se = res$es$se.egt,
    ci_lo = res$es$att.egt - 1.96 * res$es$se.egt,
    ci_hi = res$es$att.egt + 1.96 * res$es$se.egt
  )
})
safe_write_csv(dynamic_ld, file.path(output_dir, "receiver_event_study_low_dose_controls_calibrated.csv"))

message("Running spillover regressions with the same specifications...")
W_geo <- readRDS(file.path(input_dir, "W_geo.rds"))

dose_lookup <- panel %>%
  mutate(muni_id = paste(state, municipality, sep = "_")) %>%
  group_by(muni_id) %>%
  summarise(dose = max(mean_pga, na.rm = TRUE), .groups = "drop")

dose_vector_geo <- setNames(rep(0, nrow(W_geo)), rownames(W_geo))
matched_geo <- intersect(rownames(W_geo), dose_lookup$muni_id)
dose_vector_geo[matched_geo] <- dose_lookup$dose[match(matched_geo, dose_lookup$muni_id)]

geo_pga_df <- tibble(
  muni_id = rownames(W_geo),
  geo_pga = as.vector(W_geo %*% dose_vector_geo)
)

spillover_df <- panel_cs %>%
  left_join(geo_pga_df, by = "muni_id") %>%
  filter(G_cs == 0, !is.na(geo_pga)) %>%
  mutate(
    post = as.integer(time_index >= min(muni_meta$G)),
    geo_pga_c = geo_pga - mean(geo_pga, na.rm = TRUE)
  )

spillover_mod_geo <- feols(
  asinhremit ~ post:geo_pga_c | muni_num + time_index,
  data = spillover_df,
  cluster = ~muni_num
)

M <- readxl::read_excel(file.path(input_dir, "AVG_WEIGHTING_MATRIX.xlsx"))
muni_ids <- M[[2]]
state_ids <- M[[1]]
muni_labels <- paste(state_ids, muni_ids, sep = "_")
M <- as.matrix(M[c(-1, -2)])
W_network <- M %*% t(M)
rownames(W_network) <- muni_labels
colnames(W_network) <- muni_labels
diag(W_network) <- 0
W_network <- W_network / rowSums(W_network)

dose_vector_net <- setNames(rep(0, nrow(W_network)), rownames(W_network))
matched_net <- intersect(rownames(W_network), dose_lookup$muni_id)
dose_vector_net[matched_net] <- dose_lookup$dose[match(matched_net, dose_lookup$muni_id)]

network_pga_df <- tibble(
  muni_id = rownames(W_network),
  network_pga = as.vector(W_network %*% dose_vector_net)
)

spillover_df_net <- panel_cs %>%
  left_join(network_pga_df, by = "muni_id") %>%
  filter(G_cs == 0, !is.na(network_pga)) %>%
  mutate(
    post = as.integer(time_index >= min(muni_meta$G)),
    network_pga_c = network_pga - mean(network_pga, na.rm = TRUE)
  )

spillover_mod_net <- feols(
  asinhremit ~ post:network_pga_c | muni_num + time_index,
  data = spillover_df_net,
  cluster = ~muni_num
)

spillover_df_joint <- panel_cs %>%
  left_join(network_pga_df, by = "muni_id") %>%
  left_join(geo_pga_df, by = "muni_id") %>%
  filter(G_cs == 0, !is.na(network_pga), !is.na(geo_pga)) %>%
  mutate(
    post = as.integer(time_index >= min(muni_meta$G)),
    network_pga_c = network_pga - mean(network_pga, na.rm = TRUE),
    geo_pga_c = geo_pga - mean(geo_pga, na.rm = TRUE)
  )

spillover_mod_joint <- feols(
  asinhremit ~ post:network_pga_c + post:geo_pga_c | muni_num + time_index,
  data = spillover_df_joint,
  cluster = ~muni_num
)

spillover_table <- bind_rows(
  tidy(spillover_mod_geo, conf.int = TRUE) %>% mutate(model = "geographic"),
  tidy(spillover_mod_net, conf.int = TRUE) %>% mutate(model = "network"),
  tidy(spillover_mod_joint, conf.int = TRUE) %>% mutate(model = "joint")
) %>%
  filter(term %in% c("post:geo_pga_c", "post:network_pga_c"))

safe_write_csv(spillover_table, file.path(output_dir, "receiver_spillover_results_calibrated.csv"))

summary_lines <- c(
  "# Receiver-side calibrated rerun",
  "",
  paste0("Analysis date: ", Sys.Date()),
  paste0("Panel rows used: ", nrow(panel)),
  paste0("Matched municipalities retained: ", n_distinct(panel$muni_id)),
  paste0("Dropped unmatched municipalities: ", nrow(missing_report)),
  "",
  "## Baseline ATT estimates",
  capture.output(print(att_table)),
  "",
  "## Low-dose-controls ATT estimates",
  capture.output(print(att_table_ld)),
  "",
  "## Spillover coefficients",
  capture.output(print(spillover_table))
)

writeLines(summary_lines, file.path(output_dir, "receiver_side_calibrated_summary.md"))

message("Receiver-side calibrated rerun complete.")
