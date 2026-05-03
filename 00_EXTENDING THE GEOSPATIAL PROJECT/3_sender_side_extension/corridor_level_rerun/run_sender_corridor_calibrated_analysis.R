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
      "3_sender_side_extension",
      "corridor_level_rerun",
      "run_sender_corridor_calibrated_analysis.R"
    ),
    winslash = "/",
    mustWork = FALSE
  )
}

safe_write_csv <- function(df, path) {
  readr::write_csv(df, path, na = "")
}

safe_save_plot <- function(plot_obj, path, width = 7, height = 5, dpi = 300) {
  ggplot2::ggsave(
    filename = path,
    plot = plot_obj,
    width = width,
    height = height,
    dpi = dpi
  )
}

sig_stars <- function(p_val) {
  dplyr::case_when(
    p_val < 0.01 ~ "***",
    p_val < 0.05 ~ "**",
    p_val < 0.10 ~ "*",
    TRUE ~ ""
  )
}

build_cohort_table <- function(results_list, lookup_df, counts_df, count_name) {
  purrr::map_dfr(results_list, function(res) {
    if (is.null(res$group)) {
      return(NULL)
    }

    tibble::tibble(
      partition = res$partition,
      cohort_time_index = as.integer(res$group$egt),
      att = res$group$att.egt,
      se = res$group$se.egt
    ) %>%
      dplyr::left_join(lookup_df, by = "cohort_time_index") %>%
      dplyr::left_join(counts_df, by = c("partition", "cohort_time_index")) %>%
      dplyr::mutate(
        ci_lo = att - 1.96 * se,
        ci_hi = att + 1.96 * se,
        p_val = 2 * pnorm(-abs(att / se)),
        sig = sig_stars(p_val)
      ) %>%
      dplyr::rename(!!count_name := cohort_treated_ids) %>%
      dplyr::select(
        partition,
        cohort_events,
        year,
        quarter,
        cohort_time_index,
        dplyr::all_of(count_name),
        att,
        se,
        ci_lo,
        ci_hi,
        p_val,
        sig
      ) %>%
      dplyr::arrange(partition, cohort_time_index)
  })
}

save_event_study_plots <- function(results_list, plot_dir, spec_slug, spec_label) {
  purrr::walk(results_list, function(res) {
    if (is.null(res$es)) {
      return(invisible(NULL))
    }

    plot_obj <- ggdid(res$es) +
      labs(
        title = paste("Sender Corridor Event Study - PGA Partition:", res$partition),
        subtitle = paste0(
          "CS (2021) | ",
          spec_label,
          " | ",
          res$n_treated,
          " treated, ",
          res$n_controls,
          " control corridors"
        ),
        x = "Quarters relative to earthquake",
        y = "ATT (Inverse hyperbolic sine remittances)"
      ) +
      theme_bw(base_size = 11) +
      theme(
        plot.title = element_text(hjust = 0.5),
        plot.subtitle = element_text(hjust = 0.5, size = 8),
        legend.position = "bottom"
      )

    file_stub <- gsub("[^A-Za-z0-9]+", "_", res$partition)
    safe_save_plot(
      plot_obj,
      file.path(plot_dir, paste0("sender_corridor_event_study_", spec_slug, "_", file_stub, ".png"))
    )
  })
}

save_att_plot <- function(att_df, plot_dir, spec_slug, title_text, subtitle_text) {
  if (nrow(att_df) == 0) {
    return(invisible(NULL))
  }

  plot_obj <- ggplot(att_df, aes(x = partition, y = att)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "grey50") +
    geom_errorbar(
      aes(ymin = ci_lo, ymax = ci_hi),
      width = 0.15,
      color = "steelblue",
      linewidth = 0.8
    ) +
    geom_point(color = "steelblue", size = 3) +
    geom_text(aes(label = sig, y = ci_hi + 0.01), size = 5, color = "steelblue") +
    labs(
      title = title_text,
      subtitle = subtitle_text,
      x = "PGA Partition (%g)",
      y = "ATT (Inverse hyperbolic sine remittances)"
    ) +
    theme_bw() +
    theme(
      plot.title = element_text(hjust = 0.5),
      plot.subtitle = element_text(hjust = 0.5)
    )

  safe_save_plot(
    plot_obj,
    file.path(plot_dir, paste0("sender_corridor_att_comparison_", spec_slug, ".png")),
    width = 6.5,
    height = 4.5
  )
}

run_cs_partition <- function(data, pga_lo, pga_hi, partition_label,
                             pre_window = -10, post_window = 10) {
  treated_in_partition <- data %>%
    dplyr::filter(!is.na(event), dose > pga_lo, dose <= pga_hi) %>%
    dplyr::distinct(corridor_id)

  if (nrow(treated_in_partition) == 0) {
    return(NULL)
  }

  sample_df <- data %>%
    dplyr::filter(corridor_id %in% treated_in_partition$corridor_id | G_cs == 0) %>%
    dplyr::mutate(G_cs = ifelse(corridor_id %in% treated_in_partition$corridor_id, G_cs, 0))

  model_df <- sample_df %>%
    dplyr::select(asinhremit, time_index, corridor_num, G_cs, corridor_id) %>%
    dplyr::mutate(
      time_index = as.numeric(time_index),
      corridor_num = as.numeric(corridor_num),
      G_cs = as.numeric(G_cs)
    ) %>%
    as.data.frame()

  n_treated <- dplyr::n_distinct(model_df$corridor_id[model_df$G_cs > 0])
  n_controls <- dplyr::n_distinct(model_df$corridor_id[model_df$G_cs == 0])
  n_cohorts <- dplyr::n_distinct(model_df$G_cs[model_df$G_cs > 0])

  if (n_treated < 2) {
    return(NULL)
  }

  cs_out <- tryCatch(
    att_gt(
      yname = "asinhremit",
      tname = "time_index",
      idname = "corridor_num",
      gname = "G_cs",
      data = model_df,
      control_group = "nevertreated",
      est_method = "dr",
      clustervars = "corridor_num",
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

  group <- tryCatch(
    aggte(cs_out, type = "group", na.rm = TRUE),
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
    es = es,
    group = group
  )
}

run_cs_partition_ld <- function(data, pga_lo, pga_hi, partition_label,
                                pre_window = -10, post_window = 10) {
  treated_in_partition <- data %>%
    dplyr::filter(!is.na(event), dose > pga_lo, dose <= pga_hi) %>%
    dplyr::distinct(corridor_id)

  if (nrow(treated_in_partition) == 0) {
    return(NULL)
  }

  other_high_dose <- data %>%
    dplyr::filter(!is.na(event), dose > 10, !(corridor_id %in% treated_in_partition$corridor_id)) %>%
    dplyr::distinct(corridor_id)

  sample_df <- data %>%
    dplyr::filter(corridor_id %in% treated_in_partition$corridor_id | G_cs == 0) %>%
    dplyr::filter(!(corridor_id %in% other_high_dose$corridor_id)) %>%
    dplyr::mutate(G_cs = ifelse(corridor_id %in% treated_in_partition$corridor_id, G_cs, 0))

  model_df <- sample_df %>%
    dplyr::select(asinhremit, time_index, corridor_num, G_cs, corridor_id) %>%
    dplyr::mutate(
      time_index = as.numeric(time_index),
      corridor_num = as.numeric(corridor_num),
      G_cs = as.numeric(G_cs)
    ) %>%
    as.data.frame()

  n_treated <- dplyr::n_distinct(model_df$corridor_id[model_df$G_cs > 0])
  n_controls <- dplyr::n_distinct(model_df$corridor_id[model_df$G_cs == 0])
  n_cohorts <- dplyr::n_distinct(model_df$G_cs[model_df$G_cs > 0])

  if (n_treated < 2) {
    return(NULL)
  }

  cs_out <- tryCatch(
    att_gt(
      yname = "asinhremit",
      tname = "time_index",
      idname = "corridor_num",
      gname = "G_cs",
      data = model_df,
      control_group = "nevertreated",
      est_method = "dr",
      clustervars = "corridor_num",
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

  group <- tryCatch(
    aggte(cs_out, type = "group", na.rm = TRUE),
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
    es = es,
    group = group
  )
}

script_path <- get_script_path()
repo_root <- normalizePath(file.path(dirname(script_path), "..", "..", ".."), winslash = "/", mustWork = TRUE)
extension_dir <- file.path(repo_root, "00_EXTENDING THE GEOSPATIAL PROJECT", "3_sender_side_extension", "corridor_level_rerun")
output_dir <- file.path(extension_dir, "output")
plot_dir <- file.path(output_dir, "plots")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)

suppressPackageStartupMessages({
  library(tidyverse)
  library(fixest)
  library(did)
  library(readxl)
  library(broom)
})

panel_path <- file.path(output_dir, "panel_sender_corridor_pga_calibrated.csv")
if (!file.exists(panel_path)) {
  build_script <- file.path(extension_dir, "build_sender_corridor_panel_pga.R")
  message("Panel not found. Building corridor panel first...")
  source(build_script, local = new.env(parent = globalenv()))
}

message("Reading calibrated sender corridor panel...")
panel <- readr::read_csv(panel_path, show_col_types = FALSE)

dropped_corridors <- panel %>%
  filter(is.na(mean_pga)) %>%
  distinct(us_state, state, municipality)
safe_write_csv(
  dropped_corridors,
  file.path(output_dir, "dropped_unmatched_sender_corridors.csv")
)

message("Dropping unmatched corridors with missing earthquake exposure...")
panel <- panel %>%
  filter(!is.na(mean_pga)) %>%
  mutate(event = na_if(event, ""))

message("Preparing corridor panel for Callaway-Sant'Anna estimation...")
panel <- panel %>%
  rename(event_quarter = event) %>%
  mutate(
    corridor_id = paste(us_state, state, municipality, sep = "_"),
    recipient_id = paste(state, municipality, sep = "_"),
    time_index = (year - min(year)) * 4 + quarter,
    asinhremit = asinh_remittances
  )

partitions <- list(
  "4-10" = c(4, 10),
  "10-20" = c(10, 20),
  "20+" = c(20, Inf)
)

PGA_THRESHOLD <- 4

recipient_meta <- panel %>%
  filter(!is.na(event_quarter), mean_pga > PGA_THRESHOLD) %>%
  group_by(recipient_id) %>%
  slice_max(mean_pga, n = 1, with_ties = FALSE) %>%
  transmute(
    recipient_id,
    event = event_quarter,
    dose = mean_pga,
    G = time_index
  ) %>%
  ungroup()

panel <- panel %>%
  left_join(recipient_meta, by = "recipient_id")

panel_cs <- panel %>%
  mutate(
    G_cs = ifelse(is.na(event), 0, as.numeric(G)),
    corridor_num = as.numeric(factor(corridor_id))
  )

time_lookup <- panel %>%
  distinct(time_index, year, quarter)

cohort_lookup <- recipient_meta %>%
  distinct(event, G) %>%
  rename(event_quarter = event, cohort_time_index = G) %>%
  left_join(time_lookup, by = c("cohort_time_index" = "time_index")) %>%
  group_by(cohort_time_index, year, quarter) %>%
  summarise(
    cohort_events = paste(sort(unique(event_quarter)), collapse = "; "),
    .groups = "drop"
  )

cohort_counts <- purrr::map_dfr(names(partitions), function(part_name) {
  bounds <- partitions[[part_name]]
  panel %>%
    distinct(corridor_id, recipient_id, dose, G) %>%
    filter(!is.na(G), dose > bounds[1], dose <= bounds[2]) %>%
    count(G, name = "cohort_treated_ids") %>%
    rename(cohort_time_index = G) %>%
    mutate(partition = part_name)
})

treated_overview <- panel %>%
  distinct(corridor_id, us_state, state, municipality, recipient_id, dose, G, event) %>%
  filter(!is.na(G)) %>%
  mutate(
    partition = case_when(
      dose > 4 & dose <= 10 ~ "4-10",
      dose > 10 & dose <= 20 ~ "10-20",
      dose > 20 ~ "20+",
      TRUE ~ "control"
    )
  ) %>%
  arrange(partition, desc(dose), us_state, state, municipality)
safe_write_csv(treated_overview, file.path(output_dir, "sender_corridor_treated_overview.csv"))

message("Running baseline corridor-level Callaway-Sant'Anna partitions...")
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
    sig = sig_stars(p_val),
    partition = factor(partition, levels = c("4-10", "10-20", "20+"))
  ) %>%
  arrange(partition)
safe_write_csv(att_table, file.path(output_dir, "sender_corridor_att_table_calibrated.csv"))

cohort_att_table <- build_cohort_table(
  cs_results,
  cohort_lookup,
  cohort_counts,
  count_name = "cohort_treated_corridors"
)
safe_write_csv(
  cohort_att_table,
  file.path(output_dir, "sender_corridor_att_by_cohort_calibrated.csv")
)

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
safe_write_csv(dynamic_baseline, file.path(output_dir, "sender_corridor_event_study_baseline_calibrated.csv"))
save_event_study_plots(cs_results, plot_dir, "baseline", "never-treated controls")
save_att_plot(
  att_table,
  plot_dir,
  "baseline",
  "Sender Corridor CS ATT by PGA Partition",
  "Callaway & Sant'Anna (2021) | US state x municipality x quarter | never-treated controls"
)

message("Running low-dose-controls robustness specification...")
panel_cs_ld <- panel %>%
  mutate(
    G_cs = case_when(
      is.na(event) ~ 0,
      dose > 4 & dose <= 10 ~ 0,
      TRUE ~ as.numeric(G)
    ),
    corridor_num = as.numeric(factor(corridor_id))
  )

partitions_ld <- list(
  "10-20" = c(10, 20),
  "20+" = c(20, Inf)
)

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
    sig = sig_stars(p_val),
    partition = factor(partition, levels = c("10-20", "20+"))
  ) %>%
  arrange(partition)
safe_write_csv(att_table_ld, file.path(output_dir, "sender_corridor_att_table_low_dose_controls_calibrated.csv"))

cohort_counts_ld <- purrr::map_dfr(names(partitions_ld), function(part_name) {
  bounds <- partitions_ld[[part_name]]
  panel %>%
    distinct(corridor_id, recipient_id, dose, G) %>%
    filter(!is.na(G), dose > bounds[1], dose <= bounds[2]) %>%
    count(G, name = "cohort_treated_ids") %>%
    rename(cohort_time_index = G) %>%
    mutate(partition = part_name)
})

cohort_att_table_ld <- build_cohort_table(
  cs_results_ld,
  cohort_lookup,
  cohort_counts_ld,
  count_name = "cohort_treated_corridors"
)
safe_write_csv(
  cohort_att_table_ld,
  file.path(output_dir, "sender_corridor_att_by_cohort_low_dose_controls_calibrated.csv")
)

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
safe_write_csv(dynamic_ld, file.path(output_dir, "sender_corridor_event_study_low_dose_controls_calibrated.csv"))
save_event_study_plots(cs_results_ld, plot_dir, "low_dose_controls", "low-dose (4-10) controls")
save_att_plot(
  att_table_ld,
  plot_dir,
  "low_dose_controls",
  "Sender Corridor CS ATT by PGA Partition (low-dose controls)",
  "Callaway & Sant'Anna (2021) | US state x municipality x quarter | low-dose controls"
)

message("Running corridor-level spillover regressions...")
W_geo <- readRDS(file.path(repo_root, "00_EXTENDING THE GEOSPATIAL PROJECT", "2_receiver_side_rerun", "input", "W_geo.rds"))

dose_lookup <- panel %>%
  group_by(recipient_id) %>%
  summarise(dose = max(mean_pga, na.rm = TRUE), .groups = "drop")

dose_vector_geo <- setNames(rep(0, nrow(W_geo)), rownames(W_geo))
matched_geo <- intersect(rownames(W_geo), dose_lookup$recipient_id)
dose_vector_geo[matched_geo] <- dose_lookup$dose[match(matched_geo, dose_lookup$recipient_id)]

geo_pga_df <- tibble(
  recipient_id = rownames(W_geo),
  geo_pga = as.vector(W_geo %*% dose_vector_geo)
)

M <- readxl::read_excel(file.path(repo_root, "00_EXTENDING THE GEOSPATIAL PROJECT", "2_receiver_side_rerun", "input", "AVG_WEIGHTING_MATRIX.xlsx"))
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
matched_net <- intersect(rownames(W_network), dose_lookup$recipient_id)
dose_vector_net[matched_net] <- dose_lookup$dose[match(matched_net, dose_lookup$recipient_id)]

network_pga_df <- tibble(
  recipient_id = rownames(W_network),
  network_pga = as.vector(W_network %*% dose_vector_net)
)

first_treated_time <- min(recipient_meta$G, na.rm = TRUE)

spillover_df_geo <- panel_cs %>%
  left_join(geo_pga_df, by = "recipient_id") %>%
  filter(G_cs == 0, !is.na(geo_pga)) %>%
  mutate(
    post = as.integer(time_index >= first_treated_time),
    geo_pga_c = geo_pga - mean(geo_pga, na.rm = TRUE)
  )

spillover_mod_geo <- feols(
  asinhremit ~ post:geo_pga_c | corridor_num + time_index,
  data = spillover_df_geo,
  cluster = ~corridor_num
)

spillover_df_net <- panel_cs %>%
  left_join(network_pga_df, by = "recipient_id") %>%
  filter(G_cs == 0, !is.na(network_pga)) %>%
  mutate(
    post = as.integer(time_index >= first_treated_time),
    network_pga_c = network_pga - mean(network_pga, na.rm = TRUE)
  )

spillover_mod_net <- feols(
  asinhremit ~ post:network_pga_c | corridor_num + time_index,
  data = spillover_df_net,
  cluster = ~corridor_num
)

spillover_df_joint <- panel_cs %>%
  left_join(network_pga_df, by = "recipient_id") %>%
  left_join(geo_pga_df, by = "recipient_id") %>%
  filter(G_cs == 0, !is.na(network_pga), !is.na(geo_pga)) %>%
  mutate(
    post = as.integer(time_index >= first_treated_time),
    network_pga_c = network_pga - mean(network_pga, na.rm = TRUE),
    geo_pga_c = geo_pga - mean(geo_pga, na.rm = TRUE)
  )

spillover_mod_joint <- feols(
  asinhremit ~ post:network_pga_c + post:geo_pga_c | corridor_num + time_index,
  data = spillover_df_joint,
  cluster = ~corridor_num
)

spillover_table <- bind_rows(
  tidy(spillover_mod_geo, conf.int = TRUE) %>% mutate(model = "geographic"),
  tidy(spillover_mod_net, conf.int = TRUE) %>% mutate(model = "network"),
  tidy(spillover_mod_joint, conf.int = TRUE) %>% mutate(model = "joint")
) %>%
  filter(term %in% c("post:geo_pga_c", "post:network_pga_c"))
safe_write_csv(spillover_table, file.path(output_dir, "sender_corridor_spillover_results_calibrated.csv"))

summary_lines <- c(
  "# Sender corridor-level calibrated rerun",
  "",
  paste0("Analysis date: ", Sys.Date()),
  paste0("Panel rows used: ", nrow(panel)),
  paste0("Unique corridors retained: ", n_distinct(panel$corridor_id)),
  paste0("Unique treated recipient municipalities: ", n_distinct(recipient_meta$recipient_id)),
  paste0("Dropped unmatched corridors: ", nrow(dropped_corridors)),
  "",
  "## Main design note",
  "This rerun uses the US state x Mexican municipality x quarter corridor as the unit of analysis.",
  "Treatment is assigned from the recipient municipality earthquake exposure, while the outcome is the remittance flow sent along that specific corridor.",
  "",
  "## Baseline ATT estimates",
  capture.output(print(att_table)),
  "",
  "## Baseline ATT by cohort",
  capture.output(print(cohort_att_table)),
  "",
  "## Low-dose-controls ATT estimates",
  capture.output(print(att_table_ld)),
  "",
  "## Low-dose-controls ATT by cohort",
  capture.output(print(cohort_att_table_ld)),
  "",
  "## Spillover coefficients",
  capture.output(print(spillover_table)),
  "",
  "## Saved event-study and ATT plots",
  "- output/plots/sender_corridor_event_study_baseline_*.png",
  "- output/plots/sender_corridor_event_study_low_dose_controls_*.png",
  "- output/plots/sender_corridor_att_comparison_baseline.png",
  "- output/plots/sender_corridor_att_comparison_low_dose_controls.png"
)

writeLines(summary_lines, file.path(output_dir, "sender_corridor_calibrated_summary.md"))

message("Sender corridor-level rerun complete.")
