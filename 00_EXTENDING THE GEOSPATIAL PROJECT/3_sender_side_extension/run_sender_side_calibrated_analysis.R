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
      "run_sender_side_calibrated_analysis.R"
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

normalize_text <- function(x) {
  x <- iconv(x, to = "ASCII//TRANSLIT")
  x <- tolower(x)
  x <- gsub("[^a-z0-9]+", " ", x)
  x <- gsub(" +", " ", x)
  trimws(x)
}

normalize_state <- function(x) {
  x <- normalize_text(x)
  x <- sub("^coahuila de zaragoza$", "coahuila", x)
  x <- sub("^michoacan de ocampo$", "michoacan", x)
  x <- sub("^estado de mexico$", "mexico", x)
  x <- sub("^veracruz de ignacio de la llave$", "veracruz", x)
  x <- sub("^queretaro de arteaga$", "queretaro", x)
  x <- sub("^distrito federal$", "ciudad de mexico", x)
  x
}

normalize_municipality <- function(state_key, muni_key) {
  out <- muni_key

  out[state_key == "chihuahua" & muni_key == "batopilas de manuel gomez morin"] <- "batopilas"
  out[state_key == "guanajuato" & muni_key == "san jose de iturbide"] <- "san jose iturbide"
  out[state_key == "morelos" & muni_key == "jonacatepec de leandro valle"] <- "jonacatepec"
  out[state_key == "oaxaca" & muni_key == "constancia del rosario"] <- "rosario"
  out[state_key == "oaxaca" & muni_key == "heroica villa tezoatlan de segura y luna cuna de la independencia de oaxaca"] <- "heroica villa tezoatlan de segura y luna"
  out[state_key == "oaxaca" & muni_key == "magdalena apazco"] <- "magdalena apasco"
  out[state_key == "oaxaca" & muni_key == "san juan mixtepec distr 08"] <- "san juan mixtepec dto 08"
  out[state_key == "oaxaca" & muni_key == "san juan mixtepec distr 26"] <- "san juan mixtepec dto 26"
  out[state_key == "oaxaca" & muni_key == "san pedro mixtepec distr 22"] <- "san pedro mixtepec dto 22"
  out[state_key == "oaxaca" & muni_key == "san pedro mixtepec distr 26"] <- "san pedro mixtepec dto 26"
  out[state_key == "oaxaca" & muni_key == "villa de tututepec"] <- "villa de tututepec de melchor ocampo"
  out[state_key == "quintana roo" & muni_key == "playa del carmen"] <- "solidaridad"
  out[state_key == "veracruz" & muni_key == "medellin de bravo"] <- "medellin"

  out
}

haversine_km <- function(lon1, lat1, lon2, lat2) {
  to_rad <- pi / 180
  lon1 <- lon1 * to_rad
  lat1 <- lat1 * to_rad
  lon2 <- lon2 * to_rad
  lat2 <- lat2 * to_rad

  dlon <- lon2 - lon1
  dlat <- lat2 - lat1

  a <- sin(dlat / 2)^2 + cos(lat1) * cos(lat2) * sin(dlon / 2)^2
  c <- 2 * atan2(sqrt(a), sqrt(1 - a))
  6371 * c
}

row_normalize <- function(mat) {
  rs <- rowSums(mat)
  rs[rs == 0 | !is.finite(rs)] <- 1
  sweep(mat, 1, rs, "/")
}

build_geo_weights <- function(states) {
  centers <- tibble::tibble(
    us_state = c(datasets::state.name, "District Of Columbia"),
    lon = c(datasets::state.center$x, -77.0369),
    lat = c(datasets::state.center$y, 38.9072)
  ) %>%
    filter(us_state %in% states) %>%
    arrange(match(us_state, states))

  if (!identical(centers$us_state, states)) {
    stop("Missing geographic centers for some sender states.")
  }

  n <- nrow(centers)
  dist_mat <- matrix(0, nrow = n, ncol = n, dimnames = list(states, states))

  for (i in seq_len(n)) {
    for (j in seq_len(n)) {
      if (i != j) {
        dist_mat[i, j] <- haversine_km(
          centers$lon[i], centers$lat[i],
          centers$lon[j], centers$lat[j]
        )
      }
    }
  }

  weight_mat <- ifelse(dist_mat > 0, 1 / dist_mat, 0)
  row_normalize(weight_mat)
}

build_network_weights <- function(flows) {
  flow_matrix <- flows %>%
    group_by(us_state, recipient_id) %>%
    summarise(remittances_musd = sum(remittances_musd, na.rm = TRUE), .groups = "drop") %>%
    tidyr::pivot_wider(
      names_from = recipient_id,
      values_from = remittances_musd,
      values_fill = 0
    ) %>%
    arrange(us_state)

  states <- flow_matrix$us_state
  M <- as.matrix(flow_matrix[, -1, drop = FALSE])
  rownames(M) <- states
  M <- row_normalize(M)

  W <- M %*% t(M)
  diag(W) <- 0
  row_normalize(W)
}

build_cohort_table <- function(results_list, lookup_df, counts_df) {
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
      dplyr::select(
        partition,
        cohort_events,
        year,
        quarter,
        cohort_time_index,
        cohort_treated_states,
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
        title = paste("Sender Event Study - PGA Partition:", res$partition),
        subtitle = paste0(
          "CS (2021) | ",
          spec_label,
          " | ",
          res$n_treated,
          " treated, ",
          res$n_controls,
          " control sender states"
        ),
        x = "Quarters relative to first sender exposure",
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
      file.path(plot_dir, paste0("sender_event_study_", spec_slug, "_", file_stub, ".png"))
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
      x = "Sender Exposure Partition (%g)",
      y = "ATT (Inverse hyperbolic sine remittances)"
    ) +
    theme_bw() +
    theme(
      plot.title = element_text(hjust = 0.5),
      plot.subtitle = element_text(hjust = 0.5)
    )

  safe_save_plot(
    plot_obj,
    file.path(plot_dir, paste0("sender_att_comparison_", spec_slug, ".png")),
    width = 6.5,
    height = 4.5
  )
}

run_cs_partition <- function(data, pga_lo, pga_hi, partition_label,
                             pre_window = -10, post_window = 10) {
  treated_in_partition <- data %>%
    dplyr::filter(!is.na(event), dose > pga_lo, dose <= pga_hi) %>%
    dplyr::distinct(sender_id)

  if (nrow(treated_in_partition) == 0) {
    return(NULL)
  }

  sample_df <- data %>%
    dplyr::filter(sender_id %in% treated_in_partition$sender_id | G_cs == 0) %>%
    dplyr::mutate(G_cs = ifelse(sender_id %in% treated_in_partition$sender_id, G_cs, 0L))

  model_df <- sample_df %>%
    dplyr::select(asinhremit, time_index, state_num, G_cs, sender_id) %>%
    dplyr::mutate(
      time_index = as.numeric(time_index),
      state_num = as.numeric(state_num),
      G_cs = as.numeric(G_cs)
    ) %>%
    as.data.frame()

  n_treated <- dplyr::n_distinct(model_df$sender_id[model_df$G_cs > 0])
  n_controls <- dplyr::n_distinct(model_df$sender_id[model_df$G_cs == 0])
  n_cohorts <- dplyr::n_distinct(model_df$G_cs[model_df$G_cs > 0])

  if (n_treated < 2) {
    return(NULL)
  }

  cs_out <- tryCatch(
    att_gt(
      yname = "asinhremit",
      tname = "time_index",
      idname = "state_num",
      gname = "G_cs",
      data = model_df,
      control_group = "nevertreated",
      est_method = "dr",
      clustervars = "state_num",
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
    dplyr::distinct(sender_id)

  if (nrow(treated_in_partition) == 0) {
    return(NULL)
  }

  other_high_dose <- data %>%
    dplyr::filter(!is.na(event), dose > 10, !(sender_id %in% treated_in_partition$sender_id)) %>%
    dplyr::distinct(sender_id)

  sample_df <- data %>%
    dplyr::filter(sender_id %in% treated_in_partition$sender_id | G_cs == 0) %>%
    dplyr::filter(!(sender_id %in% other_high_dose$sender_id)) %>%
    dplyr::mutate(G_cs = ifelse(sender_id %in% treated_in_partition$sender_id, G_cs, 0L))

  model_df <- sample_df %>%
    dplyr::select(asinhremit, time_index, state_num, G_cs, sender_id) %>%
    dplyr::mutate(
      time_index = as.numeric(time_index),
      state_num = as.numeric(state_num),
      G_cs = as.numeric(G_cs)
    ) %>%
    as.data.frame()

  n_treated <- dplyr::n_distinct(model_df$sender_id[model_df$G_cs > 0])
  n_controls <- dplyr::n_distinct(model_df$sender_id[model_df$G_cs == 0])
  n_cohorts <- dplyr::n_distinct(model_df$G_cs[model_df$G_cs > 0])

  if (n_treated < 2) {
    return(NULL)
  }

  cs_out <- tryCatch(
    att_gt(
      yname = "asinhremit",
      tname = "time_index",
      idname = "state_num",
      gname = "G_cs",
      data = model_df,
      control_group = "nevertreated",
      est_method = "dr",
      clustervars = "state_num",
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
repo_root <- normalizePath(file.path(dirname(script_path), "..", ".."), winslash = "/", mustWork = TRUE)
extension_dir <- file.path(repo_root, "00_EXTENDING THE GEOSPATIAL PROJECT")
panel_dir <- file.path(extension_dir, "1_cleaned_panels")
analysis_dir <- file.path(extension_dir, "3_sender_side_extension")
output_dir <- file.path(analysis_dir, "output")
plot_dir <- file.path(output_dir, "plots")

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)

suppressPackageStartupMessages({
  library(tidyverse)
  library(fixest)
  library(did)
  library(broom)
})

message("Reading calibrated sender-side panel...")
panel_path <- file.path(panel_dir, "panel_sending_states_pga_calibrated.csv")
panel <- readr::read_csv(panel_path, show_col_types = FALSE)

message("Reading calibrated flow matrix for sender network overlap...")
flows_path <- file.path(
  repo_root,
  "1_network_estimation",
  "4_remittance_calibration",
  "output",
  "calibrated_remittance_flows_master_2013q1_2024q4.csv"
)
flows <- readr::read_csv(flows_path, show_col_types = FALSE) %>%
  mutate(
    state_key = normalize_state(mx_state),
    muni_key = normalize_municipality(state_key, normalize_text(mx_municipality)),
    recipient_id = paste(state_key, muni_key, sep = "_")
  )

message("Preparing sender-side panel...")
panel <- panel %>%
  mutate(
    dominant_event = na_if(dominant_event, ""),
    sender_id = us_state,
    time_index = (year - min(year)) * 4 + quarter,
    asinhremit = asinh_remittances
  )

partitions <- list(
  "4-10" = c(4, 10),
  "10-20" = c(10, 20),
  "20+" = c(20, Inf)
)

PGA_THRESHOLD <- 4

sender_meta <- panel %>%
  filter(mean_pga > PGA_THRESHOLD) %>%
  group_by(sender_id) %>%
  arrange(time_index, .by_group = TRUE) %>%
  summarise(
    event = dominant_event[match(min(time_index), time_index)],
    dose = max(mean_pga, na.rm = TRUE),
    G = min(time_index),
    .groups = "drop"
  )

panel <- panel %>%
  left_join(sender_meta, by = "sender_id")

panel_cs <- panel %>%
  mutate(
    G_cs = ifelse(is.na(event), 0, as.numeric(G)),
    state_num = as.numeric(factor(sender_id))
  )

time_lookup <- panel %>%
  distinct(time_index, year, quarter)

cohort_lookup <- sender_meta %>%
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
  sender_meta %>%
    filter(dose > bounds[1], dose <= bounds[2]) %>%
    count(G, name = "cohort_treated_states") %>%
    rename(cohort_time_index = G) %>%
    mutate(partition = part_name)
})

treated_partition_summary <- sender_meta %>%
  mutate(
    partition = case_when(
      dose > 4 & dose <= 10 ~ "4-10",
      dose > 10 & dose <= 20 ~ "10-20",
      dose > 20 ~ "20+",
      TRUE ~ "control"
    )
  ) %>%
  arrange(desc(dose), sender_id)

safe_write_csv(
  treated_partition_summary,
  file.path(output_dir, "sender_treated_states_overview.csv")
)

message("Running baseline Callaway-Sant'Anna sender partitions...")
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

safe_write_csv(att_table, file.path(output_dir, "sender_att_table_calibrated.csv"))

cohort_att_table <- build_cohort_table(cs_results, cohort_lookup, cohort_counts)
safe_write_csv(
  cohort_att_table,
  file.path(output_dir, "sender_att_by_cohort_calibrated.csv")
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
safe_write_csv(dynamic_baseline, file.path(output_dir, "sender_event_study_baseline_calibrated.csv"))
save_event_study_plots(cs_results, plot_dir, "baseline", "never-treated controls")
save_att_plot(
  att_table,
  plot_dir,
  "baseline",
  "Sender CS ATT by Exposure Partition",
  "Callaway & Sant'Anna (2021) | calibrated sender panel | never-treated controls"
)

message("Running sender low-dose-controls robustness specification...")
panel_cs_ld <- panel %>%
  mutate(
    G_cs = case_when(
      is.na(event) ~ 0,
      dose > 4 & dose <= 10 ~ 0,
      TRUE ~ as.numeric(G)
    ),
    state_num = as.numeric(factor(sender_id))
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

safe_write_csv(att_table_ld, file.path(output_dir, "sender_att_table_low_dose_controls_calibrated.csv"))

cohort_counts_ld <- purrr::map_dfr(names(partitions_ld), function(part_name) {
  bounds <- partitions_ld[[part_name]]
  sender_meta %>%
    filter(dose > bounds[1], dose <= bounds[2]) %>%
    count(G, name = "cohort_treated_states") %>%
    rename(cohort_time_index = G) %>%
    mutate(partition = part_name)
})

cohort_att_table_ld <- build_cohort_table(cs_results_ld, cohort_lookup, cohort_counts_ld)
safe_write_csv(
  cohort_att_table_ld,
  file.path(output_dir, "sender_att_by_cohort_low_dose_controls_calibrated.csv")
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
safe_write_csv(dynamic_ld, file.path(output_dir, "sender_event_study_low_dose_controls_calibrated.csv"))
save_event_study_plots(cs_results_ld, plot_dir, "low_dose_controls", "low-dose (4-10) controls")
save_att_plot(
  att_table_ld,
  plot_dir,
  "low_dose_controls",
  "Sender CS ATT by Exposure Partition (low-dose controls)",
  "Callaway & Sant'Anna (2021) | calibrated sender panel | low-dose controls"
)

message("Building sender-state spillover matrices...")
states <- panel %>%
  distinct(sender_id) %>%
  arrange(sender_id) %>%
  pull(sender_id)

W_geo <- build_geo_weights(states)
W_network <- build_network_weights(flows)
W_network <- W_network[states, states]

sender_dose_lookup <- panel %>%
  group_by(sender_id) %>%
  summarise(dose = max(mean_pga, na.rm = TRUE), .groups = "drop")

dose_vector_geo <- setNames(rep(0, nrow(W_geo)), rownames(W_geo))
matched_geo <- intersect(rownames(W_geo), sender_dose_lookup$sender_id)
dose_vector_geo[matched_geo] <- sender_dose_lookup$dose[match(matched_geo, sender_dose_lookup$sender_id)]

geo_pga_df <- tibble(
  sender_id = rownames(W_geo),
  geo_pga = as.vector(W_geo %*% dose_vector_geo)
)

dose_vector_net <- setNames(rep(0, nrow(W_network)), rownames(W_network))
matched_net <- intersect(rownames(W_network), sender_dose_lookup$sender_id)
dose_vector_net[matched_net] <- sender_dose_lookup$dose[match(matched_net, sender_dose_lookup$sender_id)]

network_pga_df <- tibble(
  sender_id = rownames(W_network),
  network_pga = as.vector(W_network %*% dose_vector_net)
)

first_treated_time <- min(sender_meta$G, na.rm = TRUE)

spillover_df_geo <- panel_cs %>%
  left_join(geo_pga_df, by = "sender_id") %>%
  filter(G_cs == 0, !is.na(geo_pga)) %>%
  mutate(
    post = as.integer(time_index >= first_treated_time),
    geo_pga_c = geo_pga - mean(geo_pga, na.rm = TRUE)
  )

spillover_mod_geo <- feols(
  asinhremit ~ post:geo_pga_c | state_num + time_index,
  data = spillover_df_geo,
  cluster = ~state_num
)

spillover_df_net <- panel_cs %>%
  left_join(network_pga_df, by = "sender_id") %>%
  filter(G_cs == 0, !is.na(network_pga)) %>%
  mutate(
    post = as.integer(time_index >= first_treated_time),
    network_pga_c = network_pga - mean(network_pga, na.rm = TRUE)
  )

spillover_mod_net <- feols(
  asinhremit ~ post:network_pga_c | state_num + time_index,
  data = spillover_df_net,
  cluster = ~state_num
)

spillover_df_joint <- panel_cs %>%
  left_join(network_pga_df, by = "sender_id") %>%
  left_join(geo_pga_df, by = "sender_id") %>%
  filter(G_cs == 0, !is.na(network_pga), !is.na(geo_pga)) %>%
  mutate(
    post = as.integer(time_index >= first_treated_time),
    network_pga_c = network_pga - mean(network_pga, na.rm = TRUE),
    geo_pga_c = geo_pga - mean(geo_pga, na.rm = TRUE)
  )

spillover_mod_joint <- feols(
  asinhremit ~ post:network_pga_c + post:geo_pga_c | state_num + time_index,
  data = spillover_df_joint,
  cluster = ~state_num
)

spillover_table <- bind_rows(
  tidy(spillover_mod_geo, conf.int = TRUE) %>% mutate(model = "geographic"),
  tidy(spillover_mod_net, conf.int = TRUE) %>% mutate(model = "network"),
  tidy(spillover_mod_joint, conf.int = TRUE) %>% mutate(model = "joint")
) %>%
  filter(term %in% c("post:geo_pga_c", "post:network_pga_c"))

safe_write_csv(spillover_table, file.path(output_dir, "sender_spillover_results_calibrated.csv"))

summary_lines <- c(
  "# Sender-side calibrated extension",
  "",
  paste0("Analysis date: ", Sys.Date()),
  paste0("Panel rows used: ", nrow(panel)),
  paste0("Sender states in panel: ", n_distinct(panel$sender_id)),
  paste0("Ever-treated sender states (mean PGA > 4): ", nrow(sender_meta)),
  paste0("Available sender treatment partitions: ", paste(sort(unique(treated_partition_summary$partition[treated_partition_summary$partition != 'control'])), collapse = ", ")),
  "",
  "## Main design note",
  "The sender-side extension keeps the receiver-side econometric pipeline, but the unit of observation is the US sending state by quarter and treatment is defined using remittance-weighted mean recipient PGA exposure.",
  "Because no sender states ever reach a remittance-weighted mean exposure above 20%g, the sender-side analysis only identifies the 4-10 and 10-20 bins.",
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
  "- output/plots/sender_event_study_baseline_*.png",
  "- output/plots/sender_event_study_low_dose_controls_*.png",
  "- output/plots/sender_att_comparison_baseline.png",
  "- output/plots/sender_att_comparison_low_dose_controls.png"
)

writeLines(summary_lines, file.path(output_dir, "sender_side_calibrated_summary.md"))

message("Sender-side calibrated extension complete.")
