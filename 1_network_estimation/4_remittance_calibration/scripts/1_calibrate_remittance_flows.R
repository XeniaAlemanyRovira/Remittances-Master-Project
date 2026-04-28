# Calibrate quarterly US-state to Mexican-municipality remittance flows
# using the migration weighting matrices and cleaned Banxico margins.

rm(list = ls())

library(tidyverse)
library(readxl)

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
      "1_network_estimation",
      "4_remittance_calibration",
      "scripts",
      "1_calibrate_remittance_flows.R"
    ),
    winslash = "/",
    mustWork = FALSE
  )
}

script_path <- get_script_path()
repo_root <- normalizePath(file.path(dirname(script_path), "..", "..", ".."), winslash = "/", mustWork = TRUE)

path_in_repo <- function(...) {
  normalizePath(file.path(repo_root, ...), winslash = "/", mustWork = FALSE)
}

normalize_origin_state <- function(x) {
  dplyr::case_when(
    x == "Carolina Del Norte" ~ "North Carolina",
    x == "Carolina Del Sur" ~ "South Carolina",
    x == "Dakota Del Norte" ~ "North Dakota",
    x == "Dakota Del Sur" ~ "South Dakota",
    x == "Mississipi" ~ "Mississippi",
    x == "Misuri" ~ "Missouri",
    x == "Nueva Jersey" ~ "New Jersey",
    x == "Nueva York" ~ "New York",
    x == "Nuevo Hampshire" ~ "New Hampshire",
    x == "Nuevo Mexico" ~ "New Mexico",
    x == "Pensilvania" ~ "Pennsylvania",
    x %in% c("Washington, D.c.", "Washington, D.C.", "Washington Dc") ~ "District Of Columbia",
    TRUE ~ x
  )
}

ipfp_matrix <- function(seed_matrix, row_targets, col_targets, tol = 1e-8, max_iter = 5000) {
  fitted <- seed_matrix
  last_gap <- Inf

  for (iter in seq_len(max_iter)) {
    row_sums <- rowSums(fitted)
    row_factors <- ifelse(row_targets > 0, row_targets / pmax(row_sums, .Machine$double.eps), 0)
    fitted <- sweep(fitted, 1, row_factors, "*")

    col_sums <- colSums(fitted)
    col_factors <- ifelse(col_targets > 0, col_targets / pmax(col_sums, .Machine$double.eps), 0)
    fitted <- sweep(fitted, 2, col_factors, "*")

    row_gap <- max(abs(rowSums(fitted) - row_targets))
    col_gap <- max(abs(colSums(fitted) - col_targets))
    last_gap <- max(row_gap, col_gap)

    if (last_gap < tol) {
      return(list(
        fitted = fitted,
        iterations = iter,
        converged = TRUE,
        max_gap = last_gap
      ))
    }
  }

  list(
    fitted = fitted,
    iterations = max_iter,
    converged = FALSE,
    max_gap = last_gap
  )
}

write_or_append_csv <- function(df, path, append_mode) {
  if (!append_mode) {
    readr::write_csv(df, path)
  } else {
    readr::write_csv(df, path, append = TRUE)
  }
}

weights_dir <- path_in_repo("1_network_estimation", "2_migration_matrix_estimation", "migration_weighting_matrices_2")
banxico_dir <- path_in_repo("1_network_estimation", "3_banxico_cleaning", "output")
output_dir <- path_in_repo("1_network_estimation", "4_remittance_calibration", "output")

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

origin_path <- file.path(banxico_dir, "banxico_origin_state_remittances_2013q1_2024q4.csv")
municipality_path <- file.path(banxico_dir, "banxico_municipality_remittances_2013q1_2024q4.csv")
avg_weights_path <- file.path(weights_dir, "AVG_WEIGHTING_MATRIX.xlsx")
master_output_path <- file.path(output_dir, "calibrated_remittance_flows_master_2013q1_2024q4.csv")
diagnostics_output_path <- file.path(output_dir, "calibration_diagnostics_2013q1_2024q4.csv")
origin_mapping_output_path <- file.path(output_dir, "origin_state_mapping_summary.csv")

if (file.exists(master_output_path)) {
  unlink(master_output_path)
}

avg_weights <- readxl::read_excel(avg_weights_path)
state_cols <- setdiff(names(avg_weights), c("mx_state", "mx_municipality"))
municipality_universe <- avg_weights %>%
  select(mx_state, mx_municipality) %>%
  arrange(mx_state, mx_municipality)

origin_raw <- readr::read_csv(origin_path, show_col_types = FALSE) %>%
  mutate(
    us_state_original = us_state,
    us_state = normalize_origin_state(us_state)
  )

origin_mapping_summary <- origin_raw %>%
  group_by(us_state_original, us_state) %>%
  summarise(total_remittances_musd = sum(remittances_musd, na.rm = TRUE), .groups = "drop") %>%
  mutate(
    mapping_status = case_when(
      us_state %in% state_cols ~ "kept",
      us_state_original == "No Identificado" ~ "dropped_no_identificado",
      us_state_original == "Puerto Rico" ~ "dropped_puerto_rico",
      TRUE ~ "dropped_unmapped"
    )
  ) %>%
  arrange(desc(total_remittances_musd))

readr::write_csv(origin_mapping_summary, origin_mapping_output_path)

origin_kept <- origin_raw %>%
  filter(us_state %in% state_cols) %>%
  group_by(period_date, year, quarter, year_quarter, us_state) %>%
  summarise(remittances_musd = sum(remittances_musd, na.rm = TRUE), .groups = "drop")

origin_dropped_summary <- origin_raw %>%
  filter(!(us_state %in% state_cols)) %>%
  group_by(period_date, year, quarter, year_quarter) %>%
  summarise(origin_total_dropped_musd = sum(remittances_musd, na.rm = TRUE), .groups = "drop")

municipality_targets <- readr::read_csv(municipality_path, show_col_types = FALSE) %>%
  group_by(period_date, year, quarter, year_quarter, mx_state, mx_municipality) %>%
  summarise(remittances_musd = sum(remittances_musd, na.rm = TRUE), .groups = "drop")

load_weight_matrix <- function(year_value) {
  year_path <- file.path(weights_dir, paste0("WEIGHTING_MATRIX_", year_value, ".xlsx"))
  year_weights <- readxl::read_excel(year_path)

  missing_cols <- setdiff(state_cols, names(year_weights))
  if (length(missing_cols) > 0) {
    for (state_name in missing_cols) {
      year_weights[[state_name]] <- NA_real_
    }
  }

  year_weights <- municipality_universe %>%
    left_join(year_weights, by = c("mx_state", "mx_municipality")) %>%
    arrange(mx_state, mx_municipality)

  fallback_states <- character(0)

  for (state_name in state_cols) {
    state_values <- year_weights[[state_name]]
    state_values[is.na(state_values)] <- 0

    if (sum(state_values, na.rm = TRUE) <= 0) {
      state_values <- avg_weights[[state_name]]
      fallback_states <- c(fallback_states, state_name)
    }

    year_weights[[state_name]] <- state_values
  }

  weight_matrix <- as.matrix(year_weights[, state_cols])
  col_sums <- colSums(weight_matrix, na.rm = TRUE)
  conditional_weights <- sweep(weight_matrix, 2, col_sums, "/")
  conditional_weights[is.na(conditional_weights)] <- 0

  # Small numerical regularization prevents structural-zero infeasibility in IPFP.
  conditional_weights <- pmax(conditional_weights, 1e-12)
  conditional_weights <- sweep(conditional_weights, 2, colSums(conditional_weights), "/")

  list(
    municipality_universe = year_weights %>% select(mx_state, mx_municipality),
    conditional_weights = conditional_weights,
    fallback_states = fallback_states
  )
}

weight_cache <- purrr::map(set_names(2013:2024), load_weight_matrix)
master_started <- FALSE
diagnostics <- vector("list", length = 0)

for (year_value in 2013:2024) {
  message("Calibrating year ", year_value)

  year_weights <- weight_cache[[as.character(year_value)]]
  yearly_quarters <- sort(unique(origin_kept$quarter[origin_kept$year == year_value]))
  yearly_outputs <- vector("list", length = length(yearly_quarters))

  for (idx in seq_along(yearly_quarters)) {
    quarter_value <- yearly_quarters[[idx]]

    origin_quarter <- origin_kept %>%
      filter(year == year_value, quarter == quarter_value) %>%
      right_join(tibble(us_state = state_cols), by = "us_state") %>%
      mutate(
        period_date = first(na.omit(period_date)),
        year = year_value,
        quarter = quarter_value,
        year_quarter = paste0(year_value, "Q", quarter_value),
        remittances_musd = replace_na(remittances_musd, 0)
      ) %>%
      arrange(match(us_state, state_cols))

    municipality_quarter <- municipality_targets %>%
      filter(year == year_value, quarter == quarter_value) %>%
      right_join(year_weights$municipality_universe, by = c("mx_state", "mx_municipality")) %>%
      mutate(
        period_date = first(na.omit(origin_quarter$period_date)),
        year = year_value,
        quarter = quarter_value,
        year_quarter = paste0(year_value, "Q", quarter_value),
        remittances_musd = replace_na(remittances_musd, 0)
      ) %>%
      arrange(mx_state, mx_municipality)

    origin_total_kept <- sum(origin_quarter$remittances_musd, na.rm = TRUE)
    municipality_total_raw <- sum(municipality_quarter$remittances_musd, na.rm = TRUE)
    municipality_scale_factor <- ifelse(municipality_total_raw > 0, origin_total_kept / municipality_total_raw, 0)
    municipality_targets_scaled <- municipality_quarter$remittances_musd * municipality_scale_factor

    seed_matrix <- sweep(
      year_weights$conditional_weights,
      2,
      origin_quarter$remittances_musd,
      "*"
    )

    fit <- ipfp_matrix(
      seed_matrix = seed_matrix,
      row_targets = municipality_targets_scaled,
      col_targets = origin_quarter$remittances_musd
    )

    fitted_matrix <- fit$fitted

    quarter_output <- tibble(
      period_date = origin_quarter$period_date[[1]],
      year = year_value,
      quarter = quarter_value,
      year_quarter = paste0(year_value, "Q", quarter_value),
      us_state = rep(state_cols, each = nrow(year_weights$municipality_universe)),
      mx_state = rep(year_weights$municipality_universe$mx_state, times = length(state_cols)),
      mx_municipality = rep(year_weights$municipality_universe$mx_municipality, times = length(state_cols)),
      remittances_musd = as.vector(fitted_matrix)
    )

    yearly_outputs[[idx]] <- quarter_output

    diagnostics[[length(diagnostics) + 1]] <- tibble(
      period_date = origin_quarter$period_date[[1]],
      year = year_value,
      quarter = quarter_value,
      year_quarter = paste0(year_value, "Q", quarter_value),
      origin_total_kept_musd = origin_total_kept,
      origin_total_dropped_musd = origin_dropped_summary %>%
        filter(year == year_value, quarter == quarter_value) %>%
        summarise(total = sum(origin_total_dropped_musd, na.rm = TRUE)) %>%
        pull(total),
      municipality_total_raw_musd = municipality_total_raw,
      municipality_scale_factor = municipality_scale_factor,
      municipality_total_scaled_musd = sum(municipality_targets_scaled, na.rm = TRUE),
      ipfp_iterations = fit$iterations,
      ipfp_converged = fit$converged,
      ipfp_max_gap = fit$max_gap,
      fallback_states_used = paste(year_weights$fallback_states, collapse = " | ")
    )

    message(
      "  ",
      year_value,
      "Q",
      quarter_value,
      ": origin total = ",
      round(origin_total_kept, 3),
      ", municipality scale factor = ",
      round(municipality_scale_factor, 6),
      ", iterations = ",
      fit$iterations
    )
  }

  yearly_panel <- bind_rows(yearly_outputs)
  yearly_output_path <- file.path(output_dir, paste0("calibrated_remittance_flows_", year_value, ".csv"))
  readr::write_csv(yearly_panel, yearly_output_path)

  write_or_append_csv(yearly_panel, master_output_path, append_mode = master_started)
  master_started <- TRUE
}

diagnostics_df <- bind_rows(diagnostics)
readr::write_csv(diagnostics_df, diagnostics_output_path)

message("Master panel written to: ", master_output_path)
message("Diagnostics written to: ", diagnostics_output_path)
