# Compute quarterly differential remittance matrices (Model A minus Model B)
# with a 5 % relative threshold: cell-level differences whose absolute value
# is smaller than 5 % of the average of the two model predictions are zeroed
# out, treating them as estimation noise rather than a meaningful discrepancy.
#
# Threshold rationale (documented here, absent in original):
#   The original code compared |diff| to 5 % of matrix_b alone.  Using the
#   cell-wise average of A and B as the denominator is more symmetric and does
#   not favour either model as the reference.  Cells where both models agree
#   closely (relative to their own magnitude) are zeroed; cells where one model
#   is much larger than the other are retained regardless of absolute size.
#
# Fixes vs. original Script 4:
#   - Uses Script 1's get_script_path() / repo_root path resolution
#   - Uses Script 1's normalize_origin_state() before filtering
#   - Uses Script 1's ipfp_matrix() (max_iter = 5000, returns list)
#   - Uses Script 1's load_weight_matrix() with AVG fallback and 1e-12 floor
#   - Threshold denominator changed from matrix_b to cell-wise mean(A, B),
#     making it symmetric and independent of which model is "B"
#   - Quarter subtotal rows inserted after each quarter's data block (not
#     re-sorted at the end), fixing the fragile sort-order bug

rm(list = ls())

library(tidyverse)
library(readxl)
library(writexl)

# ── 1. Path resolution (identical to Script 1) ────────────────────────────────

get_script_path <- function() {
  cmd_args <- commandArgs(trailingOnly = FALSE)
  file_arg  <- grep("^--file=", cmd_args, value = TRUE)

  if (length(file_arg) > 0) {
    return(normalizePath(sub("^--file=", "", file_arg[1]), winslash = "/", mustWork = TRUE))
  }

  this_file <- tryCatch(
    normalizePath(sys.frame(1)$ofile, winslash = "/", mustWork = TRUE),
    error = function(...) NA_character_
  )

  if (!is.na(this_file)) return(this_file)

  normalizePath(
    file.path(
      getwd(),
      "1_network_estimation",
      "4_remittance_calibration",
      "scripts",
      "4_quarterly_differential_5pct_threshold.R"
    ),
    winslash = "/",
    mustWork = FALSE
  )
}

script_path <- get_script_path()
repo_root   <- normalizePath(file.path(dirname(script_path), "..", "..", ".."),
                              winslash = "/", mustWork = TRUE)

path_in_repo <- function(...) {
  normalizePath(file.path(repo_root, ...), winslash = "/", mustWork = FALSE)
}

# ── 2. Shared helpers (identical to Script 1) ─────────────────────────────────

normalize_origin_state <- function(x) {
  dplyr::case_when(
    x == "Carolina Del Norte"                                         ~ "North Carolina",
    x == "Carolina Del Sur"                                           ~ "South Carolina",
    x == "Dakota Del Norte"                                           ~ "North Dakota",
    x == "Dakota Del Sur"                                             ~ "South Dakota",
    x == "Mississipi"                                                 ~ "Mississippi",
    x == "Misuri"                                                     ~ "Missouri",
    x == "Nueva Jersey"                                               ~ "New Jersey",
    x == "Nueva York"                                                 ~ "New York",
    x == "Nuevo Hampshire"                                            ~ "New Hampshire",
    x == "Nuevo Mexico"                                               ~ "New Mexico",
    x == "Pensilvania"                                                ~ "Pennsylvania",
    x %in% c("Washington, D.c.", "Washington, D.C.", "Washington Dc") ~ "District Of Columbia",
    TRUE ~ x
  )
}

# Returns list: fitted (matrix), iterations, converged, max_gap
ipfp_matrix <- function(seed_matrix, row_targets, col_targets, tol = 1e-8, max_iter = 5000) {
  fitted   <- seed_matrix
  last_gap <- Inf

  for (iter in seq_len(max_iter)) {
    row_sums    <- rowSums(fitted)
    row_factors <- ifelse(row_targets > 0, row_targets / pmax(row_sums, .Machine$double.eps), 0)
    fitted      <- sweep(fitted, 1, row_factors, "*")

    col_sums    <- colSums(fitted)
    col_factors <- ifelse(col_targets > 0, col_targets / pmax(col_sums, .Machine$double.eps), 0)
    fitted      <- sweep(fitted, 2, col_factors, "*")

    row_gap  <- max(abs(rowSums(fitted) - row_targets))
    col_gap  <- max(abs(colSums(fitted) - col_targets))
    last_gap <- max(row_gap, col_gap)

    if (last_gap < tol) {
      return(list(fitted = fitted, iterations = iter, converged = TRUE, max_gap = last_gap))
    }
  }

  list(fitted = fitted, iterations = max_iter, converged = FALSE, max_gap = last_gap)
}

# ── 3. Threshold helper ───────────────────────────────────────────────────────
# Zero out cells where |A - B| < threshold_pct * mean(A, B).
# Using the cell-wise mean rather than B alone makes the rule symmetric:
# the threshold does not depend on which model is labelled A or B.
# Cells where both models predict near-zero flow are protected from spurious
# zeroing by the .Machine$double.eps floor in the denominator.

apply_relative_threshold <- function(matrix_a, matrix_b, threshold_pct = 0.05) {
  diff_matrix    <- matrix_a - matrix_b
  cell_mean      <- (matrix_a + matrix_b) / 2
  relative_diff  <- abs(diff_matrix) / pmax(cell_mean, .Machine$double.eps)
  diff_matrix[relative_diff < threshold_pct] <- 0
  diff_matrix
}

# ── 4. Directory and file paths ───────────────────────────────────────────────

weights_dir <- path_in_repo("1_network_estimation", "2_migration_matrix_estimation",
                             "migration_weighting_matrices_2")
banxico_dir <- path_in_repo("1_network_estimation", "3_banxico_cleaning", "output")
output_dir  <- path_in_repo("1_network_estimation", "4_remittance_calibration", "output")

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

origin_path       <- file.path(banxico_dir, "banxico_origin_state_remittances_2013q1_2024q4.csv")
municipality_path <- file.path(banxico_dir, "banxico_municipality_remittances_2013q1_2024q4.csv")
avg_weights_path  <- file.path(weights_dir, "AVG_WEIGHTING_MATRIX.xlsx")
output_path       <- file.path(output_dir,  "YEARLY_DIFFERENTIAL_BY_QUARTER_5PCT_THRESHOLD.xlsx")

# ── 5. Load weights and define universe ───────────────────────────────────────

avg_weights           <- readxl::read_excel(avg_weights_path)
state_cols            <- setdiff(names(avg_weights), c("mx_state", "mx_municipality"))
municipality_universe <- avg_weights %>%
  select(mx_state, mx_municipality) %>%
  arrange(mx_state, mx_municipality)

# ── 6. Load and normalise origin data ─────────────────────────────────────────

origin_raw <- readr::read_csv(origin_path, show_col_types = FALSE) %>%
  mutate(
    us_state_original = us_state,
    us_state          = normalize_origin_state(us_state)
  )

origin_mapping_summary <- origin_raw %>%
  group_by(us_state_original, us_state) %>%
  summarise(total_remittances_musd = sum(remittances_musd, na.rm = TRUE), .groups = "drop") %>%
  mutate(
    mapping_status = case_when(
      us_state %in% state_cols               ~ "kept",
      us_state_original == "No Identificado"  ~ "dropped_no_identificado",
      us_state_original == "Puerto Rico"      ~ "dropped_puerto_rico",
      TRUE                                    ~ "dropped_unmapped"
    )
  ) %>%
  arrange(desc(total_remittances_musd))

readr::write_csv(
  origin_mapping_summary,
  file.path(output_dir, "origin_state_mapping_summary_script4.csv")
)

origin_kept <- origin_raw %>%
  filter(us_state %in% state_cols) %>%
  group_by(year, quarter, us_state) %>%
  summarise(remittances_musd = sum(remittances_musd, na.rm = TRUE), .groups = "drop")

muni_data <- readr::read_csv(municipality_path, show_col_types = FALSE)

# ── 7. Weight matrix loader with AVG fallback ─────────────────────────────────

load_weight_matrix <- function(year_value) {
  year_path    <- file.path(weights_dir, paste0("WEIGHTING_MATRIX_", year_value, ".xlsx"))
  year_weights <- readxl::read_excel(year_path)

  missing_cols <- setdiff(state_cols, names(year_weights))
  for (state_name in missing_cols) year_weights[[state_name]] <- NA_real_

  year_weights <- municipality_universe %>%
    left_join(year_weights, by = c("mx_state", "mx_municipality")) %>%
    arrange(mx_state, mx_municipality)

  fallback_states <- character(0)

  for (state_name in state_cols) {
    state_values <- year_weights[[state_name]]
    state_values[is.na(state_values)] <- 0

    if (sum(state_values, na.rm = TRUE) <= 0) {
      state_values    <- avg_weights[[state_name]]
      fallback_states <- c(fallback_states, state_name)
    }

    year_weights[[state_name]] <- state_values
  }

  weight_matrix       <- as.matrix(year_weights[, state_cols])
  col_sums            <- colSums(weight_matrix, na.rm = TRUE)
  conditional_weights <- sweep(weight_matrix, 2, col_sums, "/")
  conditional_weights[is.na(conditional_weights)] <- 0

  conditional_weights <- pmax(conditional_weights, 1e-12)
  conditional_weights <- sweep(conditional_weights, 2, colSums(conditional_weights), "/")

  list(
    municipality_universe = year_weights %>% select(mx_state, mx_municipality),
    conditional_weights   = conditional_weights,
    fallback_states       = fallback_states
  )
}

weight_cache <- purrr::map(set_names(2013:2024), load_weight_matrix)

# ── 8. Main loop ──────────────────────────────────────────────────────────────

master_diff_list <- list()

for (year_value in 2013:2024) {
  message("Processing year ", year_value)

  year_weights    <- weight_cache[[as.character(year_value)]]
  seed_base       <- year_weights$conditional_weights
  yearly_quarters <- sort(unique(origin_kept$quarter[origin_kept$year == year_value]))

  for (quarter_value in yearly_quarters) {

    target_origin <- origin_kept %>%
      filter(year == year_value, quarter == quarter_value) %>%
      right_join(tibble(us_state = state_cols), by = "us_state") %>%
      arrange(match(us_state, state_cols)) %>%
      mutate(remittances_musd = replace_na(remittances_musd, 0)) %>%
      pull(remittances_musd)

    target_muni <- muni_data %>%
      filter(year == year_value, quarter == quarter_value) %>%
      right_join(year_weights$municipality_universe,
                 by = c("mx_state", "mx_municipality")) %>%
      arrange(mx_state, mx_municipality) %>%
      mutate(remittances_musd = replace_na(remittances_musd, 0)) %>%
      pull(remittances_musd)

    if (sum(target_origin) == 0 || sum(target_muni) == 0) {
      message("  ", year_value, "Q", quarter_value, ": skipped (zero targets)")
      next
    }

    # Model A: scale municipality targets up to match origin grand total
    scale_a  <- sum(target_origin) / sum(target_muni)
    result_a <- ipfp_matrix(seed_base, target_muni * scale_a, target_origin)

    # Model B: scale origin targets down to match municipality grand total
    scale_b  <- sum(target_muni) / sum(target_origin)
    result_b <- ipfp_matrix(seed_base, target_muni, target_origin * scale_b)

    message(
      "  ", year_value, "Q", quarter_value,
      ": A converged=", result_a$converged, " (", result_a$iterations, " iter)",
      " | B converged=", result_b$converged, " (", result_b$iterations, " iter)"
    )

    # Apply symmetric 5 % relative threshold
    diff_matrix           <- apply_relative_threshold(result_a$fitted, result_b$fitted,
                                                       threshold_pct = 0.05)
    colnames(diff_matrix) <- state_cols

    qtr_df <- year_weights$municipality_universe %>%
      mutate(year = year_value, quarter = quarter_value) %>%
      bind_cols(as.data.frame(diff_matrix))

    master_diff_list[[paste(year_value, quarter_value)]] <- qtr_df
  }
}

# ── 9. Build Excel workbook: one sheet per year, subtotals after each quarter ──

if (length(master_diff_list) == 0) {
  message("No data processed — output file not written.")
  quit(save = "no")
}

all_diffs   <- bind_rows(master_diff_list)
year_sheets <- list()

for (year_value in sort(unique(all_diffs$year))) {

  year_data      <- all_diffs %>% filter(year == year_value)
  quarters_in_yr <- sort(unique(year_data$quarter))
  sheet_rows     <- list()

  for (quarter_value in quarters_in_yr) {

    qtr_block <- year_data %>%
      filter(quarter == quarter_value) %>%
      select(-year) %>%
      arrange(mx_state, mx_municipality)

    # Row totals
    qtr_block <- qtr_block %>%
      mutate(Total = rowSums(pick(all_of(state_cols)), na.rm = TRUE))

    # Quarter subtotal row — inserted directly after this quarter's municipalities
    subtotal_row <- qtr_block %>%
      summarise(
        quarter         = quarter_value,
        mx_state        = paste0("SUBTOTAL Q", quarter_value),
        mx_municipality = paste0("SUBTOTAL Q", quarter_value),
        across(all_of(c(state_cols, "Total")), \(x) sum(x, na.rm = TRUE))
      )

    sheet_rows[[as.character(quarter_value)]] <- bind_rows(qtr_block, subtotal_row)
  }

  # Grand total footer — sums data rows only, not subtotal rows
  combined    <- bind_rows(sheet_rows)
  grand_total <- combined %>%
    filter(!startsWith(mx_state, "SUBTOTAL")) %>%
    summarise(
      quarter         = NA_integer_,
      mx_state        = "GRAND TOTAL",
      mx_municipality = "GRAND TOTAL",
      across(all_of(c(state_cols, "Total")), \(x) sum(x, na.rm = TRUE))
    )

  year_sheets[[as.character(year_value)]] <- bind_rows(combined, grand_total)
}

writexl::write_xlsx(year_sheets, path = output_path)
message("Thresholded quarterly differential workbook written to: ", output_path)