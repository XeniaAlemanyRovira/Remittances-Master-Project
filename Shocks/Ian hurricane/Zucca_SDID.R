#!/usr/bin/env Rscript

# Zucca synthetic DID setup ----------------------------------------------------
# This script loads the municipality-to-US-state migration weighting matrix used
# to map Mexican municipalities into US-state exposure shares.
#
# Source:
# 1_network_estimation/2_migration_matrix_estimation/Scripts/2_weighting_matrices.R
#
# In that script:
# - weighting_matrices_rows normalizes each Mexican municipality row to sum to 1.
# - avg_row_matrix averages those row-normalized yearly matrices, excluding 2020
#   and 2021.
# - FINAL_AVG_ROW_MATRIX.xlsx is the exported row-normalized average matrix.

suppressPackageStartupMessages({
  library(data.table)
  library(readxl)
})

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

migration_weighting_dir <- path_in_repo(
  "1_network_estimation",
  "2_migration_matrix_estimation",
  "migration_weighting_matrices_2"
)

row_weight_matrix_path <- file.path(
  migration_weighting_dir,
  "FINAL_AVG_ROW_MATRIX.xlsx"
)

if (!file.exists(row_weight_matrix_path)) {
  stop("Could not find row-normalized migration weighting matrix: ", row_weight_matrix_path)
}

# Load row-normalized municipality weights ------------------------------------

migration_weights_exported <- as.data.table(read_excel(row_weight_matrix_path))

id_cols <- c("mx_state", "mx_municipality")
missing_id_cols <- setdiff(id_cols, names(migration_weights_exported))
if (length(missing_id_cols) > 0L) {
  stop(
    "The migration weighting matrix is missing required columns: ",
    paste(missing_id_cols, collapse = ", ")
  )
}

us_state_cols <- setdiff(names(migration_weights_exported), id_cols)
migration_weights_exported[
  ,
  (us_state_cols) := lapply(.SD, as.numeric),
  .SDcols = us_state_cols
]

migration_weights_exported[
  ,
  exported_row_weight_sum := rowSums(.SD, na.rm = TRUE),
  .SDcols = us_state_cols
]

row_sum_tolerance <- 1e-8
exported_row_weight_checks <- migration_weights_exported[
  ,
  .(
    municipalities = .N,
    min_row_sum = min(exported_row_weight_sum, na.rm = TRUE),
    mean_row_sum = mean(exported_row_weight_sum, na.rm = TRUE),
    max_row_sum = max(exported_row_weight_sum, na.rm = TRUE),
    max_abs_deviation_from_one = max(abs(exported_row_weight_sum - 1), na.rm = TRUE),
    rows_equal_to_one = sum(abs(exported_row_weight_sum - 1) <= row_sum_tolerance, na.rm = TRUE),
    rows_not_equal_to_one = sum(abs(exported_row_weight_sum - 1) > row_sum_tolerance, na.rm = TRUE)
  )
]

if (any(migration_weights_exported$exported_row_weight_sum <= 0, na.rm = TRUE)) {
  stop("Some municipality rows have zero total exported weight and cannot be normalized.")
}

migration_weights_wide <- copy(migration_weights_exported)
migration_weights_wide[
  ,
  (us_state_cols) := lapply(.SD, function(x) x / exported_row_weight_sum),
  .SDcols = us_state_cols
]
migration_weights_wide[, row_weight_sum := rowSums(.SD, na.rm = TRUE), .SDcols = us_state_cols]

row_weight_checks <- migration_weights_wide[
  ,
  .(
    municipalities = .N,
    min_row_sum = min(row_weight_sum, na.rm = TRUE),
    mean_row_sum = mean(row_weight_sum, na.rm = TRUE),
    max_row_sum = max(row_weight_sum, na.rm = TRUE),
    max_abs_deviation_from_one = max(abs(row_weight_sum - 1), na.rm = TRUE),
    rows_equal_to_one = sum(abs(row_weight_sum - 1) <= row_sum_tolerance, na.rm = TRUE),
    rows_not_equal_to_one = sum(abs(row_weight_sum - 1) > row_sum_tolerance, na.rm = TRUE)
  )
]

if (row_weight_checks$rows_not_equal_to_one > 0L) {
  stop(
    "Internal row normalization failed. Maximum absolute deviation: ",
    row_weight_checks$max_abs_deviation_from_one
  )
}

migration_weights_long <- melt(
  migration_weights_wide,
  id.vars = c("mx_state", "mx_municipality"),
  measure.vars = us_state_cols,
  variable.name = "us_state",
  value.name = "migration_weight"
)

migration_weights_long <- migration_weights_long[migration_weight > 0]
setorder(migration_weights_long, mx_state, mx_municipality, -migration_weight)

message("Loaded row-normalized migration weighting matrix: ", row_weight_matrix_path)
message("Municipalities: ", nrow(migration_weights_wide))
message("US-state columns: ", length(us_state_cols))
message(
  "Exported matrix row-sum check before final normalization: max absolute deviation from 1 = ",
  signif(exported_row_weight_checks$max_abs_deviation_from_one, 4)
)
message(
  "Loaded Zucca matrix row-sum check after final normalization: max absolute deviation from 1 = ",
  signif(row_weight_checks$max_abs_deviation_from_one, 4)
)

# Objects available for the SDID below:
# - migration_weights_exported: raw FINAL_AVG_ROW_MATRIX.xlsx as exported.
# - migration_weights_wide: one row per Mexican municipality, US-state columns sum to 1.
# - migration_weights_long: municipality-US-state weights in long format.
# - exported_row_weight_checks: validation of the exported file before final normalization.
# - row_weight_checks: compact validation table.
