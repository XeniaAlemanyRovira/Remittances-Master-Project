# This script reads the single state xlsx files and spits out cross-sectional
# MX municipality to US State migration matrices for each year

rm(list = ls())

library(tidyverse)
library(readxl)
library(writexl)

# Config
base_dir <- "2_SQL_database/Data_clean_updated/MCAS/Estados_US"
output_dir  <- "1_network_estimation/2_migration_matrix_estimation/yearly_migration_matrices_2"
years    <- 2010:2024

# Helper: extracts the US State name from the file path
parse_us_state <- function(path) {
  fname  <- tools::file_path_sans_ext(basename(path))
  pieces <- str_replace(fname, "_\\d{4}$", "")
  str_to_title(str_replace_all(pieces, "_", " "))
}

# Helper: takes away accents from names
normalize_text <- function(x) {
  x %>%
    str_to_title() %>%
    stringi::stri_trans_general("Latin-ASCII")
}

# Helper: applies name corrections using the lookup table
fix_names <- function(df) {
  df %>%
    left_join(name_corrections, by = c("mx_state" = "mx_state", "mx_municipality" = "wrong")) %>%
    mutate(mx_municipality = coalesce(correct, mx_municipality)) %>%
    select(-correct)
}

# Helper: reads each file based on the path and year
read_state_file <- function(path, yr) {
  read_excel(path) %>%
    select(mx_state, mx_municipality, n_matriculas) %>% # leaves out pct variables
    mutate(
      mx_state        = normalize_text(mx_state),
      mx_municipality = normalize_text(mx_municipality)
    ) %>%
    filter(
      mx_municipality != "Total", # drops subtotal rows for each MX municipality
      mx_state        != "Total", # drops total rows for each US State
      !str_detect(mx_municipality, regex("no se registro", ignore_case = TRUE))
    ) %>%
    fix_names() %>% # applies typo and duplicate corrections
    mutate(
      year         = yr,
      us_state     = parse_us_state(path),
      n_matriculas = as.integer(n_matriculas)
    )
}

# Migration panel
# This creates a panel with all of the US states and all of the years
panel <- map_dfr(years, function(yr) {
  yr_dir <- file.path(base_dir, paste0("Edos_USA_", yr))
  files  <- list.files(yr_dir, pattern = "\\.xlsx$",
                       full.names = TRUE, ignore.case = TRUE)
  if (length(files) == 0) {
    warning("No files found for year ", yr)
    return(NULL)
  }
  message("Reading year ", yr, " (", length(files), " files)...") 
  map_dfr(files, read_state_file, yr = yr)
}) %>%
  select(year, us_state, mx_state, mx_municipality, n_matriculas) %>%
  arrange(year, us_state, mx_state, mx_municipality)

# Migration matrix for each year
# Slices the panel to get a matrix for all the US States each year

# Full universe of municipalities across all years
all_municipalities <- panel %>%
  distinct(mx_state, mx_municipality)

matrices <- map(years, function(yr) {
  panel %>%
    filter(year == yr) %>%
    select(mx_state, mx_municipality, us_state, n_matriculas) %>%
    pivot_wider(
      names_from  = us_state,
      values_from = n_matriculas,
      values_fill = 0,
      values_fn   = sum
    ) %>%
    # Enforce full municipality universe, filling absent ones with 0
    right_join(all_municipalities, by = c("mx_state", "mx_municipality")) %>%
    mutate(across(where(is.numeric), ~ replace_na(., 0))) %>%
    arrange(mx_state, mx_municipality) %>%
    # Set to NA the xlsx files we are missing
    { if (yr == 2013) mutate(., Florida     = NA_real_) else . } %>%
    { if (yr == 2020) mutate(., Alaska      = NA_real_) else . } %>%
    { if (yr == 2024) mutate(., Connecticut = NA_real_) else . } %>%
    select(mx_state, mx_municipality, sort(tidyselect::peek_vars())) %>%
    # Add total column (row sums per municipality)
    mutate(Total = rowSums(across(where(is.numeric)), na.rm = TRUE)) %>%
    # Add total row (column sums per US state)
    bind_rows(
      summarise(.,
                mx_state        = "Total",
                mx_municipality = "Total",
                across(where(is.numeric), sum)
      )
    )
}) %>%
  set_names(paste0("MIGRATION_MATRIX_", years))

# Directory for the exported datasets
if (dir.exists(output_dir)) {
  unlink(output_dir, recursive = TRUE)
}
dir.create(output_dir)

# Export each matrix as a xlsx file
walk2(matrices, names(matrices), function(df, name) {
  write_xlsx(df, path = file.path(output_dir, paste0(name, ".xlsx")))
})