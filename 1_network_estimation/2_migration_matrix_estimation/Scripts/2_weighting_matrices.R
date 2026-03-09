# This script reads the cross-sectional xlsx files, provides the weighting matrix
# for each year and the final weighting matrix

rm(list = ls())

library(tidyverse)
library(readxl)
library(writexl)

# Config
input_dir <- "1_network_estimation/2_migration_matrix_estimation/yearly_migration_matrices_2"
output_dir <- "1_network_estimation/2_migration_matrix_estimation/migration_weighting_matrices_2"
years    <- 2010:2024

# Function that loads each years migration matrix
matrices <- map(years, function(yr) {
  path <- file.path(input_dir, paste0("MIGRATION_MATRIX_", yr, ".xlsx"))
  if (!file.exists(path)) {
    warning("File not found for year ", yr)
    return(NULL)
  }
  read_excel(path) %>%
    select(-Total) %>% # drops Total column
    filter(mx_state != "Total") # drops Total row
}) %>%
  set_names(paste0("MIGRATION_MATRIX_", years))

# Compute the weighting matrix by computing the share of each US State - MX
# municipality combination over the total immigration flow

# 1 - Compute raw weighting matrices for all years
weighting_matrices_raw <- map(matrices, function(df) {
  total <- df %>%
    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE))) %>%
    sum()
  df %>%
    mutate(across(where(is.numeric), ~ . / total))
})

# 2 - Compute average weights per missing state (exluding also Covid years)
compute_avg_weight <- function(state_col, excl_years) {
  weighting_matrices_raw[
    paste0("MIGRATION_MATRIX_", setdiff(years, excl_years))
  ] %>%
    map(~ select(.x, mx_state, mx_municipality, all_of(state_col))) %>%
    reduce(full_join, by = c("mx_state", "mx_municipality")) %>%
    mutate(avg = rowMeans(across(where(is.numeric)), na.rm = TRUE)) %>%
    summarise(total_avg_weight = sum(avg, na.rm = TRUE)) %>%
    pull()
}

florida_total_avg_weight     <- compute_avg_weight("Florida",     c(2013, 2020, 2021))
alaska_total_avg_weight      <- compute_avg_weight("Alaska",      c(2020, 2021))
connecticut_total_avg_weight <- compute_avg_weight("Connecticut", c(2024, 2020, 2021))

message("Average Florida total weight:     ", round(florida_total_avg_weight,     4))
message("Average Alaska total weight:      ", round(alaska_total_avg_weight,      4))
message("Average Connecticut total weight: ", round(connecticut_total_avg_weight, 4))

# 3 - Compute the denominator with the mean imputation for missing states
get_observed_total <- function(yr) {
  matrices[[paste0("MIGRATION_MATRIX_", yr)]] %>%
    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE))) %>%
    sum()
}

total_2013_imputed <- get_observed_total(2013) / (1 - florida_total_avg_weight)
total_2020_imputed <- get_observed_total(2020) / (1 - alaska_total_avg_weight)
total_2024_imputed <- get_observed_total(2024) / (1 - connecticut_total_avg_weight)

message("Observed 2013 total: ", round(get_observed_total(2013)))
message("Imputed  2013 total: ", round(total_2013_imputed))
message("Observed 2020 total: ", round(get_observed_total(2020)))
message("Imputed  2020 total: ", round(total_2020_imputed))
message("Observed 2024 total: ", round(get_observed_total(2024)))
message("Imputed  2024 total: ", round(total_2024_imputed))

# 4 - Compute the final weighting matrices for each year
weighting_matrices <- imap(matrices, function(df, name) {
  
  total <- case_when(
    name == "MIGRATION_MATRIX_2013" ~ total_2013_imputed,
    name == "MIGRATION_MATRIX_2020" ~ total_2020_imputed,
    name == "MIGRATION_MATRIX_2024" ~ total_2024_imputed,
    TRUE ~ df %>%
      summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE))) %>%
      sum()
  )
  
  df %>%
    mutate(across(where(is.numeric), ~ . / total))
}) %>%
  set_names(paste0("WEIGHTING_MATRIX_", years))

# 5 - Sanity check only for Florida
# Non-Florida weight and Florida average weight should sum up to 1 in 2013
non_florida_sum_2013 <- weighting_matrices[["WEIGHTING_MATRIX_2013"]] %>%
  select(-Florida) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE))) %>%
  sum()
non_florida_sum_2013 + florida_total_avg_weight

# Sanity check of every weighting matrix
# Note that for 2013, 2020, 2024 it is normal for the weights to not be exactly
# 1 since we have NAs in Florida, Alaska, and Connecticut respectively (though
# the ones in Alaska are negligible)
weighting_matrices %>%
  imap_dfr(~ tibble(
    year  = str_extract(.y, "\\d+"),
    total = .x %>%
      filter(mx_municipality != "Total") %>%   # exclude total row if present
      summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE))) %>%
      sum()
  ))

# Average weighting matrix (excluding Covid years 2020 and 2021)
avg_weighting_matrix <- weighting_matrices %>%
  imap(~ mutate(.x, year = as.integer(str_extract(.y, "\\d+")))) %>%
  bind_rows() %>%
  filter(!year %in% c(2020, 2021)) %>%
  select(-year) %>%
  group_by(mx_state, mx_municipality) %>%
  summarise(across(where(is.numeric), ~ mean(., na.rm = TRUE)), .groups = "drop") %>%
  arrange(mx_state, mx_municipality)

# Sanity check
avg_weighting_matrix %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE))) %>%
  sum()

# Directory for the exported datasets
if (dir.exists(output_dir)) {
  unlink(output_dir, recursive = TRUE)
}
dir.create(output_dir)

# Export each matrix as an xlsx file
walk2(weighting_matrices, names(weighting_matrices), function(df, name) {
  write_xlsx(df, path = file.path(output_dir, paste0(name, ".xlsx")))
})

write_xlsx(avg_weighting_matrix, path = file.path(output_dir, "AVG_WEIGHTING_MATRIX.xlsx"))