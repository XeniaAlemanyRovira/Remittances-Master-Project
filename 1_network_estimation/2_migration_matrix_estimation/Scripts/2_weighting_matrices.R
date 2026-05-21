# ==============================================================================
# SCRIPT: Migration Weighting Matrix Estimation & Robustness Checks
# ==============================================================================

rm(list = ls())

library(tidyverse)
library(readxl)
library(writexl)
library(corrplot)
library(lsa)
library(ggplot2)
library(scales)

# --- Config ---
input_dir <- "1_network_estimation/2_migration_matrix_estimation/yearly_migration_matrices_2"
output_dir <- "1_network_estimation/2_migration_matrix_estimation/migration_weighting_matrices_2"
years    <- 2010:2024

# --- Function that loads each years migration matrix ---
matrices <- map(years, function(yr) {
  path <- file.path(input_dir, paste0("MIGRATION_MATRIX_", yr, ".xlsx"))
  if (!file.exists(path)) {
    warning("File not found for year ", yr)
    return(NULL)
  }
  read_excel(path) %>%
    select(-any_of("Total")) %>% # drops Total column if exists
    filter(mx_state != "Total")   # drops Total row
}) %>%
  set_names(paste0("MIGRATION_MATRIX_", years))

# --- 1 - Compute raw weighting matrices for all years ---
weighting_matrices_raw <- map(matrices, function(df) {
  total <- df %>%
    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE))) %>%
    sum()
  df %>%
    mutate(across(where(is.numeric), ~ . / total))
})

# --- 1.2 - ROW NORMALIZATION (Each Mexican Municipality sums to 1) ---
normalize_municipality_rows <- function(df, allow_zero_rows = FALSE) {
  id_cols <- c("mx_state", "mx_municipality")
  numeric_cols <- names(df %>% select(where(is.numeric)))
  row_sum <- rowSums(df %>% select(all_of(numeric_cols)), na.rm = TRUE)
  
  if (!allow_zero_rows && any(row_sum <= 0, na.rm = TRUE)) {
    zero_rows <- df %>%
      mutate(row_sum = row_sum) %>%
      filter(row_sum <= 0) %>%
      select(any_of(id_cols), row_sum)
    
    stop(
      "Cannot row-normalize municipalities with zero total migration weight. Examples: ",
      paste(head(paste(zero_rows$mx_state, zero_rows$mx_municipality, sep = " - "), 10), collapse = "; ")
    )
  }
  
  df %>%
    mutate(across(all_of(numeric_cols), ~ if_else(row_sum > 0, . / row_sum, 0)))
}

normalize_us_state_columns <- function(df) {
  numeric_cols <- names(df %>% select(where(is.numeric)))
  
  df %>%
    mutate(across(all_of(numeric_cols), ~ {
      col_sum <- sum(.x, na.rm = TRUE)
      if (col_sum > 0) .x / col_sum else 0
    }))
}

normalize_global_total <- function(df) {
  numeric_cols <- names(df %>% select(where(is.numeric)))
  total <- df %>%
    summarise(across(all_of(numeric_cols), ~ sum(.x, na.rm = TRUE))) %>%
    sum()
  
  if (total <= 0) {
    stop("Cannot globally normalize a matrix with non-positive total weight.")
  }
  
  df %>%
    mutate(across(all_of(numeric_cols), ~ . / total))
}

weighting_matrices_rows <- map(matrices, function(df) {
  df_clean <- df %>% filter(mx_state != "Total") %>% select(-any_of("Total"))
  
  df_clean %>%
    mutate(row_sum = rowSums(across(where(is.numeric)), na.rm = TRUE)) %>%
    mutate(across(where(is.numeric) & !all_of("row_sum"), 
                  ~ if_else(row_sum > 0, . / row_sum, 0))) %>%
    select(-row_sum)
})

# --- 1.3 - COLUMN NORMALIZATION (Each US State sums to 1) ---
weighting_matrices_cols <- map(matrices, function(df) {
  df_clean <- df %>% filter(mx_state != "Total") %>% select(-any_of("Total"))

  df_clean %>%
    mutate(across(where(is.numeric), ~ {
      col_sum <- sum(.x, na.rm = TRUE)
      if (col_sum > 0) .x / col_sum else 0
    }))
})

# --- Robustness check: Verify column-normalized matrices ---
column_sums_check <- weighting_matrices_cols %>%
  imap_dfr(~ {
    col_sums <- colSums(select(.x, where(is.numeric)), na.rm = TRUE)
    tibble(matrix = .y, column = names(col_sums), sum = col_sums, is_one = near(col_sums, 1, tol = 1e-8))
  })
View(column_sums_check)

# --- Robustness check: Verify row-normalized matrices ---
row_sums_check <- weighting_matrices_rows %>%
  imap_dfr(~ {
    row_sums <- rowSums(select(.x, where(is.numeric)), na.rm = TRUE)
    tibble(matrix = .y, mx_state = .x$mx_state, mx_municipality = .x$mx_municipality, sum = row_sums, is_one = near(row_sums, 1, tol = 1e-8))
  })
View(row_sums_check)

# --- 2 - Compute average weights per missing state ---
compute_avg_weight <- function(state_col, excl_years) {
  weighting_matrices_raw[paste0("MIGRATION_MATRIX_", setdiff(years, excl_years))] %>%
    map(~ select(.x, mx_state, mx_municipality, any_of(state_col))) %>%
    reduce(full_join, by = c("mx_state", "mx_municipality")) %>%
    mutate(avg = rowMeans(across(where(is.numeric)), na.rm = TRUE)) %>%
    summarise(total_avg_weight = sum(avg, na.rm = TRUE)) %>%
    pull()
}

florida_total_avg_weight     <- compute_avg_weight("Florida",     c(2013, 2020, 2021))
alaska_total_avg_weight      <- compute_avg_weight("Alaska",      c(2020, 2021))
connecticut_total_avg_weight <- compute_avg_weight("Connecticut", c(2024, 2020, 2021))

# --- CORRELATION CHECK ---
yearly_weights_wide <- map_dfr(names(weighting_matrices_raw), function(name) {
  weighting_matrices_raw[[name]] %>%
    mutate(year = name) %>%
    select(mx_state, mx_municipality, year, where(is.numeric)) %>%
    pivot_longer(cols = where(is.numeric), names_to = "us_state", values_to = "weight") %>%
    unite("pair", mx_state, mx_municipality, us_state)
}) %>%
  pivot_wider(names_from = year, values_from = weight) %>%
  select(-pair)

cor_matrix <- cor(yearly_weights_wide, use = "pairwise.complete.obs")
corrplot(cor_matrix, method = "color", type = "upper", tl.col = "black", addCoef.col = "black", number.cex = 0.7, title = "Year-to-Year Correlation", mar = c(0,0,1,0))

# --- STRUCTURAL DRIFT CHECK ---
drift_analysis <- tibble(
  year_pair = paste0(years[-1], " vs ", years[-length(years)]),
  distance = map_dbl(2:length(years), function(i) {
    curr <- yearly_weights_wide[[i]]; prev <- yearly_weights_wide[[i-1]]
    sqrt(sum((curr - prev)^2, na.rm = TRUE))
  })
)
ggplot(drift_analysis, aes(x = year_pair, y = distance, group = 1)) + geom_line(color = "red") + geom_point() + theme_minimal()

# --- COSINE SIMILARITY CHECK ---
cosine_check <- map_dfr(2:ncol(yearly_weights_wide), function(i) {
  vec1 <- yearly_weights_wide[[i]]; vec2 <- yearly_weights_wide[[i-1]]
  valid_indices <- which(!is.na(vec1) & !is.na(vec2))
  sim <- lsa::cosine(vec1[valid_indices], vec2[valid_indices])
  tibble(pair = paste0(names(yearly_weights_wide)[i], " vs ", names(yearly_weights_wide)[i-1]), similarity = as.numeric(sim))
})

# --- TOTAL VOLUME CHECK ---
annual_totals <- map_dfr(names(matrices), function(name) {
  grand_total <- matrices[[name]] %>% select(where(is.numeric)) %>% as.matrix() %>% sum(na.rm = TRUE)
  tibble(year = as.numeric(gsub("MIGRATION_MATRIX_", "", name)), total_migrants = grand_total)
})

ggplot(annual_totals, aes(x = year, y = total_migrants)) + geom_line(color = "steelblue") + geom_point() + scale_y_continuous(labels = comma) + theme_minimal()

# --- GROWTH CHANGE CHECK ---
annual_totals <- annual_totals %>% mutate(pct_change = (total_migrants - lag(total_migrants)) / lag(total_migrants) * 100)
avg_growth_pre_covid <- annual_totals %>% filter(year < 2020) %>% pull(pct_change) %>% mean(na.rm = TRUE)

# --- 3 - Compute the denominator with the mean imputation for missing states ---
get_observed_total <- function(yr) {
  matrices[[paste0("MIGRATION_MATRIX_", yr)]] %>% summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE))) %>% sum()
}

total_2013_imputed <- get_observed_total(2013) / (1 - florida_total_avg_weight)
total_2020_imputed <- get_observed_total(2020) / (1 - alaska_total_avg_weight)
total_2024_imputed <- get_observed_total(2024) / (1 - connecticut_total_avg_weight)

# --- 4 - Compute final weighting matrices ---
weighting_matrices <- imap(matrices, function(df, name) {
  total <- case_when(
    name == "MIGRATION_MATRIX_2013" ~ total_2013_imputed,
    name == "MIGRATION_MATRIX_2020" ~ total_2020_imputed,
    name == "MIGRATION_MATRIX_2024" ~ total_2024_imputed,
    TRUE ~ df %>% summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE))) %>% sum()
  )
  df %>% mutate(across(where(is.numeric), ~ . / total))
}) %>% set_names(paste0("WEIGHTING_MATRIX_", years))

known_missing_state_years <- tribble(
  ~year, ~us_state,
  2013L, "Florida",
  2020L, "Alaska",
  2024L, "Connecticut"
)

impute_known_missing_state_columns <- function(matrix_list, normalization) {
  out <- matrix_list
  id_cols <- c("mx_state", "mx_municipality")
  
  for (i in seq_len(nrow(known_missing_state_years))) {
    missing_year <- known_missing_state_years$year[[i]]
    missing_state <- known_missing_state_years$us_state[[i]]
    target_name <- names(out)[str_detect(names(out), paste0(missing_year, "$"))][1]
    
    if (is.na(target_name)) {
      next
    }
    
    reference_names <- names(out)[
      !str_detect(names(out), paste0(missing_year, "$")) &
        !str_detect(names(out), "2020$|2021$")
    ]
    reference_names <- reference_names[
      map_lgl(out[reference_names], ~ missing_state %in% names(.x))
    ]
    
    if (length(reference_names) == 0) {
      warning("No reference years available to impute ", missing_state, " in ", missing_year)
      next
    }
    
    imputed_state_weights <- map_dfr(reference_names, function(name) {
      out[[name]] %>%
        select(all_of(id_cols), all_of(missing_state)) %>%
        mutate(reference_matrix = name)
    }) %>%
      group_by(across(all_of(id_cols))) %>%
      summarise(imputed_state_weight = mean(.data[[missing_state]], na.rm = TRUE), .groups = "drop")
    
    target_matrix <- out[[target_name]]
    if (!missing_state %in% names(target_matrix)) {
      target_matrix[[missing_state]] <- NA_real_
    }
    
    target_matrix <- target_matrix %>%
      left_join(imputed_state_weights, by = id_cols) %>%
      mutate("{missing_state}" := imputed_state_weight) %>%
      select(-imputed_state_weight)
    
    if (normalization == "global") {
      numeric_cols <- names(target_matrix %>% select(where(is.numeric)))
      other_cols <- setdiff(numeric_cols, missing_state)
      imputed_total <- sum(target_matrix[[missing_state]], na.rm = TRUE)
      observed_other_total <- target_matrix %>%
        summarise(across(all_of(other_cols), ~ sum(.x, na.rm = TRUE))) %>%
        sum()
      
      if (observed_other_total > 0 && imputed_total < 1) {
        target_matrix <- target_matrix %>%
          mutate(across(all_of(other_cols), ~ . * ((1 - imputed_total) / observed_other_total)))
      }
      target_matrix <- normalize_global_total(target_matrix)
    } else if (normalization == "row") {
      target_matrix <- normalize_municipality_rows(target_matrix, allow_zero_rows = TRUE)
    } else if (normalization == "column") {
      target_matrix <- normalize_us_state_columns(target_matrix)
    } else {
      stop("Unknown normalization type: ", normalization)
    }
    
    out[[target_name]] <- target_matrix
  }
  
  out
}

weighting_matrices_raw_final <- impute_known_missing_state_columns(weighting_matrices_raw, "global")
weighting_matrices_rows_final <- impute_known_missing_state_columns(weighting_matrices_rows, "row")
weighting_matrices_cols_final <- impute_known_missing_state_columns(weighting_matrices_cols, "column")

# --- Average weighting matrix (Excluding Covid) ---
avg_weighting_matrix <- weighting_matrices %>%
  imap(~ mutate(.x, year = as.integer(str_extract(.y, "\\d+")))) %>%
  bind_rows() %>%
  filter(!year %in% c(2020, 2021)) %>%
  select(-year) %>%
  group_by(mx_state, mx_municipality) %>%
  summarise(across(where(is.numeric), ~ mean(., na.rm = TRUE)), .groups = "drop")

# --- Export ---
if (dir.exists(output_dir)) unlink(output_dir, recursive = TRUE)
dir.create(output_dir)
walk2(weighting_matrices, names(weighting_matrices), function(df, name) write_xlsx(df, path = file.path(output_dir, paste0(name, ".xlsx"))))
write_xlsx(avg_weighting_matrix, path = file.path(output_dir, "AVG_WEIGHTING_MATRIX.xlsx"))

# --- FINAL ROBUSTNESS CHECK (FIXED) ---
avg_numeric_cols <- names(avg_weighting_matrix %>% select(where(is.numeric)))

deviation_results <- imap_dfr(weighting_matrices, function(df, name) {
  
  # FIX: Intersect columns to avoid "Florida/Alaska" missing errors in specific years
  common_cols <- intersect(names(df), avg_numeric_cols)
  
  year_vec <- df %>% filter(mx_municipality != "Total") %>% 
    select(all_of(common_cols)) %>% as.matrix() %>% as.vector()
  
  avg_vec <- avg_weighting_matrix %>% 
    select(all_of(common_cols)) %>% as.matrix() %>% as.vector()
  
  valid_idx <- which(!is.na(year_vec) & !is.na(avg_vec))
  
  tibble(
    year_label = name,
    year = as.integer(stringr::str_extract(name, "\\d+")),
    euclidean_dist = sqrt(sum((year_vec[valid_idx] - avg_vec[valid_idx])^2)),
    cosine_similarity = as.numeric(lsa::cosine(year_vec[valid_idx], avg_vec[valid_idx]))
  )
})

# Final Visualizations
ggplot(deviation_results, aes(x = year, y = cosine_similarity)) + geom_line() + geom_point(aes(color = year %in% c(2020, 2021))) + theme_minimal()
ggplot(deviation_results, aes(x = year, y = euclidean_dist)) + geom_col(aes(fill = year %in% c(2020, 2021))) + theme_minimal()

# Check that the total weight of every matrix is equal to 1
matrix_total_checks <- weighting_matrices %>%
  imap_dfr(~ tibble(
    matrix = .y,
    total = .x %>%
      filter(mx_municipality != "Total") %>%
      summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE))) %>%
      sum(),
    is_equal_to_one = near(total, 1, tol = 1e-8)
  ))

print(matrix_total_checks)

if (any(!matrix_total_checks$is_equal_to_one)) {
  failed_matrices <- matrix_total_checks %>%
    filter(!is_equal_to_one) %>%
    pull(matrix)
  
  warning(
    "The following global weighting matrices do not sum exactly to 1 because of known missing-state imputations: ",
    paste(failed_matrices, collapse = ", ")
  )
}

# There are three years that do not exactly sum to one but they sum up almost to 1, that is because we are imputing the average weight of that problematic state on the problematic year.

# The 2020 Alaska not summing to 1 is due to the data not containing the state directly. 

# The other two years (2013 and 2024) are caused by an error in the data. For both years, the municipality dataset that should give the information is not by municipality but by sex. Therefore, the code cannot extract the information from it. 

# --- EXPORT AVERAGES FOR RAW, ROW, AND COLUMN NORMALIZATIONS ---

# 1. Average of Raw Weighting Matrices (Global normalization)
avg_raw_matrix <- weighting_matrices_raw_final %>%
  imap(~ mutate(.x, year = as.integer(str_extract(.y, "\\d+")))) %>%
  bind_rows() %>%
  filter(!year %in% c(2020, 2021)) %>%
  select(-year) %>%
  group_by(mx_state, mx_municipality) %>%
  summarise(across(where(is.numeric), ~ mean(., na.rm = TRUE)), .groups = "drop") %>%
  normalize_global_total()

avg_raw_total_check <- avg_raw_matrix %>%
  summarise(total_weight = sum(across(where(is.numeric)), na.rm = TRUE)) %>%
  mutate(is_equal_to_one = near(total_weight, 1, tol = 1e-8))

print(avg_raw_total_check)

if (!avg_raw_total_check$is_equal_to_one) {
  stop("FINAL_AVG_RAW_MATRIX total does not sum to 1 after missing-state imputation.")
}

# 2. Average of Row Weighting Matrices (Each row sums to 1)
avg_row_matrix <- weighting_matrices_rows_final %>%
  imap(~ mutate(.x, year = as.integer(str_extract(.y, "\\d+")))) %>%
  bind_rows() %>%
  filter(!year %in% c(2020, 2021)) %>%
  select(-year) %>%
  group_by(mx_state, mx_municipality) %>%
  summarise(across(where(is.numeric), ~ mean(., na.rm = TRUE)), .groups = "drop") %>%
  normalize_municipality_rows()

avg_row_sums_check <- avg_row_matrix %>%
  mutate(row_sum = rowSums(across(where(is.numeric)), na.rm = TRUE)) %>%
  summarise(
    min_row_sum = min(row_sum, na.rm = TRUE),
    mean_row_sum = mean(row_sum, na.rm = TRUE),
    max_row_sum = max(row_sum, na.rm = TRUE),
    max_abs_deviation_from_one = max(abs(row_sum - 1), na.rm = TRUE),
    all_rows_sum_to_one = all(near(row_sum, 1, tol = 1e-8))
  )

print(avg_row_sums_check)

if (!avg_row_sums_check$all_rows_sum_to_one) {
  stop("FINAL_AVG_ROW_MATRIX rows do not sum to 1 after normalization.")
}

# 3. Average of Column Weighting Matrices (Each column sums to 1)
avg_col_matrix <- weighting_matrices_cols_final %>%
  imap(~ mutate(.x, year = as.integer(str_extract(.y, "\\d+")))) %>%
  bind_rows() %>%
  filter(!year %in% c(2020, 2021)) %>%
  select(-year) %>%
  group_by(mx_state, mx_municipality) %>%
  summarise(across(where(is.numeric), ~ mean(., na.rm = TRUE)), .groups = "drop") %>%
  normalize_us_state_columns()

avg_col_sums_check <- avg_col_matrix %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  pivot_longer(everything(), names_to = "us_state", values_to = "column_sum") %>%
  summarise(
    min_column_sum = min(column_sum, na.rm = TRUE),
    mean_column_sum = mean(column_sum, na.rm = TRUE),
    max_column_sum = max(column_sum, na.rm = TRUE),
    max_abs_deviation_from_one = max(abs(column_sum - 1), na.rm = TRUE),
    all_columns_sum_to_one = all(near(column_sum, 1, tol = 1e-8))
  )

print(avg_col_sums_check)

if (!avg_col_sums_check$all_columns_sum_to_one) {
  stop("FINAL_AVG_COL_MATRIX columns do not sum to 1 after missing-state imputation.")
}

# Exporting the three summary matrices
write_xlsx(avg_raw_matrix, path = file.path(output_dir, "FINAL_AVG_RAW_MATRIX.xlsx"))
write_xlsx(avg_row_matrix, path = file.path(output_dir, "FINAL_AVG_ROW_MATRIX.xlsx"))
write_xlsx(avg_col_matrix, path = file.path(output_dir, "FINAL_AVG_COL_MATRIX.xlsx"))

message("Success: Average Raw, Row, and Column matrices exported to ", output_dir)

###
# Main outputs: 
# - FINAL_AVG_RAW_MATRIX.xlsx
# - FINAL_AVG_ROW_MATRIX.xlsx
# - FINAL_AVG_COL_MATRIX.xlsx
###
