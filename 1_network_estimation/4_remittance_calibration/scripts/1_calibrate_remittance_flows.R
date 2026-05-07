# =============================================================================
# Construct quarterly US-state to Mexican-municipality remittance flows
# using ONLY origin-state outflow data + migration weighting matrix.
#
# Municipality inflow data is used ONLY as a validation check:
#   - Compare municipality share distributions between the two sources.
#   - Plot Florida total remittances over time from both datasets.
# =============================================================================

rm(list = ls())
library(tidyverse)
library(readxl)

# -----------------------------------------------------------------------------
# 1. Path Management
# -----------------------------------------------------------------------------
get_script_path <- function() {
  cmd_args <- commandArgs(trailingOnly = FALSE)
  file_arg  <- grep("^--file=", cmd_args, value = TRUE)
  if (length(file_arg) > 0)
    return(normalizePath(sub("^--file=", "", file_arg[1]), winslash = "/", mustWork = TRUE))
  this_file <- tryCatch(
    normalizePath(sys.frame(1)$ofile, winslash = "/", mustWork = TRUE),
    error = function(...) NA_character_
  )
  if (!is.na(this_file)) return(this_file)
  normalizePath(
    file.path(getwd(), "1_network_estimation", "4_remittance_calibration",
              "scripts", "1_calibrate_remittance_flows.R"),
    winslash = "/", mustWork = FALSE
  )
}

script_path  <- get_script_path()
repo_root    <- normalizePath(file.path(dirname(script_path), "..", "..", ".."),
                               winslash = "/", mustWork = TRUE)
path_in_repo <- function(...) normalizePath(file.path(repo_root, ...), winslash = "/", mustWork = FALSE)

# -----------------------------------------------------------------------------
# 2. Helper Functions
# -----------------------------------------------------------------------------
normalize_origin_state <- function(x) {
  dplyr::case_when(
    x == "Carolina Del Norte"                                        ~ "North Carolina",
    x == "Carolina Del Sur"                                          ~ "South Carolina",
    x == "Dakota Del Norte"                                          ~ "North Dakota",
    x == "Dakota Del Sur"                                            ~ "South Dakota",
    x == "Mississipi"                                                ~ "Mississippi",
    x == "Misuri"                                                    ~ "Missouri",
    x == "Nueva Jersey"                                              ~ "New Jersey",
    x == "Nueva York"                                                ~ "New York",
    x == "Nuevo Hampshire"                                           ~ "New Hampshire",
    x == "Nuevo Mexico"                                              ~ "New Mexico",
    x == "Pensilvania"                                               ~ "Pennsylvania",
    x %in% c("Washington, D.c.", "Washington, D.C.", "Washington Dc") ~ "District Of Columbia",
    TRUE                                                             ~ x
  )
}

# Append-safe CSV writer (suppresses header on subsequent writes)
write_or_append_csv <- function(df, path, append_mode) {
  if (!append_mode) {
    readr::write_csv(df, path)
  } else {
    readr::write_csv(df, path, append = TRUE, col_names = FALSE)
  }
}

# -----------------------------------------------------------------------------
# 3. Directory and Path Setup
# -----------------------------------------------------------------------------
weights_dir <- path_in_repo("1_network_estimation", "2_migration_matrix_estimation",
                             "migration_weighting_matrices_2")
banxico_dir <- path_in_repo("1_network_estimation", "3_banxico_cleaning", "output")
output_dir  <- path_in_repo("1_network_estimation", "4_remittance_calibration", "output")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

origin_path          <- file.path(banxico_dir, "banxico_origin_state_remittances_2013q1_2024q4.csv")
municipality_path    <- file.path(banxico_dir, "banxico_municipality_remittances_2013q1_2024q4.csv")
avg_weights_col_path <- file.path(weights_dir, "FINAL_AVG_COL_MATRIX.xlsx")

# Output paths
master_output_path_usd       <- file.path(output_dir, "remittance_flows_origin_state_usd_2013q1_2024q4.csv")
master_output_path_musd      <- file.path(output_dir, "remittance_flows_origin_state_musd_2013q1_2024q4.csv")
validation_output_path       <- file.path(output_dir, "validation_municipality_shares_2013q1_2024q4.csv")
validation_correlation_path  <- file.path(output_dir, "validation_share_correlations_by_year.csv")
validation_correlation_plot_path <- file.path(output_dir, "validation_share_correlations_by_year.png")
total_quarterly_comparison_path <- file.path(output_dir, "validation_total_remittances_by_quarter.csv")
total_quarterly_plot_path <- file.path(output_dir, "validation_total_remittances_by_quarter.png")
total_yearly_comparison_path <- file.path(output_dir, "validation_total_remittances_by_year.csv")
total_yearly_plot_path <- file.path(output_dir, "validation_total_remittances_by_year.png")
origin_mapping_output_path   <- file.path(output_dir, "origin_state_mapping_summary.csv")
florida_plot_path            <- file.path(output_dir, "florida_remittances_comparison.png")

if (file.exists(master_output_path_usd))  unlink(master_output_path_usd)
if (file.exists(master_output_path_musd)) unlink(master_output_path_musd)

# -----------------------------------------------------------------------------
# 4. Load & Prepare Migration Weighting Matrix
# -----------------------------------------------------------------------------
avg_weights_col <- readxl::read_excel(avg_weights_col_path)
state_cols      <- setdiff(names(avg_weights_col), c("mx_state", "mx_municipality", "...1"))

municipality_universe <- avg_weights_col %>%
  select(mx_state, mx_municipality) %>%
  arrange(mx_state, mx_municipality)

# Column-normalise: each column sums to 1 so it distributes one state's
# outflow across all municipalities proportionally to migration shares.
weights_matrix_base <- as.matrix(avg_weights_col[, state_cols])
weights_matrix_base[is.na(weights_matrix_base)] <- 0
weights_matrix_base <- pmax(weights_matrix_base, 1e-12)
weights_matrix_base <- sweep(weights_matrix_base, 2, colSums(weights_matrix_base), "/")

# -----------------------------------------------------------------------------
# 5. Load & Clean Origin-State Outflow Data
# -----------------------------------------------------------------------------
origin_col <- readr::read_csv(origin_path, show_col_types = FALSE) %>%
  mutate(
    us_state_original = us_state,
    us_state          = normalize_origin_state(us_state),
    remittances_usd   = remittances_musd * 1e6
  )

# Save mapping audit
origin_mapping_summary <- origin_col %>%
  group_by(us_state_original, us_state) %>%
  summarise(
    total_remittances_musd = sum(remittances_musd, na.rm = TRUE),
    total_remittances_usd  = sum(remittances_usd,  na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(mapping_status = case_when(
    us_state %in% state_cols               ~ "kept",
    us_state_original == "No Identificado" ~ "dropped_no_identificado",
    TRUE                                   ~ "dropped_unmapped"
  ))
readr::write_csv(origin_mapping_summary, origin_mapping_output_path)

# Keep only states present in the weights matrix
origin_kept <- origin_col %>%
  filter(us_state %in% state_cols) %>%
  group_by(year, quarter, us_state) %>%
  summarise(remittances_usd = sum(remittances_usd, na.rm = TRUE), .groups = "drop")

# -----------------------------------------------------------------------------
# 6. Main Construction Loop
#    For each quarter: multiply each state's outflow by its column in the
#    weights matrix.  No IPFP, no rescaling — origin totals are preserved
#    exactly as reported by Banxico.
# -----------------------------------------------------------------------------
master_started <- FALSE

for (year_value in 2013:2024) {
  message("Processing year ", year_value)
  yearly_quarters <- sort(unique(origin_kept$quarter[origin_kept$year == year_value]))
  yearly_outputs  <- list()

  for (idx in seq_along(yearly_quarters)) {
    quarter_value <- yearly_quarters[[idx]]

    # Full state vector aligned to weights matrix columns
    origin_quarter <- origin_kept %>%
      filter(year == year_value, quarter == quarter_value) %>%
      right_join(tibble(us_state = state_cols), by = "us_state") %>%
      mutate(remittances_usd = replace_na(remittances_usd, 0)) %>%
      arrange(match(us_state, state_cols))

    # Distribute: multiply each column of the weights matrix by that
    # state's total outflow.  Result rows = municipalities, cols = states.
    flow_matrix <- sweep(weights_matrix_base, 2, origin_quarter$remittances_usd, "*")

    # Flatten to long format
    id_cols <- tibble(
      year            = year_value,
      quarter         = quarter_value,
      year_quarter    = paste0(year_value, "Q", quarter_value),
      us_state        = rep(state_cols, each = nrow(municipality_universe)),
      mx_state        = rep(municipality_universe$mx_state,        times = length(state_cols)),
      mx_municipality = rep(municipality_universe$mx_municipality, times = length(state_cols))
    )

    yearly_outputs[[idx]] <- list(
      usd  = id_cols %>% mutate(remittances_usd  = as.vector(flow_matrix)),
      musd = id_cols %>% mutate(remittances_musd = as.vector(flow_matrix) / 1e6)
    )
  }

  write_or_append_csv(
    bind_rows(lapply(yearly_outputs, `[[`, "usd")),
    master_output_path_usd, master_started
  )
  write_or_append_csv(
    bind_rows(lapply(yearly_outputs, `[[`, "musd")),
    master_output_path_musd, master_started
  )
  master_started <- TRUE
}

message("Matrix construction complete.")

# -----------------------------------------------------------------------------
# 7. Validation: Municipality Share Comparison
#
#    For each quarter compute, for every municipality:
#      share_matrix   = municipality's share of total inflows implied by the
#                       origin-state matrix (summed across all US states)
#      share_banxico  = municipality's share of total inflows as directly
#                       reported in the Banxico municipality dataset
#    Then store both shares and their absolute difference.
# -----------------------------------------------------------------------------
message("Computing validation shares...")

# 7a. Municipality shares from the constructed matrix
#     Read back the USD master, aggregate to municipality x quarter level
matrix_muni_shares <- readr::read_csv(master_output_path_usd, show_col_types = FALSE) %>%
  group_by(year, quarter, year_quarter, mx_state, mx_municipality) %>%
  summarise(remittances_usd_matrix = sum(remittances_usd, na.rm = TRUE), .groups = "drop") %>%
  group_by(year_quarter) %>%
  mutate(
    total_matrix_usd   = sum(remittances_usd_matrix),
    share_matrix       = remittances_usd_matrix / total_matrix_usd
  ) %>%
  ungroup()

# 7b. Municipality shares from raw Banxico municipality inflow data
banxico_muni_shares <- readr::read_csv(municipality_path, show_col_types = FALSE) %>%
  mutate(remittances_usd = remittances_musd * 1e6) %>%
  group_by(year, quarter, mx_state, mx_municipality) %>%
  summarise(remittances_usd_banxico = sum(remittances_usd, na.rm = TRUE), .groups = "drop") %>%
  mutate(year_quarter = paste0(year, "Q", quarter)) %>%
  group_by(year_quarter) %>%
  mutate(
    total_banxico_usd  = sum(remittances_usd_banxico),
    share_banxico      = remittances_usd_banxico / total_banxico_usd
  ) %>%
  ungroup()

# 7c. Join and compute divergence
validation <- matrix_muni_shares %>%
  full_join(
    banxico_muni_shares %>%
      select(year_quarter, mx_state, mx_municipality,
             remittances_usd_banxico, total_banxico_usd, share_banxico),
    by = c("year_quarter", "mx_state", "mx_municipality")
  ) %>%
  mutate(
    share_matrix  = replace_na(share_matrix,  0),
    share_banxico = replace_na(share_banxico, 0),
    share_diff    = share_matrix - share_banxico,      # signed difference
    share_abs_diff = abs(share_diff)                   # magnitude
  ) %>%
  select(
    year, quarter, year_quarter,
    mx_state, mx_municipality,
    remittances_usd_matrix, total_matrix_usd, share_matrix,
    remittances_usd_banxico, total_banxico_usd, share_banxico,
    share_diff, share_abs_diff
  ) %>%
  arrange(year_quarter, mx_state, mx_municipality)

readr::write_csv(validation, validation_output_path)
message("Validation file written: ", validation_output_path)

# Quick summary of average absolute divergence per quarter
validation_summary <- validation %>%
  group_by(year_quarter) %>%
  summarise(
    mean_abs_diff   = mean(share_abs_diff, na.rm = TRUE),
    median_abs_diff = median(share_abs_diff, na.rm = TRUE),
    max_abs_diff    = max(share_abs_diff, na.rm = TRUE),
    .groups = "drop"
  )
message("\nValidation summary (mean absolute share difference per quarter):")
print(validation_summary, n = Inf)

# Robustness check: yearly correlation between the municipality shares implied
# by the origin-state output matrix and the shares observed in Banxico inflows.
validation_share_correlations <- validation %>%
  group_by(year) %>%
  summarise(
    share_correlation = cor(share_matrix, share_banxico, use = "complete.obs"),
    n_municipality_quarters = sum(complete.cases(share_matrix, share_banxico)),
    .groups = "drop"
  ) %>%
  arrange(year)

readr::write_csv(validation_share_correlations, validation_correlation_path)
message("Validation share correlations written: ", validation_correlation_path)

validation_correlation_plot <- ggplot(
  validation_share_correlations,
  aes(x = year, y = share_correlation)
) +
  geom_line(colour = "#1f77b4", linewidth = 0.9) +
  geom_point(colour = "#1f77b4", size = 2) +
  scale_x_continuous(
    breaks = validation_share_correlations$year,
    labels = as.character(validation_share_correlations$year)
  ) +
  scale_y_continuous(limits = c(0, 1), labels = scales::number_format(accuracy = 0.01)) +
  labs(
    title = "Yearly Correlation of Municipality Remittance Shares",
    subtitle = "Origin-state output matrix shares vs Banxico municipality inflow shares",
    x = NULL,
    y = "Correlation coefficient"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 14),
    plot.subtitle = element_text(colour = "grey40", size = 10),
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid.minor = element_blank()
  )

ggsave(validation_correlation_plot_path, validation_correlation_plot, width = 9, height = 5, dpi = 150)
message("Validation share correlation plot saved: ", validation_correlation_plot_path)

# Robustness check: compare total raw remittances in the full origin-state
# outflow dataset and the full municipality inflow dataset. This keeps every
# category, including non-identified/unmapped origin states and municipalities.
origin_totals_quarterly <- origin_col %>%
  group_by(year, quarter) %>%
  summarise(remittances_usd = sum(remittances_usd, na.rm = TRUE), .groups = "drop") %>%
  mutate(
    year_quarter = paste0(year, "Q", quarter),
    source = "Origin-state outflows"
  )

municipality_totals_quarterly <- readr::read_csv(municipality_path, show_col_types = FALSE) %>%
  mutate(remittances_usd = remittances_musd * 1e6) %>%
  group_by(year, quarter) %>%
  summarise(remittances_usd = sum(remittances_usd, na.rm = TRUE), .groups = "drop") %>%
  mutate(
    year_quarter = paste0(year, "Q", quarter),
    source = "Municipality inflows"
  )

total_remittances_quarterly <- bind_rows(
  origin_totals_quarterly,
  municipality_totals_quarterly
) %>%
  mutate(time = year + (quarter - 1) / 4) %>%
  arrange(year, quarter, source)

readr::write_csv(total_remittances_quarterly, total_quarterly_comparison_path)
message("Quarterly total remittances comparison written: ", total_quarterly_comparison_path)

total_quarterly_plot <- ggplot(
  total_remittances_quarterly,
  aes(x = time, y = remittances_usd / 1e6, colour = source, linetype = source)
) +
  geom_line(linewidth = 0.9) +
  geom_point(size = 1.6) +
  scale_colour_manual(
    values = c(
      "Origin-state outflows" = "#d62728",
      "Municipality inflows" = "#1f77b4"
    )
  ) +
  scale_linetype_manual(
    values = c(
      "Origin-state outflows" = "dashed",
      "Municipality inflows" = "solid"
    )
  ) +
  scale_x_continuous(
    breaks = 2013:2024,
    labels = as.character(2013:2024)
  ) +
  scale_y_continuous(labels = scales::comma_format(suffix = "M")) +
  labs(
    title = "Total Remittances by Quarter",
    subtitle = "Full origin-state outflows vs full municipality inflows",
    x = NULL,
    y = "Remittances (USD millions)",
    colour = "Dataset",
    linetype = "Dataset"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 14),
    plot.subtitle = element_text(colour = "grey40", size = 10),
    legend.position = "bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid.minor = element_blank()
  )

ggsave(total_quarterly_plot_path, total_quarterly_plot, width = 10, height = 5.5, dpi = 150)
message("Quarterly total remittances plot saved: ", total_quarterly_plot_path)

total_remittances_yearly <- total_remittances_quarterly %>%
  group_by(year, source) %>%
  summarise(remittances_usd = sum(remittances_usd, na.rm = TRUE), .groups = "drop") %>%
  arrange(year, source)

readr::write_csv(total_remittances_yearly, total_yearly_comparison_path)
message("Yearly total remittances comparison written: ", total_yearly_comparison_path)

total_yearly_plot <- ggplot(
  total_remittances_yearly,
  aes(x = year, y = remittances_usd / 1e6, colour = source, linetype = source)
) +
  geom_line(linewidth = 0.9) +
  geom_point(size = 2) +
  scale_colour_manual(
    values = c(
      "Origin-state outflows" = "#d62728",
      "Municipality inflows" = "#1f77b4"
    )
  ) +
  scale_linetype_manual(
    values = c(
      "Origin-state outflows" = "dashed",
      "Municipality inflows" = "solid"
    )
  ) +
  scale_x_continuous(
    breaks = 2013:2024,
    labels = as.character(2013:2024)
  ) +
  scale_y_continuous(labels = scales::comma_format(suffix = "M")) +
  labs(
    title = "Total Remittances by Year",
    subtitle = "Full origin-state outflows vs full municipality inflows",
    x = NULL,
    y = "Remittances (USD millions)",
    colour = "Dataset",
    linetype = "Dataset"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 14),
    plot.subtitle = element_text(colour = "grey40", size = 10),
    legend.position = "bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid.minor = element_blank()
  )

ggsave(total_yearly_plot_path, total_yearly_plot, width = 10, height = 5.5, dpi = 150)
message("Yearly total remittances plot saved: ", total_yearly_plot_path)

# -----------------------------------------------------------------------------
# 8. Florida Plot: total remittances over time from both datasets
#
#    - Matrix source:  sum of all municipality flows whose origin = Florida,
#                      taken from the constructed USD matrix.
#    - Banxico source: Florida's directly reported quarterly outflow from the
#                      origin-state dataset (before any matrix construction).
# -----------------------------------------------------------------------------
if (FALSE) {
message("Building Florida comparison plot...")

# Florida from the constructed matrix (sum across all destination municipalities)
florida_matrix <- readr::read_csv(master_output_path_usd, show_col_types = FALSE) %>%
  filter(us_state == "Florida") %>%
  group_by(year_quarter, year, quarter) %>%
  summarise(remittances_usd = sum(remittances_usd, na.rm = TRUE), .groups = "drop") %>%
  mutate(source = "Origin-state matrix")

# Florida from the raw Banxico origin-state dataset
florida_banxico <- origin_kept %>%
  filter(us_state == "Florida") %>%
  mutate(year_quarter = paste0(year, "Q", quarter),
         source       = "Banxico origin-state (raw)")

# Bind and create a numeric time index for clean x-axis ordering
florida_plot_data <- bind_rows(
  florida_matrix  %>% select(year, quarter, year_quarter, remittances_usd, source),
  florida_banxico %>% select(year, quarter, year_quarter, remittances_usd, source)
) %>%
  mutate(
    # Fractional year for ordered x-axis (Q1=.0, Q2=.25, Q3=.5, Q4=.75)
    time = year + (quarter - 1) / 4
  )

florida_plot <- ggplot(florida_plot_data,
                       aes(x = time, y = remittances_usd / 1e6,
                           colour = source, linetype = source)) +
  geom_line(linewidth = 0.9) +
  geom_point(size = 1.8) +
  scale_colour_manual(
    values = c("Origin-state matrix"          = "#1f77b4",
               "Banxico origin-state (raw)"   = "#d62728")
  ) +
  scale_linetype_manual(
    values = c("Origin-state matrix"          = "solid",
               "Banxico origin-state (raw)"   = "dashed")
  ) +
  scale_x_continuous(
    breaks = 2013:2024,
    labels = as.character(2013:2024)
  ) +
  scale_y_continuous(labels = scales::comma_format(suffix = "M")) +
  labs(
    title    = "Florida: Total Remittances Sent — Matrix vs. Banxico Raw",
    subtitle = "Quarterly 2013 Q1 – 2024 Q4  |  USD millions",
    x        = NULL,
    y        = "Remittances (USD millions)",
    colour   = "Source",
    linetype = "Source",
    caption  = paste0(
      "Matrix source: origin-state outflows distributed via migration weights matrix.\n",
      "Banxico raw: directly reported state-level outflow (before matrix construction).\n",
      "The two series should be identical — any gap reflects dropped/unmapped states."
    )
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title      = element_text(face = "bold", size = 14),
    plot.subtitle   = element_text(colour = "grey40", size = 10),
    plot.caption    = element_text(colour = "grey50", size = 8, hjust = 0),
    legend.position = "bottom",
    legend.title    = element_blank(),
    axis.text.x     = element_text(angle = 45, hjust = 1),
    panel.grid.minor = element_blank()
  )

ggsave(florida_plot_path, florida_plot, width = 10, height = 5.5, dpi = 150)
message("Florida plot saved: ", florida_plot_path)
}

# Replace the previous matrix-vs-origin plot with the requested comparison:
# origin-state Florida outflows vs Florida inferred from municipality inflows.
message("Building Florida comparison plot...")

avg_row_weights <- readxl::read_excel(file.path(weights_dir, "FINAL_AVG_ROW_MATRIX.xlsx")) %>%
  select(mx_state, mx_municipality, Florida) %>%
  rename(avg_florida_weight = Florida)

get_florida_municipality_weights <- function(year_value) {
  yearly_weight_path <- file.path(weights_dir, paste0("WEIGHTING_MATRIX_", year_value, ".xlsx"))
  
  if (!file.exists(yearly_weight_path)) {
    warning("Missing weighting matrix for ", year_value, "; using average row weights for Florida.")
    return(avg_row_weights %>% rename(florida_weight = avg_florida_weight))
  }
  
  yearly_weights <- readxl::read_excel(yearly_weight_path)
  numeric_cols <- names(yearly_weights %>% select(where(is.numeric)))
  
  if (!("Florida" %in% numeric_cols)) {
    warning("Florida is missing from the ", year_value, " weighting matrix; using average row weights.")
    return(avg_row_weights %>% rename(florida_weight = avg_florida_weight))
  }
  
  yearly_weights %>%
    mutate(
      row_total = rowSums(across(all_of(numeric_cols)), na.rm = TRUE),
      florida_weight = if_else(row_total > 0, Florida / row_total, NA_real_)
    ) %>%
    select(mx_state, mx_municipality, florida_weight) %>%
    left_join(avg_row_weights, by = c("mx_state", "mx_municipality")) %>%
    mutate(florida_weight = coalesce(florida_weight, avg_florida_weight)) %>%
    select(mx_state, mx_municipality, florida_weight)
}

florida_weight_panel <- map_dfr(2013:2024, function(year_value) {
  get_florida_municipality_weights(year_value) %>%
    mutate(year = year_value)
})

florida_origin_state <- origin_kept %>%
  filter(us_state == "Florida") %>%
  mutate(
    year_quarter = paste0(year, "Q", quarter),
    source = "Banxico origin-state"
  )

florida_municipality_inflow <- readr::read_csv(municipality_path, show_col_types = FALSE) %>%
  mutate(remittances_usd = remittances_musd * 1e6) %>%
  group_by(year, quarter, mx_state, mx_municipality) %>%
  summarise(remittances_usd = sum(remittances_usd, na.rm = TRUE), .groups = "drop") %>%
  left_join(
    florida_weight_panel,
    by = c("year", "mx_state", "mx_municipality")
  ) %>%
  mutate(florida_weight = replace_na(florida_weight, 0)) %>%
  group_by(year, quarter) %>%
  summarise(
    remittances_usd = sum(remittances_usd * florida_weight, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    year_quarter = paste0(year, "Q", quarter),
    source = "Municipality inflows weighted to Florida"
  )

florida_plot_data <- bind_rows(
  florida_origin_state %>% select(year, quarter, year_quarter, remittances_usd, source),
  florida_municipality_inflow %>% select(year, quarter, year_quarter, remittances_usd, source)
) %>%
  mutate(time = year + (quarter - 1) / 4)

florida_plot <- ggplot(
  florida_plot_data,
  aes(x = time, y = remittances_usd / 1e6, colour = source, linetype = source)
) +
  geom_line(linewidth = 0.9) +
  geom_point(size = 1.8) +
  scale_colour_manual(
    values = c(
      "Banxico origin-state" = "#d62728",
      "Municipality inflows weighted to Florida" = "#1f77b4"
    )
  ) +
  scale_linetype_manual(
    values = c(
      "Banxico origin-state" = "dashed",
      "Municipality inflows weighted to Florida" = "solid"
    )
  ) +
  scale_x_continuous(
    breaks = 2013:2024,
    labels = as.character(2013:2024)
  ) +
  scale_y_continuous(labels = scales::comma_format(suffix = "M")) +
  labs(
    title    = "Florida: Origin-State Remittances vs Municipality-Inflow Estimate",
    subtitle = "Quarterly 2013 Q1 - 2024 Q4 | USD millions",
    x        = NULL,
    y        = "Remittances (USD millions)",
    colour   = "Source",
    linetype = "Source",
    caption  = paste0(
      "Origin-state: directly reported Banxico Florida outflows.\n",
      "Municipality inflows: Banxico municipality receipts multiplied by Florida row-normalized migration weights.\n",
      "When the yearly Florida weight is missing, the average row-normalized Florida weight is used."
    )
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title       = element_text(face = "bold", size = 14),
    plot.subtitle    = element_text(colour = "grey40", size = 10),
    plot.caption     = element_text(colour = "grey50", size = 8, hjust = 0),
    legend.position  = "bottom",
    legend.title     = element_blank(),
    axis.text.x      = element_text(angle = 45, hjust = 1),
    panel.grid.minor = element_blank()
  )

ggsave(florida_plot_path, florida_plot, width = 10, height = 5.5, dpi = 150)
message("Florida plot overwritten with municipality-inflow comparison: ", florida_plot_path)

# -----------------------------------------------------------------------------
# 9. Done
# -----------------------------------------------------------------------------
message("\nAll outputs written:")
message("  Matrix (USD)       -> ", master_output_path_usd)
message("  Matrix (MUSD)      -> ", master_output_path_musd)
message("  Validation shares  -> ", validation_output_path)
message("  Origin mapping     -> ", origin_mapping_output_path)
message("  Florida plot       -> ", florida_plot_path)
