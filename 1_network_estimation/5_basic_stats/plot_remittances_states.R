#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(ggplot2)
  library(stringr)
})

csv_path <- "C:/Users/xenia/Escriptori/BSE/TFM/Remittances-Master-Project/1_network_estimation/4_remittance_calibration/output/remittance_flows_origin_state_usd_2013q1_2024q4.csv"

df <- read_csv(csv_path, show_col_types = FALSE)

# --- DEBUG: inspect the data first ---
cat("Columns:", paste(names(df), collapse=", "), "\n")
cat("First few rows:\n")
print(head(df, 3))

# --- State column ---
state_candidates <- c("origin_state","state","us_state","origin","state_name","origin_state_name")
state_col <- intersect(state_candidates, names(df))[1]
if (is.na(state_col)) stop("State column not found. Columns: ", paste(names(df), collapse=", "))

# --- Amount column ---
amount_candidates <- c("amount","flow_usd","remittance_usd","value","remittances",
                       "flow_value","usd","remittances_usd","remittance_amount")
amount_col <- intersect(amount_candidates, names(df))[1]
if (is.na(amount_col)) stop("Amount column not found. Columns: ", paste(names(df), collapse=", "))

# --- Period column ---
# If a 'quarter' column exists but only holds 1-4, we need year+quarter combined.
# Prefer columns that already encode the full year-quarter string.
full_period_candidates <- c("period","year_quarter","year_q","date","time",
                             "year_qtr","yearquarter","year_quarter_str")
period_col <- intersect(full_period_candidates, names(df))[1]

if (is.na(period_col)) {
  # Fall back: build from year + quarter integer columns
  if (all(c("year","quarter") %in% names(df))) {
    df <- df %>% mutate(period = paste0(year, "q", quarter))
    period_col <- "period"
    cat("Built period column from year + quarter.\n")
  } else {
    stop("No period/quarter column found. Columns: ", paste(names(df), collapse=", "))
  }
}

cat("Using columns — state:", state_col, "| amount:", amount_col, "| period:", period_col, "\n")

# --- Parse period strings to Date ---
# FIX: return as.Date(NA) instead of plain NA so rowwise mutate gets a Date vector
parse_period <- function(s) {
  if (is.na(s)) return(as.Date(NA))
  if (grepl("^\\d{4}q[1-4]$", s, perl = TRUE)) {
    y <- as.integer(substr(s, 1, 4))
    q <- as.integer(substr(s, 6, 6))
    return(as.Date(sprintf("%d-%02d-01", y, (q - 1) * 3 + 1)))
  }
  if (grepl("^\\d{4}[- ]?[Qq][1-4]$", s, perl = TRUE)) {
    y <- as.integer(substr(s, 1, 4))
    q <- as.integer(substr(s, nchar(s), nchar(s)))
    return(as.Date(sprintf("%d-%02d-01", y, (q - 1) * 3 + 1)))
  }
  if (grepl("^\\d{4}-\\d{2}-\\d{2}$", s, perl = TRUE)) return(as.Date(s))
  return(as.Date(NA))
}

df <- df %>%
  mutate(period_str = as.character(.data[[period_col]])) %>%
  mutate(period_date = as.Date(sapply(period_str, parse_period),
                               origin = "1970-01-01"))
# ^^^ sapply returns a numeric vector; wrap with as.Date(..., origin=) to restore class

cat("Period date sample:", format(head(df$period_date, 5)), "\n")
if (all(is.na(df$period_date))) {
  stop("Could not parse period column. Example values: ",
       paste(head(df$period_str, 10), collapse=", "))
}

# --- Rename & cast ---
df <- df %>%
  rename(state  = all_of(state_col),
         amount = all_of(amount_col)) %>%
  mutate(state  = as.character(state),
         amount = as.numeric(amount))

# --- Aggregate ---
totals <- df %>%
  group_by(state, period_date) %>%
  summarise(total = sum(amount, na.rm = TRUE), .groups = "drop") %>%
  arrange(state, period_date) %>%
  group_by(state) %>%
  mutate(pct_change = 100 * (total / lag(total) - 1)) %>%
  ungroup()

# --- Plot per state ---
out_dir <- file.path(dirname(csv_path), "..", "plots", "per_state")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

states <- sort(unique(totals$state))

for (s in states) {
  dat <- totals %>% filter(state == s)
  if (nrow(dat) == 0) next
  safe_name <- str_replace_all(s, "[^A-Za-z0-9_\\-]", "_")

  p1 <- ggplot(dat, aes(x = period_date, y = total)) +
    geom_line() + geom_point() +
    labs(title = paste("Total remittances -", s), x = "Quarter", y = "USD") +
    theme_minimal()

  p2 <- ggplot(dat, aes(x = period_date, y = pct_change)) +
    geom_hline(yintercept = 0, colour = "gray70") +
    geom_line() + geom_point() +
    labs(title = paste("Pct change remittances -", s), x = "Quarter", y = "% change") +
    theme_minimal()

  ggsave(file.path(out_dir, paste0(safe_name, "_total.png")),
         plot = p1, width = 9, height = 4)
  ggsave(file.path(out_dir, paste0(safe_name, "_pct_change.png")),
         plot = p2, width = 9, height = 4)
}

cat("Saved", length(states), "state plots into", out_dir, "\n")