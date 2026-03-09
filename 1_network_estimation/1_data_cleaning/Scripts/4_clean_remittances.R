# CLEAN BANXICO REMITTANCE FILES (DS1 + DS2)
# Outputs long-format parquets to 2_SQL_database/Data_clean_updated/Remittances/
# Can be run from any working directory (resolves THESIS_DIR from script location).

library(dplyr)
library(tidyr)
library(stringr)
library(readxl)
library(arrow)

# ── Resolve THESIS_DIR from this script's location ────────────────────────────
# Script lives at: THESIS_DIR/1_network_estimation/1_data_cleaning/Utility_scripts/
# Works for both source() in RStudio and Rscript from the command line.

get_script_dir <- function() {
  # source() path
  src <- tryCatch(normalizePath(sys.frame(1)$ofile, winslash = "/"),
                  error = function(e) NULL)
  if (!is.null(src)) return(dirname(src))
  # Rscript --file= path
  args  <- commandArgs(trailingOnly = FALSE)
  farg  <- grep("--file=", args, value = TRUE)
  if (length(farg))
    return(dirname(normalizePath(sub("--file=", "", farg[1]), winslash = "/")))
  stop("Cannot resolve script location. Set THESIS_DIR manually or run from Thesis/.")
}

thesis_dir <- normalizePath(file.path(get_script_dir(), "..", "..", ".."), winslash = "/")
message("THESIS_DIR: ", thesis_dir)

# ── Paths ─────────────────────────────────────────────────────────────────────

ds2_path <- file.path(thesis_dir, "Data", "Remittances",
                      "Estado de origen de los ingresos por remesas provenientes de Estados Unidos.xlsx")
ds1_path <- file.path(thesis_dir, "Data", "Remittances",
                      "Ingresos por remesas, distribución por municipio.xlsx")
out_dir  <- file.path(thesis_dir, "2_SQL_database", "Data_clean_updated", "Remittances")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# Banxico file layout (same for both files)
HEADER_ROW <- 10   # 1-indexed: row with column name strings
DATA_START <- 19   # 1-indexed: first row of date + values

# ── Spanish → English state names ────────────────────────────────────────────
# Only the states that Banxico translates; all others are identical.

ES_TO_EN <- c(
  "Luisiana"          = "Louisiana",
  "Misuri"            = "Missouri",
  "Mississipi"        = "Mississippi",
  "Nueva York"        = "New York",
  "Nueva Jersey"      = "New Jersey",
  "Nuevo Mexico"      = "New Mexico",
  "Nuevo Hampshire"   = "New Hampshire",
  "Carolina Del Norte" = "North Carolina",
  "Carolina Del Sur"   = "South Carolina",
  "Dakota Del Norte"   = "North Dakota",
  "Dakota Del Sur"     = "South Dakota",
  "Pensilvania"       = "Pennsylvania",
  "Washington, D.C."  = "Washington DC"
)

translate_state <- function(x) {
  ifelse(x %in% names(ES_TO_EN), ES_TO_EN[x], x)
}

# ── Name normalisation (matches normalise_munimx in MXMUNI scripts) ──────────
# chartr/iconv → lowercase → strip non-[a-z0-9 ] → collapse spaces

normalise_name <- function(x) {
  x <- iconv(x, from = "UTF-8", to = "ASCII//TRANSLIT")
  x <- tolower(trimws(x))
  x <- gsub("[^a-z0-9 ]", "", x)
  x <- trimws(gsub("\\s+", " ", x))
  x
}

# ── Shared reader ─────────────────────────────────────────────────────────────

read_banxico_raw <- function(path) {
  raw <- tryCatch(
    suppressMessages(read_xlsx(path, col_names = FALSE)),
    error = function(e) { stop("Cannot read ", basename(path), ": ", e$message) }
  )
  raw
}

# ── DS2: US state quarterly outflows ──────────────────────────────────────────

clean_ds2 <- function() {
  message("Cleaning DS2 (US state outflows)...")
  raw <- read_banxico_raw(ds2_path)
  
  # Column names: row 10, columns 2+
  prefix_ds2 <- "Estado de origen de los ingresos por remesas provenientes de Estados Unidos, "
  col_names_es <- str_remove(as.character(unlist(raw[HEADER_ROW, -1])), fixed(prefix_ds2))
  col_names_en <- translate_state(col_names_es)
  
  # Dates: column 1, rows DATA_START+
  dates <- as.Date(as.numeric(unlist(raw[DATA_START:nrow(raw), 1])),
                   origin = "1899-12-30")
  
  # Values: rows DATA_START+, columns 2+
  vals <- raw[DATA_START:nrow(raw), -1]
  vals <- as.data.frame(lapply(vals, as.numeric))
  colnames(vals) <- col_names_en
  
  ds2 <- bind_cols(data.frame(date = dates), vals) %>%
    pivot_longer(-date, names_to = "us_state", values_to = "remit_musd") %>%
    mutate(
      year    = as.integer(format(date, "%Y")),
      quarter = as.integer(ceiling(as.integer(format(date, "%m")) / 3))
    ) %>%
    select(date, year, quarter, us_state, remit_musd) %>%
    arrange(us_state, date)
  
  n_states  <- n_distinct(ds2$us_state)
  n_periods <- n_distinct(ds2$date)
  message(sprintf("  DS2: %d state columns x %d quarters -> %s rows",
                  n_states, n_periods, format(nrow(ds2), big.mark = ",")))
  ds2
}

# ── DS1: MX municipality quarterly inflows ────────────────────────────────────

clean_ds1 <- function() {
  message("Cleaning DS1 (MX municipality inflows)...")
  raw <- read_banxico_raw(ds1_path)
  
  prefix_state <- "Ingresos por Remesas Familiares, "
  prefix_muni  <- "Ingresos por Remesas, Distribución por Municipio, "
  
  col_labels <- as.character(unlist(raw[HEADER_ROW, -1]))
  
  is_state_total <- str_starts(col_labels, fixed(prefix_state))
  is_muni        <- str_starts(col_labels, fixed(prefix_muni))
  
  dates <- as.Date(as.numeric(unlist(raw[DATA_START:nrow(raw), 1])),
                   origin = "1899-12-30")
  
  # ── State totals ─────────────────────────────────────────────────────────
  state_labels <- str_remove(col_labels[is_state_total], fixed(prefix_state))
  state_vals   <- raw[DATA_START:nrow(raw), c(FALSE, is_state_total)]
  state_vals   <- as.data.frame(lapply(state_vals, as.numeric))
  colnames(state_vals) <- state_labels
  
  remit_mx_states <- bind_cols(data.frame(date = dates), state_vals) %>%
    pivot_longer(-date, names_to = "mx_state", values_to = "remit_musd") %>%
    mutate(
      year          = as.integer(format(date, "%Y")),
      quarter       = as.integer(ceiling(as.integer(format(date, "%m")) / 3)),
      mx_state_norm = normalise_name(mx_state)
    ) %>%
    select(date, year, quarter, mx_state, mx_state_norm, remit_musd) %>%
    arrange(mx_state, date)
  
  # ── Municipality level ────────────────────────────────────────────────────
  muni_labels <- str_remove(col_labels[is_muni], fixed(prefix_muni))
  
  # Parse "STATE, MUNICIPALITY" — split on first ", " only
  parsed <- str_split_fixed(muni_labels, ", ", n = 2)
  muni_state_col <- parsed[, 1]
  muni_muni_col  <- parsed[, 2]
  
  muni_vals <- raw[DATA_START:nrow(raw), c(FALSE, is_muni)]
  muni_vals <- as.data.frame(lapply(muni_vals, as.numeric))
  
  # Label columns with a compound key, then melt
  colnames(muni_vals) <- paste0(muni_state_col, "|||", muni_muni_col)
  
  remit_mx_munis <- bind_cols(data.frame(date = dates), muni_vals) %>%
    pivot_longer(-date, names_to = "key", values_to = "remit_musd") %>%
    separate(key, into = c("mx_state", "mx_muni"), sep = "\\|\\|\\|") %>%
    mutate(
      year          = as.integer(format(date, "%Y")),
      quarter       = as.integer(ceiling(as.integer(format(date, "%m")) / 3)),
      mx_state_norm = normalise_name(mx_state),
      mx_muni_norm  = normalise_name(mx_muni)
    ) %>%
    select(date, year, quarter, mx_state, mx_muni, mx_state_norm, mx_muni_norm, remit_musd) %>%
    arrange(mx_state, mx_muni, date)
  
  n_munis   <- n_distinct(paste(remit_mx_munis$mx_state_norm, remit_mx_munis$mx_muni_norm))
  n_periods <- n_distinct(remit_mx_munis$date)
  message(sprintf("  DS1 munis:  %d municipalities x %d quarters -> %s rows",
                  n_munis, n_periods, format(nrow(remit_mx_munis), big.mark = ",")))
  message(sprintf("  DS1 states: %d MX states x %d quarters -> %s rows",
                  n_distinct(remit_mx_states$mx_state), n_periods,
                  format(nrow(remit_mx_states), big.mark = ",")))
  
  list(munis = remit_mx_munis, states = remit_mx_states)
}

# ── Main ──────────────────────────────────────────────────────────────────────

ds2 <- clean_ds2()
write_parquet(ds2, file.path(out_dir, "remit_us_states.parquet"))
message("  -> saved remit_us_states.parquet\n")

ds1 <- clean_ds1()
write_parquet(ds1$munis,  file.path(out_dir, "remit_mx_munis.parquet"))
write_parquet(ds1$states, file.path(out_dir, "remit_mx_states.parquet"))
message("  -> saved remit_mx_munis.parquet")
message("  -> saved remit_mx_states.parquet\n")

# Sanity: DS1 state-sum vs DS2 Total for one quarter
check_ds1 <- ds1$states %>%
  filter(year == 2019, quarter == 1) %>%
  summarise(total = sum(remit_musd, na.rm = TRUE)) %>%
  pull(total)
check_ds2 <- ds2 %>%
  filter(year == 2019, quarter == 1, us_state == "Total") %>%
  pull(remit_musd)
message(sprintf("Sanity 2019Q1: DS1 state-sum = %.1f  |  DS2 Total = %.1f", check_ds1, check_ds2))
message("(DS1 < DS2 expected: some remittances go to unmatched/small municipalities)")
message("\nDone.")
