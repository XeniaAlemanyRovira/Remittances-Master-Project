# FORMAT ALL XLSX FILES

library(tidyr)
library(readxl)
library(stringr)
library(writexl)
library(dplyr)
setwd("C:/Users/T14/7Programming/Python/Thesis")
estados_dir <- file.path("1_network_estimation", "1_data_cleaning", "Data_clean", "MCAS", "Estados_US")    # input (from _1/_2)
out_dir     <- file.path("2_SQL_database", "Data_clean_updated", "MCAS", "Estados_US")  # final output

# Rename columns
STANDARD_COLS <- c("mx_state", "mx_municipality", "n_matriculas", "pct_matriculas")

header_phrases <- c(
  "estado de origen",
  "municipio de origen",
  "n.mero de matr.culas",
  "numero de matriculas",
  "porcentaje de matr.culas",
  "porcentaje de matriculas"
)

# ─────────────────────────────────────────────────────────────────────────────
# normalise_mx_state()
#
# Applies in this order:
#   1. iconv ASCII//TRANSLIT    strips accents uniformly regardless of file
#                               encoding, so "Michoac\u00e1n" and "Michoácan" both become "Michoacan"
#   2. str_to_title / trimws    already done upstream, but safe to repeat
#   3. str_remove " Total$"     drops "Total" aggregate rows
#   4. recode                   maps all historical/formal-name variants to a single
#                               ASCII name per state, fixing the year-split duplicates

# ─────────────────────────────────────────────────────────────────────────────
normalise_mx_state <- function(x) {
  x <- iconv(trimws(as.character(x)), to = "ASCII//TRANSLIT")
  x <- str_to_title(x)
  x <- str_remove(x, "\\s+Total$")
  dplyr::recode(x,
                # CDMX: IME switched name ~2015-2016; use the current official name
                "Distrito Federal"                = "Ciudad De Mexico",
                # Estado de Mexico: appeared as short "Mexico" pre-2014
                "Mexico"                          = "Estado De Mexico",
                # Michoacan: formal name "De Ocampo" used in some years
                "Michoacan De Ocampo"             = "Michoacan",
                # Queretaro: formal name "De Arteaga" used pre-2014
                "Queretaro De Arteaga"            = "Queretaro",
                # Veracruz: full constitutional name used in some files
                "Veracruz De Ignacio De La Llave" = "Veracruz",
                # Coahuila: formal name "De Zaragoza" used in some files
                "Coahuila De Zaragoza"            = "Coahuila",
                # Typo in one 2020 file
                "Guerero"                         = "Guerrero"
                # Nuevo Leon, San Luis Potosi, Yucatan, Queretaro, etc. are already
                # handled by iconv stripping the accent, so no explicit recode needed.
  )
}

# ─────────────────────────────────────────────────────────────────────────────
# normalise_mx_muni()
#
# Strips accents via ASCII//TRANSLIT so that "Cosío" and "Cosio", or
# "Tuxtla Gutiérrez" and "Tuxtla Gutierrez", collapse to the same key.
# Applied to mx_municipality in every fix function.
# ─────────────────────────────────────────────────────────────────────────────
normalise_mx_muni <- function(x) {
  x <- iconv(trimws(as.character(x)), to = "ASCII//TRANSLIT")
  str_to_title(x)
}

# ─────────────────────────────────────────────────────────────────────────────
# is_junk_muni()
#
# Returns TRUE for municipality values that are not real place names:
#   - "Total"                   aggregate row that should have been dropped as a footer
#   - "Desconocido"             MCAS "unknown origin" category; not a municipality
#   - "Masculino" / "Femenino"  gender breakdown rows from a different file format
#   - "No Se Registro…"         MCAS "municipality not recorded" label
# ─────────────────────────────────────────────────────────────────────────────
JUNK_MUNI_PATTERN <- paste(
  "^Total$",
  "^Desconocido$",
  "^Masculino$",
  "^Femenino$",
  "^No Se Registr",   # covers full multi-word label regardless of accent
  sep = "|"
)

is_junk_muni <- function(x) grepl(JUNK_MUNI_PATTERN, trimws(x), ignore.case = TRUE)

# ─────────────────────────────────────────────────────────────────────────────
# is_junk_state()
#
# Returns TRUE for rows whose mx_state is not a real Mexican state — i.e.
# education-level metadata rows that leaked from 2013 files with an extra
# summary section (1o. Primaria, Posgrado, Profesional Titulado, etc.).
# ─────────────────────────────────────────────────────────────────────────────
JUNK_STATE_PATTERN <- paste(
  "^[0-9]",             # "1o. Primaria", "3er. Ano De Profesional", etc.
  "Posgrado",
  "Preparatoria",
  "Profesional",
  "^Primaria",
  "Secundaria",
  "Capacitacion",
  "Menores De Edad",
  "Sin Estudios",
  "^Total$",
  sep = "|"
)

is_junk_state <- function(x) grepl(JUNK_STATE_PATTERN, x, ignore.case = TRUE)

# ─────────────────────────────────────────────────────────────────────────────

fix_file <- function(path) {
  raw <- tryCatch(
    read_xlsx(path, col_names = FALSE),
    error = function(e) { message("  [ERROR] ", basename(path), " — ", e$message); NULL }
  )
  if (is.null(raw)) return(invisible(NULL))
  
  # Check if row 1 is already the standard header
  first_row <- tolower(trimws(as.character(unlist(raw[1, ]))))
  first_row <- first_row[!is.na(first_row) & first_row != "na" & first_row != ""]
  already_standard <- all(STANDARD_COLS %in% first_row)
  
  if (already_standard) {
    # Header is already correct — re-read with col_names so row 1 becomes the header
    df <- tryCatch(
      read_xlsx(path, col_names = TRUE),
      error = function(e) { message("  [ERROR] ", basename(path), " — ", e$message); NULL }
    )
    if (is.null(df)) return(invisible(NULL))
    df <- df[, STANDARD_COLS, drop = FALSE]
  } else {
    # Find the real header row (scanning first 15 rows)
    header_row <- NA_integer_
    for (i in seq_len(min(nrow(raw), 15))) {
      row_vals <- tolower(trimws(as.character(unlist(raw[i, ]))))
      row_vals  <- row_vals[!is.na(row_vals) & row_vals != "na" & row_vals != ""]
      if (length(row_vals) == 0) next
      if (any(sapply(header_phrases, function(ph) any(grepl(ph, row_vals))))) {
        header_row <- i
        break
      }
    }
    
    if (is.na(header_row)) {
      message("  [WARN - header not found] ", basename(path))
      return(invisible(NULL))
    }
    if (header_row >= nrow(raw)) {
      message("  [WARN - no data after header] ", basename(path))
      return(invisible(NULL))
    }
    
    df <- raw[(header_row + 1):nrow(raw), ]
    n_cols <- ncol(df)
    
    if (n_cols >= 4) {
      colnames(df)[1:4] <- STANDARD_COLS
      if (n_cols > 4) {
        df <- df[, 1:4]
        message("  [INFO - dropped ", n_cols - 4, " extra col(s)] ", basename(path))
      }
    } else {
      message("  [WARN - only ", n_cols, " columns, expected 4] ", basename(path))
      return(invisible(NULL))
    }
  }
  
  # Apply normalization
  
  df <- df %>%
    mutate(
      n_matriculas    = suppressWarnings(as.numeric(n_matriculas)),
      pct_matriculas  = suppressWarnings(as.numeric(pct_matriculas)),
      mx_state        = str_to_title(trimws(as.character(mx_state))),
      mx_municipality = normalise_mx_muni(mx_municipality)
    ) %>%
    fill(mx_state, .direction = "down") %>%
    mutate(mx_state = normalise_mx_state(mx_state)) %>%
    filter(!is_junk_state(mx_state), !is_junk_muni(mx_municipality))
  
  valid_rows <- which(!is.na(df$n_matriculas))
  if (length(valid_rows) == 0) {
    message("  [WARN - no numeric data found] ", basename(path))
    return(invisible(NULL))
  }
  df <- df[1:max(valid_rows), , drop = FALSE]
  df <- df[rowSums(!is.na(df)) > 0, , drop = FALSE]
  
  out_path <- file.path(out_dir, basename(dirname(path)), basename(path))
  dir.create(dirname(out_path), showWarnings = FALSE, recursive = TRUE)
  write_xlsx(df, out_path)
  message("  [DONE] ", basename(path))
}

# Run on all xlsx files
all_files <- list.files(estados_dir,
                        pattern    = "^[A-Z_]+_[0-9]{4}\\.xlsx$",
                        recursive  = TRUE,
                        full.names = TRUE)

cat(sprintf("Found %d files to check.\n\n", length(all_files)))
for (f in sort(all_files)) fix_file(f)

# Fix MASSACHUSETTS_2014 and GEORGIA_2019
fix_extra_column_file <- function(path) {
  if (!file.exists(path)) {
    message("  [MISSING] ", basename(path))
    return(invisible(NULL))
  }
  
  raw <- tryCatch(
    read_xlsx(path, col_names = FALSE),
    error = function(e) { message("  [ERROR] ", basename(path), " — ", e$message); NULL }
  )
  if (is.null(raw)) return(invisible(NULL))
  
  # Find header row
  header_row <- NA_integer_
  for (i in seq_len(min(nrow(raw), 15))) {
    row_vals <- tolower(trimws(as.character(unlist(raw[i, ]))))
    row_vals  <- row_vals[!is.na(row_vals) & row_vals != "na" & row_vals != ""]
    if (any(grepl("estado", row_vals))) { header_row <- i; break }
  }
  
  if (is.na(header_row)) {
    message("  [WARN - header not found] ", basename(path))
    return(invisible(NULL))
  }
  
  # Find which column contains "estado" — skips any leading junk column
  header_vals <- tolower(trimws(as.character(unlist(raw[header_row, ]))))
  state_col   <- which(grepl("estado", header_vals))[1]
  
  # Extract all rows after the header, using the 4 real data columns
  df <- raw[(header_row + 1):nrow(raw), state_col:(state_col + 3)]
  colnames(df) <- STANDARD_COLS
  
  # Drop any residual header rows (e.g. "Estado De Origen" repeated as data)
  # by finding the first row where the 3rd column (n_matriculas) is numeric
  first_data_row <- which(!is.na(suppressWarnings(as.numeric(df$n_matriculas))))[1]
  if (is.na(first_data_row)) {
    message("  [WARN - no numeric data found] ", basename(path))
    return(invisible(NULL))
  }
  df <- df[first_data_row:nrow(df), , drop = FALSE]
  
  df <- df %>%
    mutate(
      n_matriculas    = suppressWarnings(as.numeric(n_matriculas)),
      pct_matriculas  = suppressWarnings(as.numeric(pct_matriculas)),
      mx_state        = str_to_title(trimws(as.character(mx_state))),
      mx_municipality = normalise_mx_muni(mx_municipality)
    ) %>%
    fill(mx_state, .direction = "down") %>%
    mutate(mx_state = normalise_mx_state(mx_state)) %>%
    filter(!is_junk_state(mx_state), !is_junk_muni(mx_municipality))
  
  # Truncate at last valid numeric row (drops footer)
  valid_rows <- which(!is.na(df$n_matriculas))
  df <- df[1:max(valid_rows), , drop = FALSE]
  df <- df[rowSums(!is.na(df)) > 0, , drop = FALSE]
  
  out_path <- file.path(out_dir, basename(dirname(path)), basename(path))
  dir.create(dirname(out_path), showWarnings = FALSE, recursive = TRUE)
  write_xlsx(df, out_path)
  message("  [FIXED - extra column] ", basename(path))
}

cat("Applying special fixes...\n\n")
fix_extra_column_file(
  file.path(estados_dir, "Edos_USA_2014", "MASSACHUSETTS_2014.xlsx")
)
fix_extra_column_file(
  file.path(estados_dir, "Edos_USA_2019", "GEORGIA_2019.xlsx")
)
cat("\nAll done!\n")
