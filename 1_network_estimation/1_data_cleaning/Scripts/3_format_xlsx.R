# FORMAT ALL XLSX FILES

library(tidyr)
library(readxl)
library(stringr)
library(writexl)
library(dplyr)

estados_dir <- file.path("Data_clean", "MCAS", "Estados_US")

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

fix_file <- function(path) {
  raw <- tryCatch(
    read_xlsx(path, col_names = FALSE),
    error = function(e) { message("  [ERROR] ", basename(path), " — ", e$message); NULL }
  )
  if (is.null(raw)) return(invisible(NULL))
  
  # Check if row 1 is already the standard header
  first_row <- tolower(trimws(as.character(unlist(raw[1, ]))))
  first_row <- first_row[!is.na(first_row) & first_row != "na" & first_row != ""]
  if (all(STANDARD_COLS %in% first_row)) {
    message("  [OK - already standard] ", basename(path))
    return(invisible(NULL))
  }
  
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
  
  # Extract data rows and assign standard column names by position
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
  
  # Coerce numeric columns to handle text-stored numbers
  df <- df %>%
    mutate(
      n_matriculas   = suppressWarnings(as.numeric(n_matriculas)),
      pct_matriculas = suppressWarnings(as.numeric(pct_matriculas)),
      mx_state        = str_to_title(trimws(mx_state)),
      mx_municipality = str_to_title(trimws(mx_municipality))
    )
  
  # Fill mx_state downward
  df <- df %>% fill(mx_state, .direction = "down")
  
  # Drop footers by finding the last row in which there is a number in the 
  # n_matriculas column
  valid_rows <- which(!is.na(df$n_matriculas))
  if (length(valid_rows) == 0) {
    message("  [WARN - no numeric data found] ", basename(path))
    return(invisible(NULL))
  }
  df <- df[1:max(valid_rows), , drop = FALSE]
  
  # Drop completely empty rows
  df <- df[rowSums(!is.na(df)) > 0, , drop = FALSE]
  
  write_xlsx(df, path)
  message("  [FIXED] ", basename(path), "  (header was row ", header_row, ")")
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
      mx_municipality = str_to_title(trimws(as.character(mx_municipality)))
    ) %>%
    mutate(mx_state = str_remove(mx_state, "\\s+Total$")) %>%
    fill(mx_state, .direction = "down")
  
  # Truncate at last valid numeric row (drops footer)
  valid_rows <- which(!is.na(df$n_matriculas))
  df <- df[1:max(valid_rows), , drop = FALSE]
  df <- df[rowSums(!is.na(df)) > 0, , drop = FALSE]
  
  write_xlsx(df, path)
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