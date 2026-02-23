library(readxl)
library(writexl)

estados_dir <- file.path("Data_clean", "MCAS", "Estados_US")

# Identify the header row
header_phrases <- c(
  "estado de origen",
  "municipio de origen",
  "n\u00famero de matr\u00edculas",
  "numero de matriculas",
  "porcentaje de matr\u00edculas",
  "porcentaje de matriculas"
)

# Footer phrases to drop from data
footer_patterns <- c(
  "tama\u00f1o de la muestra", "tamano de la muestra",
  "fuente:", "elaborado por", "secretaria de relaciones",
  "marzo", "abril", "enero", "febrero", "instituto de los mexicanos"
)

fix_file <- function(path) {
  raw <- tryCatch(
    read_xlsx(path, col_names = FALSE),
    error = function(e) { message("  [ERROR] ", basename(path), " — ", e$message); NULL }
  )
  if (is.null(raw)) return(invisible(NULL))
  
  # Check if row 1 is a header
  first_row <- tolower(trimws(as.character(unlist(raw[1, ]))))
  first_row <- first_row[!is.na(first_row) & first_row != "na" & first_row != ""]
  if (any(sapply(header_phrases, function(ph) any(grepl(ph, first_row, fixed = TRUE))))) {
    message("  [OK - already correct] ", basename(path))
    return(invisible(NULL))
  }
  
  # Find the real header row (scanning the first 15 rows)
  header_row <- NA_integer_
  for (i in seq_len(min(nrow(raw), 15))) {
    row_vals <- tolower(trimws(as.character(unlist(raw[i, ]))))
    row_vals <- row_vals[!is.na(row_vals) & row_vals != "na" & row_vals != ""]
    if (length(row_vals) == 0) next
    if (any(sapply(header_phrases, function(ph) any(grepl(ph, row_vals, fixed = TRUE))))) {
      header_row <- i
      break
    }
  }
  
  if (is.na(header_row)) {
    message("  [WARN - header not found] ", basename(path))
    return(invisible(NULL))
  }
  
  # Build column names from the header row
  col_names <- trimws(as.character(unlist(raw[header_row, ])))
  col_names[is.na(col_names) | col_names == "NA" | col_names == ""] <-
    paste0("col_", which(is.na(col_names) | col_names == "NA" | col_names == ""))
  
  # Extract only data rows (everything after the header)
  if (header_row >= nrow(raw)) {
    message("  [WARN - no data after header] ", basename(path))
    return(invisible(NULL))
  }
  df <- raw[(header_row + 1):nrow(raw), ]
  colnames(df) <- col_names
  
  # Drop empty rows and footer rows
  keep <- apply(df, 1, function(r) {
    vals <- tolower(trimws(as.character(r)))
    vals <- vals[!is.na(vals) & vals != "na" & vals != ""]
    if (length(vals) == 0) return(FALSE)
    if (any(sapply(footer_patterns, function(p) any(grepl(p, vals, fixed = TRUE))))) return(FALSE)
    TRUE
  })
  df <- df[keep, , drop = FALSE]
  
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
cat("\nDone!\n")