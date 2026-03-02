# Standardize all Estado_US Excel files to STATE_YEAR.xlsx format

library(tools)
library(readxl)
library(writexl)

# Config
estados_dir <- file.path("Data_clean", "MCAS", "Estados_US")

states <- c("Alabama", "Alaska", "Arizona", "Arkansas", "California",
            "Colorado", "Connecticut", "Delaware", "District of Columbia",
            "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana",
            "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
            "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
            "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
            "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
            "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina",
            "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
            "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming")

state_lookup <- setNames(states, tolower(gsub(" ", "", states)))

typos <- c(
  "pennylvania"        = "Pennsylvania",
  "tenesse"            = "Tennessee",
  "newhamphire"        = "New Hampshire",
  "illinoise"          = "Illinois",
  "districofcolumbia"  = "District of Columbia",
  "districtofcolumbia" = "District of Columbia",
  "indianapolis"       = "Indiana",
  "delawaree"          = "Delaware",
  "tenneesee"          = "Tennessee",
  "pennsylvnia"        = "Pennsylvania",
  "pensylvannia"       = "Pennsylvania",
  "lousiana"           = "Louisiana",
  "massachussets"      = "Massachusetts",
  "iowae"              = "Iowa",
  "wisconsisn"         = "Wisconsin",
  "kansascity"         = "Kansas"
)
state_lookup <- c(state_lookup, typos)

# Helper that standardizes state names
normalize_state_name <- function(raw) {
  name <- tolower(trimws(raw))
  name <- sub("[_ ]?\\d{4}f?$", "", name)
  name <- sub("compact$", "", name)
  name <- sub("f$", "", name)
  name <- gsub("[_\\-\\.]", "", name)
  name <- trimws(name)
  if (name %in% names(state_lookup)) return(state_lookup[[name]])
  no_space <- gsub(" ", "", name)
  if (no_space %in% names(state_lookup)) return(state_lookup[[no_space]])
  for (canonical in states) {
    if (tolower(gsub(" ", "", canonical)) == no_space) return(canonical)
  }
  return(NA_character_)
}

# Helper that finds the "mun" file inside a subfolder
find_mun_file <- function(folder_path) {
  if (!dir.exists(folder_path)) return(NA_character_)
  files <- list.files(folder_path)
  candidates <- files[grepl("mun", files, ignore.case = TRUE) &
                        grepl("\\.xlsx$", files, ignore.case = TRUE) &
                        !startsWith(files, "._")]
  if (length(candidates) == 0) return(NA_character_)
  return(file.path(folder_path, candidates[1]))
}

# Helper that builds the xlsx name
make_standard_name <- function(state, year) {
  paste0(toupper(gsub(" ", "_", state)), "_", year, ".xlsx")
}

# Helper that cleans up everything except the generated xlsx files
cleanup_year_dir <- function(year_dir, kept_files) {
  all_items <- list.files(year_dir, full.names = TRUE)
  for (item in all_items) {
    if (!basename(item) %in% kept_files) {
      if (dir.exists(item)) {
        unlink(item, recursive = TRUE)
        message(sprintf("  [DEL folder] %s", basename(item)))
      } else {
        file.remove(item)
        message(sprintf("  [DEL file]   %s", basename(item)))
      }
    }
  }
}

# Helper that extracts sheets from xlsx files
copy_sheet <- function(source_path, target_path, sheet) {
  tryCatch({
    df <- read_xlsx(source_path, sheet = sheet)
    write_xlsx(df, target_path)
    if (file.exists(target_path)) file.remove(source_path)
  }, error = function(e) {
    message(sprintf("  [WARN] Could not read sheet %d from: %s — %s",
                    sheet, basename(source_path), e$message))
  })
}

# Main processing function
process_year <- function(year) {
  year_dir <- file.path(estados_dir, paste0("Edos_USA_", year))
  
  if (!dir.exists(year_dir)) {
    message(sprintf("  [SKIP] Not found: %s", year_dir))
    return(invisible(NULL))
  }
  
  kept   <- character(0)
  errors <- character(0)
  
  # For years 2010 to 2017
  if (year %in% 2010:2017) {
    for (item in sort(list.files(year_dir))) {
      item_path <- file.path(year_dir, item)
      if (!dir.exists(item_path)) next
      
      state    <- normalize_state_name(item)
      mun_file <- find_mun_file(item_path)
      
      if (is.na(state) || is.na(mun_file)) {
        message(sprintf("  [WARN] %s — state: %s | mun: %s", item,
                        ifelse(is.na(state), "not found", state),
                        ifelse(is.na(mun_file), "not found", "ok")))
        errors <- c(errors, item)
        next
      }
      
      target_name <- make_standard_name(state, year)
      copy_sheet(mun_file, file.path(year_dir, target_name), sheet = 1)
      message(sprintf("  [OK] %s -> %s", basename(mun_file), target_name))
      kept <- c(kept, target_name)
    }
    
    # For 2018
  } else if (year == 2018) {
    for (item in sort(list.files(year_dir))) {
      if (!grepl("\\.xlsx$", item, ignore.case = TRUE) || startsWith(item, "._")) next
      m <- regmatches(item, regexec("^(.+?)_info_2018\\.xlsx$", item, ignore.case = TRUE))[[1]]
      if (length(m) < 2) next
      
      state <- normalize_state_name(m[2])
      if (is.na(state)) { errors <- c(errors, item); next }
      
      target_name <- make_standard_name(state, year)
      copy_sheet(file.path(year_dir, item), file.path(year_dir, target_name), sheet = 3)
      message(sprintf("  [OK] %s -> %s", item, target_name))
      kept <- c(kept, target_name)
    }
    
    # For years 2019 to 2024
  } else if (year %in% 2019:2024) {
    for (item in sort(list.files(year_dir, pattern = "\\.xlsx$"))) {
      if (startsWith(item, "._")) next
      
      state_raw <- sub(paste0("\\s*[-\\.]*\\s*", year, "[-\\.]*\\.xlsx$"), "", item,
                       ignore.case = TRUE)
      state_raw <- trimws(gsub("\\.", "", state_raw))
      
      state <- normalize_state_name(state_raw)
      if (is.na(state)) {
        for (s in states) {
          if (tolower(trimws(state_raw)) == tolower(s)) { state <- s; break }
        }
      }
      if (is.na(state)) {
        message(sprintf("  [WARN] Could not identify state: %s", item))
        errors <- c(errors, item)
        next
      }
      
      target_name <- make_standard_name(state, year)
      copy_sheet(file.path(year_dir, item), file.path(year_dir, target_name), sheet = 3)
      message(sprintf("  [OK] %s -> %s", item, target_name))
      kept <- c(kept, target_name)
    }
  }
  
  # Cleanup
  message(sprintf("  Cleaning up %d...", year))
  cleanup_year_dir(year_dir, kept)
  message(sprintf("  Done: %d files kept, %d errors", length(kept), length(errors)))
  if (length(errors) > 0) message(sprintf("  Errors: %s", paste(errors, collapse = ", ")))
}

# Running the script
cat(strrep("=", 70), "\n")
cat("Standardizing Data_clean/MCAS/Estados_US to STATE_YEAR.xlsx\n")
cat(strrep("=", 70), "\n\n")

for (year in 2010:2024) {
  cat(sprintf("\n--- Year %d ---\n", year))
  process_year(year)
}

# Delete known bad files
cat("\n--- Post-processing: deleting known bad files ---\n")

bad_files <- list(
  list(year = 2013, name = "FLORIDA_2013.xlsx"),
  list(year = 2024, name = "CONNECTICUT_2024.xlsx")
)

for (entry in bad_files) {
  path <- file.path(estados_dir,
                    paste0("Edos_USA_", entry$year),
                    entry$name)
  if (file.exists(path)) {
    file.remove(path)
    message(sprintf("  [DEL] %s", entry$name))
  } else {
    message(sprintf("  [NOT FOUND] %s — already absent or never created", entry$name))
  }
}

cat("\n", strrep("=", 70), "\n")
cat("Done! Only STATE_YEAR.xlsx files remain in each year folder.\n")
cat(strrep("=", 70), "\n")