# Overwrites Data_clean
unlink("Data_clean", recursive = TRUE)

# Config
years       <- 2010:2024
source_base <- "Data/MCAS/Estados_US"
dest_base   <- "Data_clean/MCAS/Estados_US"

dir.create(dest_base, showWarnings = FALSE, recursive = TRUE)

# Correcting typos and standardizing names (2010–2017) 
normalise_state <- function(name) {
  # This part corrects typos
  name <- tolower(name)
  name <- gsub("[0-9]", "", name)
  name <- gsub("compact", "", name)
  name <- gsub("_", " ", name)
  name <- trimws(gsub("\\s+", " ", name))
  name <- gsub("f$", "", name)
  name <- trimws(name)
  # This part standardizes names
  name <- gsub("^illinoise$",             "illinois",             name)
  name <- gsub("^delawaree$",             "delaware",             name)
  name <- gsub("^newhamphire$",           "new hampshire",        name)
  name <- gsub("^new hamphire$",          "new hampshire",        name)
  name <- gsub("^tenneesee$",             "tennessee",            name)
  name <- gsub("^noth carolina$",         "north carolina",       name)
  name <- gsub("^kansas city$",           "kansas",               name)
  name <- gsub("^distric of columbia$",   "district of columbia", name)
  name <- gsub("^districtofcolumbia$",    "district of columbia", name)
  name <- gsub("^district of columbia.*", "district of columbia", name)
  name <- gsub("^district_of_columbia.*", "district of columbia", name)
  name <- gsub("^pennylvania$",           "pennsylvania",         name)
  name <- gsub("^indianapolis$",          "indiana",              name)
  name <- gsub("^newhampshire$",          "new hampshire",        name)
  name <- gsub("^newjersey$",             "new jersey",           name)
  name <- gsub("^newmexico$",             "new mexico",           name)
  name <- gsub("^newyork$",               "new york",             name)
  name <- gsub("^northcarolina$",         "north carolina",       name)
  name <- gsub("^northdakota$",           "north dakota",         name)
  name <- gsub("^rhodeisland$",           "rhode island",         name)
  name <- gsub("^southcarolina$",         "south carolina",       name)
  name <- gsub("^southdakota$",           "south dakota",         name)
  name <- gsub("^westvirginia$",          "west virginia",        name)
  name <- gsub("^districtofcolumbia$",    "district of columbia", name)
  
  trimws(name)
}

# Name standardization (2019-2024)
normalise_filename <- function(fname, year) {
  ext  <- tools::file_ext(fname)
  base <- tools::file_path_sans_ext(fname)
  
  # Fixing typos
  base <- gsub("New M[\u00e9e]?xico", "New Mexico", base)
  base <- gsub("^New Jersey 2019$", paste0("New Jersey ", year), base)
  base <- gsub("^Utah 2022$", paste0("Utah ", year), base)
  base <- gsub("\\.-$", "", base)
  base <- gsub("-$",    "", base)
  
  # Strip trailing dots or spaces
  base <- trimws(base)
  
  paste0(base, ".", ext)
}

# Loop
for (year in years) {
  folder_name <- paste0("Edos_USA_", year)
  outer_dir   <- file.path(source_base, folder_name)
  inner_dir   <- file.path(outer_dir, folder_name)
  
  if (dir.exists(inner_dir)) {
    copy_from <- inner_dir
    message("✓ ", year, ": double nesting detected, copying from inner folder")
  } else if (dir.exists(outer_dir)) {
    copy_from <- outer_dir
    message("✓ ", year, ": no double nesting, copying directly")
  } else {
    warning("✗ Folder not found for year: ", year, " — skipping")
    next
  }
  
  dest_dir <- file.path(dest_base, folder_name)
  dir.create(dest_dir, showWarnings = FALSE)
  
  contents <- list.files(copy_from, full.names = TRUE)
  for (item in contents) {
    raw_name <- basename(item)
    
    if (file.info(item)$isdir) {
      # Normalise folder names only for 2010–2017
      clean_name <- if (year <= 2017) normalise_state(raw_name) else raw_name
      dest_item  <- file.path(dest_dir, clean_name)
      dir.create(dest_item, showWarnings = FALSE)
      sub_files <- list.files(item, full.names = TRUE)
      for (sf in sub_files) {
        file.copy(sf, dest_item, recursive = TRUE)
      }
    } else {
      # Normalise file names for 2019–2024
      clean_name <- if (year >= 2019) normalise_filename(raw_name, year) else raw_name
      dest_item  <- file.path(dest_dir, clean_name)
      file.copy(item, dest_item)
    }
    
    if (raw_name != clean_name) message("  ", raw_name, " → ", clean_name)
  }
}

message("Done! Clean data is in: ", dest_base)