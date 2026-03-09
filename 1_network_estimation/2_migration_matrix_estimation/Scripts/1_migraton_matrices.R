# This script reads the single state xlsx files and spits out cross-sectional
# MX municipality to US State migration matrices for each year

rm(list = ls())

library(tidyverse)
library(readxl)
library(writexl)

# Config
base_dir <- "2_SQL_database/Data_clean_updated/MCAS/Estados_US"
output_dir  <- "1_network_estimation/2_migration_matrix_estimation/yearly_migration_matrices_2"
years    <- 2010:2024

# Lookup table of verified typos and duplicates to correct
# All names are post-normalize_text (title case, no accents)
name_corrections <- tribble(
  ~mx_state,              ~wrong,                                        ~correct,
  "Chiapas",              "Montecristo De Guerero",                      "Montecristo De Guerrero",
  "Chihuahua",            "Praxedis G. Guerero",                         "Praxedis G. Guerrero",
  "Chihuahua",            "Temosachi",                                   "Temosachic",
  "Chihuahua",            "Guerero",                                     "Guerrero",
  "Coahuila",             "Guerero",                                     "Guerrero",
  "Coahuila De Zaragoza", "Cuatro Cienegas",                             "Cuatrocienegas",
  "Durango",              "Gral. Simon Boivar",                          "Gral. Simon Bolivar",
  "Durango",              "Vicente Guerero",                             "Vicente Guerrero",
  "Durango",              "General Simon Bolivar",                       "Gral. Simon Bolivar",
  "Estado De Mexico",     "San Simon De Guerero",                        "San Simon De Guerrero",
  "Estado De Mexico",     "Villa Guerero",                               "Villa Guerrero",
  "Hidalgo",              "Santiago Tulantepec De Lugo Guerero",         "Santiago Tulantepec De Lugo Guerrero",
  "Hidalgo",              "Tepehuacan De Guerero",                       "Tepehuacan De Guerrero",
  "Hidalgo",              "Huehuetl",                                    "Huehuetla",
  "Hidalgo",              "Huichapa",                                    "Huichapan",
  "Jalisco",              "Villa Guerero",                               "Villa Guerrero",
  "Nuevo Leon",           "General Escobedo",                            "Gral. Escobedo",
  "Nuevo Leon",           "General Zaragoza",                            "Gral. Zaragoza",
  "Nuevo Leon",           "General Trevino",                             "Gral. Trevino",
  "Oaxaca",               "Putla Villa De Guerero",                      "Putla Villa De Guerrero",
  "Oaxaca",               "Yutanduchi De Guerero",                       "Yutanduchi De Guerrero",
  "Oaxaca",               "San Bartolome Yucane",                        "San Bartolome Yucuane",
  "Oaxaca",               "Santo Domingo Ingeni",                        "Santo Domingo Ingenio",
  "Oaxaca",               "Cuilapam De Guerero",                         "Cuilapam De Guerrero",
  "Oaxaca",               "Santa Ana Tlapacoya",                         "Santa Ana Tlapacoyan",
  "Oaxaca",               "San Pedro Totolapa",                          "San Pedro Totolapam",
  "Oaxaca",               "San Pedro Mixtepec - Distr. 22 -",            "San Pedro Mixtepec -Dto. 22 -",
  "Oaxaca",               "San Pedro Mixtepec - Distr. 26 -",            "San Pedro Mixtepec -Dto. 26 -",
  "Oaxaca",               "San Juan Mixtepec - Distr. 08 -",             "San Juan Mixtepec -Dto. 08 -",
  "Oaxaca",               "San Juan Mixtepec - Distr. 26 -",             "San Juan Mixtepec -Dto. 26 -",
  "Oaxaca",               "San Juan Mixtepec -Distrito 08-",             "San Juan Mixtepec -Dto. 08 -",
  "Oaxaca",               "San Juan Mixtepec -Distrito 26-",             "San Juan Mixtepec -Dto. 26 -",
  "Puebla",               "Totoltepec De Guerero",                       "Totoltepec De Guerrero",
  "Puebla",               "Ixcamilpa De Guerero",                        "Ixcamilpa De Guerrero",
  "Puebla",               "Ayotoxco De Guerero",                         "Ayotoxco De Guerrero",
  "Puebla",               "Vicente Guerero",                             "Vicente Guerrero",
  "Puebla",               "General Felipe _Ngeles",                      "General Felipe Angeles",
  "Sonora",               "San Pedro De La Cuev",                        "San Pedro De La Cueva",
  "Tamaulipas",           "Guerero",                                     "Guerrero",
  "Tlaxcala",             "Ziltlaltepec De Trinidad Sanchez Santos",     "Zitlaltepec De Trinidad Sanchez Santos",
  "Tlaxcala",             "Amaxac De Guerero",                           "Amaxac De Guerrero",
  "Tlaxcala",             "Yauhquemecan",                                "Yauhquemehcan",
  "Tlaxcala",             "Altzayanca",                                  "Atltzayanca",
  "Veracruz",             "Tuxpam",                                      "Tuxpan",
  "Oaxaca",               "Santo Domingo Tonaltepec",                    "Santo Domingo Tomaltepec",
  "Guanajuato",           "Alcozauca De Guerero",                        "Alcozauca De Guerrero",
  "Guerrero",             "Tixtla De Guerero",                           "Tixtla De Guerrero",
  "Hidalgo",              "San Martin Hidalgo",                          "San Martin De Hidalgo"
)

# Helper: extracts the US State name from the file path
parse_us_state <- function(path) {
  fname  <- tools::file_path_sans_ext(basename(path))
  pieces <- str_replace(fname, "_\\d{4}$", "")
  str_to_title(str_replace_all(pieces, "_", " "))
}

# Helper: takes away accents from names
normalize_text <- function(x) {
  x %>%
    str_to_title() %>%
    stringi::stri_trans_general("Latin-ASCII")
}

# Helper: applies name corrections using the lookup table
fix_names <- function(df) {
  df %>%
    left_join(name_corrections, by = c("mx_state" = "mx_state", "mx_municipality" = "wrong")) %>%
    mutate(mx_municipality = coalesce(correct, mx_municipality)) %>%
    select(-correct)
}

# Helper: reads each file based on the path and year
read_state_file <- function(path, yr) {
  read_excel(path) %>%
    select(mx_state, mx_municipality, n_matriculas) %>% # leaves out pct variables
    mutate(
      mx_state        = normalize_text(mx_state),
      mx_municipality = normalize_text(mx_municipality)
    ) %>%
    filter(
      mx_municipality != "Total", # drops subtotal rows for each MX municipality
      mx_state        != "Total", # drops total rows for each US State
      !str_detect(mx_municipality, regex("no se registro", ignore_case = TRUE))
    ) %>%
    fix_names() %>% # applies typo and duplicate corrections
    mutate(
      year         = yr,
      us_state     = parse_us_state(path),
      n_matriculas = as.integer(n_matriculas)
    )
}

# Migration panel
# This creates a panel with all of the US states and all of the years
panel <- map_dfr(years, function(yr) {
  yr_dir <- file.path(base_dir, paste0("Edos_USA_", yr))
  files  <- list.files(yr_dir, pattern = "\\.xlsx$",
                       full.names = TRUE, ignore.case = TRUE)
  if (length(files) == 0) {
    warning("No files found for year ", yr)
    return(NULL)
  }
  message("Reading year ", yr, " (", length(files), " files)...") 
  map_dfr(files, read_state_file, yr = yr)
}) %>%
  select(year, us_state, mx_state, mx_municipality, n_matriculas) %>%
  arrange(year, us_state, mx_state, mx_municipality)

# Migration matrix for each year
# Slices the panel to get a matrix for all the US States each year

# Full universe of municipalities across all years
all_municipalities <- panel %>%
  distinct(mx_state, mx_municipality)

matrices <- map(years, function(yr) {
  panel %>%
    filter(year == yr) %>%
    select(mx_state, mx_municipality, us_state, n_matriculas) %>%
    pivot_wider(
      names_from  = us_state,
      values_from = n_matriculas,
      values_fill = 0,
      values_fn   = sum
    ) %>%
    # Enforce full municipality universe, filling absent ones with 0
    right_join(all_municipalities, by = c("mx_state", "mx_municipality")) %>%
    mutate(across(where(is.numeric), ~ replace_na(., 0))) %>%
    arrange(mx_state, mx_municipality) %>%
    # Set to NA the xlsx files we are missing
    { if (yr == 2013) mutate(., Florida     = NA_real_) else . } %>%
    { if (yr == 2020) mutate(., Alaska      = NA_real_) else . } %>%
    { if (yr == 2024) mutate(., Connecticut = NA_real_) else . } %>%
    select(mx_state, mx_municipality, sort(tidyselect::peek_vars())) %>%
    # Add total column (row sums per municipality)
    mutate(Total = rowSums(across(where(is.numeric)), na.rm = TRUE)) %>%
    # Add total row (column sums per US state)
    bind_rows(
      summarise(.,
                mx_state        = "Total",
                mx_municipality = "Total",
                across(where(is.numeric), sum)
      )
    )
}) %>%
  set_names(paste0("MIGRATION_MATRIX_", years))

# Directory for the exported datasets
if (dir.exists(output_dir)) {
  unlink(output_dir, recursive = TRUE)
}
dir.create(output_dir)

# Export each matrix as a xlsx file
walk2(matrices, names(matrices), function(df, name) {
  write_xlsx(df, path = file.path(output_dir, paste0(name, ".xlsx")))
})