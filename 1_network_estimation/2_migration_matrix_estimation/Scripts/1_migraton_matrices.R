########################################################################################################## SUMMARY OF THE SCRIPT ########################### ##############################################################################

# This script:
# 1. Reads the cleaned yearly state files
# 2. Reconciles municipality-state
# 3. Pairs to the official INEGI municipality catalog
# 4. Drops rows that cannot be matched safely
# 5. Exports clean yearly migration matrices plus validation reports.

#---------------------------------------------------------------------------#

# Environment Setup: It dynamically identifies the project root so the script works on any machine without manually changing directory paths.

# Data Normalization: It creates custom functions to "clean" text (removing accents, fixing casing, expanding abbreviations like "Gral" to "General") to ensure that "Ciudad de México" and "DISTRITO FEDERAL" are treated as the same entity.

# Heuristic Matching: It uses a "scoring" system to find the best available data folder and uses tribble tables to manually fix known typos in the raw data (e.g., fixing "Guerero" to "Guerrero").

# Catalog Alignment: It reads an official INEGI (Mexico's Statistical Institute) Excel file and joins it with the migration data to ensure every municipality is valid and has a canonical name.

# Output Generation: It loops through years 2010–2024, processing state-level migration files from the US and consolidating them into clean matrices.

############################################################################
# DATA FLOW SUMMARY
# 
# INPUT PATHS:
# 1. Migration Data: Searches for yearly .xlsx files in prioritized candidates:
#    - [Root]/2_SQL_database/Data_clean_updated/MCAS/Estados_US/Edos_USA_[Year]
#    - [Root]/Data_clean_updated/MCAS/Estados_US/Edos_USA_[Year]
#    - [Root]/1_network_estimation/1_data_cleaning/Data_clean/MCAS/Estados_US/...
# 2. Official Catalog (INEGI):
#    - [Root]/Data/catun_municipio/catun_municipio/AGEEML_2026318759531.xlsx
#
# OUTPUT PATHS:
# 1. Migration Matrices: 
#    - [Root]/1_network_estimation/2_migration_matrix_estimation/yearly_migration_matrices_2/
# 2. Validation Reports: 
#    - [Root]/1_network_estimation/2_migration_matrix_estimation/validation reports/
# 3. Final Clean Data: 
#    - [Root]/1_network_estimation/2_migration_matrix_estimation/clean final data/
#
# LOGIC:
# The script reconciles non-standardized municipality names from US consular 
# records against the official INEGI catalog using fuzzy matching keys and 
# manual override tables for known spelling errors.
############################################################################

rm(list = ls())

library(tidyverse)
library(readxl)
library(writexl)
library(stringi)

get_script_path <- function() {
  cmd_args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", cmd_args, value = TRUE)

  if (length(file_arg) > 0) {
    return(normalizePath(sub("^--file=", "", file_arg[1]), winslash = "/", mustWork = TRUE))
  }

  this_file <- tryCatch(normalizePath(sys.frame(1)$ofile, winslash = "/", mustWork = TRUE),
                        error = function(...) NA_character_)

  if (!is.na(this_file)) {
    return(this_file)
  }

  normalizePath(
    file.path(getwd(), "1_network_estimation", "2_migration_matrix_estimation", "Scripts", "1_migraton_matrices.R"),
    winslash = "/",
    mustWork = FALSE
  )
}

script_path <- get_script_path()
repo_root <- normalizePath(file.path(dirname(script_path), "..", "..", ".."), winslash = "/", mustWork = TRUE)

path_in_repo <- function(...) {
  normalizePath(file.path(repo_root, ...), winslash = "/", mustWork = FALSE)
}

score_matrix_input_dir <- function(path, years) {
  if (!dir.exists(path)) {
    return(-Inf)
  }

  year_dirs <- file.path(path, paste0("Edos_USA_", years))
  existing_year_dirs <- year_dirs[dir.exists(year_dirs)]

  if (length(existing_year_dirs) == 0) {
    return(-Inf)
  }

  file_count <- purrr::map_int(existing_year_dirs, ~ length(list.files(.x, pattern = "\\.xlsx$", ignore.case = TRUE)))

  length(existing_year_dirs) * 1000 + sum(file_count)
}

resolve_matrix_input_dir <- function(candidates, years) {
  scores <- purrr::map_dbl(candidates, score_matrix_input_dir, years = years)
  best_idx <- which(scores >= 0)[1]

  if (is.na(best_idx)) {
    stop("Could not find a cleaned input directory with yearly state xlsx files.")
  }

  normalizePath(candidates[[best_idx]], winslash = "/", mustWork = TRUE)
}

resolve_existing_file <- function(candidates) {
  existing <- candidates[file.exists(candidates)]

  if (length(existing) == 0) {
    stop("Could not find the official INEGI municipality file.")
  }

  normalizePath(existing[[1]], winslash = "/", mustWork = TRUE)
}

normalize_text <- function(x) {
  out <- as.character(x)
  out[is.na(out)] <- NA_character_
  out <- stringi::stri_trans_general(out, "Latin-ASCII")
  out <- stringr::str_replace_all(out, "[[:space:]]+", " ")
  out <- stringr::str_squish(out)
  out <- stringr::str_to_title(out)
  out
}

super_clean <- function(x) {
  out <- normalize_text(x)
  out <- stringr::str_to_upper(out)
  out <- stringr::str_replace_all(out, "[^A-Z0-9 ]", " ")
  out <- stringr::str_replace_all(out, "[[:space:]]+", " ")
  out <- stringr::str_squish(out)
  dplyr::na_if(out, "")
}

canonical_municipality <- function(x) {
  out <- super_clean(x)
  out <- stringr::str_replace_all(out, "\\bGRAL\\b", "GENERAL")
  out <- stringr::str_replace_all(out, "\\bDR\\b", "DOCTOR")
  out <- stringr::str_replace_all(out, "\\bDTO\\b", "DISTRITO")
  out <- stringr::str_replace_all(out, "\\bM\\b", "MARIA")
  out <- stringr::str_replace_all(out, "[[:space:]]+", " ")
  stringr::str_squish(out)
}

normalize_state_for_matching <- function(x) {
  x_clean <- super_clean(x)

  dplyr::case_when(
    is.na(x_clean) ~ NA_character_,
    x_clean %in% c("DISTRITO FEDERAL", "CIUDAD DE MEXICO") ~ "Ciudad De Mexico",
    x_clean %in% c("MEXICO", "ESTADO DE MEXICO") ~ "Estado De Mexico",
    x_clean %in% c("MICHOACAN", "MICHOACAN DE OCAMPO") ~ "Michoacan De Ocampo",
    x_clean %in% c("VERACRUZ", "VERACRUZ DE IGNACIO DE LA LLAVE") ~ "Veracruz De Ignacio De La Llave",
    x_clean %in% c("COAHUILA", "COAHUILA DE ZARAGOZA") ~ "Coahuila De Zaragoza",
    x_clean %in% c("QUERETARO", "QUERETARO DE ARTEAGA") ~ "Queretaro",
    x_clean %in% c("GUERERO", "GUERRERO") ~ "Guerrero",
    TRUE ~ normalize_text(x)
  )
}

parse_us_state <- function(path) {
  basename(path) %>%
    tools::file_path_sans_ext() %>%
    stringr::str_remove("_\\d{4}$") %>%
    stringr::str_replace_all("_", " ") %>%
    normalize_text()
}

years <- 2010:2024

base_dir <- resolve_matrix_input_dir(
  candidates = c(
    path_in_repo("2_SQL_database", "Data_clean_updated", "MCAS", "Estados_US"),
    path_in_repo("Data_clean_updated", "MCAS", "Estados_US"),
    path_in_repo("1_network_estimation", "1_data_cleaning", "Data_clean", "MCAS", "Estados_US"),
    path_in_repo("Data_clean", "MCAS", "Estados_US")
  ),
  years = years
)

matrix_output_dir <- path_in_repo("1_network_estimation", "2_migration_matrix_estimation", "yearly_migration_matrices_2")
validation_dir <- path_in_repo("1_network_estimation", "2_migration_matrix_estimation", "validation reports")
clean_output_dir <- path_in_repo("1_network_estimation", "2_migration_matrix_estimation", "clean final data")

dir.create(matrix_output_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(validation_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(clean_output_dir, recursive = TRUE, showWarnings = FALSE)

official_file <- resolve_existing_file(c(
  path_in_repo("Data", "catun_municipio", "catun_municipio", "AGEEML_2026318759531.xlsx")
))

message("Using cleaned input directory: ", base_dir)
message("Using official INEGI file: ", official_file)

observed_name_fixes <- tribble(
  ~mx_state,              ~wrong,                                    ~correct,
  "Chiapas",              "Montecristo De Guerero",                  "Montecristo De Guerrero",
  "Chihuahua",            "Praxedis G. Guerero",                     "Praxedis G. Guerrero",
  "Chihuahua",            "Temosachi",                               "Temosachic",
  "Chihuahua",            "Guerero",                                 "Guerrero",
  "Coahuila De Zaragoza", "Guerero",                                 "Guerrero",
  "Coahuila De Zaragoza", "Cuatrocienegas",                          "Cuatro Cienegas",
  "Durango",              "Gral. Simon Boivar",                      "General Simon Bolivar",
  "Durango",              "Vicente Guerero",                         "Vicente Guerrero",
  "Estado De Mexico",     "San Simon De Guerero",                    "San Simon De Guerrero",
  "Estado De Mexico",     "Villa Guerero",                           "Villa Guerrero",
  "Hidalgo",              "Santiago Tulantepec De Lugo Guerero",     "Santiago Tulantepec De Lugo Guerrero",
  "Hidalgo",              "Tepehuacan De Guerero",                   "Tepehuacan De Guerrero",
  "Hidalgo",              "Huehuetl",                                "Huehuetla",
  "Hidalgo",              "Huichapa",                                "Huichapan",
  "Jalisco",              "Villa Guerero",                           "Villa Guerrero",
  "Oaxaca",               "Putla Villa De Guerero",                  "Putla Villa De Guerrero",
  "Oaxaca",               "Yutanduchi De Guerero",                   "Yutanduchi De Guerrero",
  "Oaxaca",               "San Bartolome Yucane",                    "San Bartolome Yucuane",
  "Oaxaca",               "Santo Domingo Ingeni",                    "Santo Domingo Ingenio",
  "Oaxaca",               "Cuilapam De Guerero",                     "Cuilapam De Guerrero",
  "Oaxaca",               "Santa Ana Tlapacoya",                     "Santa Ana Tlapacoyan",
  "Oaxaca",               "San Pedro Totolapa",                      "San Pedro Totolapam",
  "Oaxaca",               "San Pedro Mixtepec - Distr. 22 -",        "San Pedro Mixtepec - Dto. 22 -",
  "Oaxaca",               "San Pedro Mixtepec - Distr. 26 -",        "San Pedro Mixtepec - Dto. 26 -",
  "Oaxaca",               "San Juan Mixtepec - Distr. 08 -",         "San Juan Mixtepec - Dto. 08 -",
  "Oaxaca",               "San Juan Mixtepec - Distr. 26 -",         "San Juan Mixtepec - Dto. 26 -",
  "Oaxaca",               "San Juan Mixtepec - Distrito 08 -",       "San Juan Mixtepec - Dto. 08 -",
  "Oaxaca",               "San Juan Mixtepec - Distrito 26 -",       "San Juan Mixtepec - Dto. 26 -",
  "Puebla",               "Totoltepec De Guerero",                   "Totoltepec De Guerrero",
  "Puebla",               "Ixcamilpa De Guerero",                    "Ixcamilpa De Guerrero",
  "Puebla",               "Ayotoxco De Guerero",                     "Ayotoxco De Guerrero",
  "Puebla",               "Vicente Guerero",                         "Vicente Guerrero",
  "Puebla",               "General Felipe _Ngeles",                  "General Felipe Angeles",
  "Sonora",               "San Pedro De La Cuev",                    "San Pedro De La Cueva",
  "Tamaulipas",           "Guerero",                                 "Guerrero",
  "Tlaxcala",             "Amaxac De Guerero",                       "Amaxac De Guerrero",
  "Tlaxcala",             "Yauhquemecan",                            "Yauhquemehcan",
  "Tlaxcala",             "Altzayanca",                              "Atltzayanca"
) %>%
  mutate(
    mx_state = normalize_state_for_matching(mx_state),
    wrong = normalize_text(wrong),
    correct = normalize_text(correct)
  )

municipality_aliases <- tribble(
  ~mx_state,              ~source,                                  ~official,
  "Coahuila De Zaragoza", "Cuatrocienegas",                         "Cuatro Cienegas",
  "Durango",              "Gral Simon Bolivar",                     "General Simon Bolivar",
  "Durango",              "Gral. Simon Bolivar",                    "General Simon Bolivar",
  "Guanajuato",           "San Jose Iturbide",                      "San Jose De Iturbide",
  "Jalisco",              "San Martin De Hidalgo",                  "San Martin Hidalgo",
  "Nuevo Leon",           "Gral Escobedo",                          "General Escobedo",
  "Nuevo Leon",           "Gral. Escobedo",                         "General Escobedo",
  "Nuevo Leon",           "Gral Zaragoza",                          "General Zaragoza",
  "Nuevo Leon",           "Gral. Zaragoza",                         "General Zaragoza",
  "Nuevo Leon",           "Gral Trevino",                           "General Trevino",
  "Nuevo Leon",           "Gral. Trevino",                          "General Trevino",
  "Oaxaca",               "Magdalena Apasco",                       "Magdalena Apazco",
  "Quintana Roo",         "Solidaridad",                            "Playa Del Carmen",
  "Tlaxcala",             "Zitlaltepec De Trinidad Sanchez Santos", "Ziltlaltepec De Trinidad Sanchez Santos",
  "Veracruz De Ignacio De La Llave", "Tuxpam",                      "Tuxpan"
) %>%
  mutate(
    mx_state = normalize_state_for_matching(mx_state),
    source = normalize_text(source),
    official = normalize_text(official)
  )

apply_observed_name_fixes <- function(df) {
  df %>%
    left_join(
      observed_name_fixes,
      by = c("mx_state" = "mx_state", "mx_municipality" = "wrong")
    ) %>%
    mutate(mx_municipality = coalesce(correct, mx_municipality)) %>%
    select(-correct)
}

read_state_file <- function(path, yr) {
  read_excel(path) %>%
    select(mx_state, mx_municipality, n_matriculas) %>%
    mutate(
      mx_state = normalize_state_for_matching(mx_state),
      mx_municipality = normalize_text(mx_municipality),
      n_matriculas = suppressWarnings(as.numeric(n_matriculas))
    ) %>%
    filter(
      !is.na(mx_state),
      !is.na(mx_municipality),
      mx_state != "Total",
      mx_municipality != "Total",
      !str_detect(mx_municipality, regex("no se registro", ignore_case = TRUE)),
      !str_detect(mx_municipality, regex("desconocido", ignore_case = TRUE))
    ) %>%
    apply_observed_name_fixes() %>%
    mutate(
      year = yr,
      us_state = parse_us_state(path)
    ) %>%
    select(year, us_state, mx_state, mx_municipality, n_matriculas)
}

read_inegi_catalog <- function(path) {
  col_names <- c(
    "CVEGEO", "CVE_ENT", "NOM_ENT", "NOM_ABR",
    "CVE_MUN", "NOM_MUN", "CVE_CAB", "NOM_CAB",
    "POB_TOTAL", "POB_MASC", "POB_FEM", "TOTAL_LOC"
  )

  read_excel(path, skip = 4, col_names = col_names) %>%
    filter(
      !is.na(CVEGEO),
      !is.na(NOM_ENT),
      !is.na(NOM_MUN),
      CVEGEO != "",
      NOM_ENT != "",
      NOM_MUN != ""
    ) %>%
    distinct(NOM_ENT, NOM_MUN, .keep_all = TRUE) %>%
    transmute(
      official_state_raw = normalize_text(NOM_ENT),
      official_municipality_raw = normalize_text(NOM_MUN),
      official_state = normalize_state_for_matching(NOM_ENT),
      official_municipality = normalize_text(NOM_MUN),
      state_key = super_clean(official_state),
      muni_key = super_clean(official_municipality),
      muni_canon = canonical_municipality(official_municipality)
    )
}

message("Reading yearly cleaned files...")

# -----------------------------------------------------------------------------------#

# Load the data that we are going to use

panel <- purrr::map_dfr(years, function(yr) {
  yr_dir <- file.path(base_dir, paste0("Edos_USA_", yr))
  files <- list.files(yr_dir, pattern = "\\.xlsx$", full.names = TRUE, ignore.case = TRUE)

  if (length(files) == 0) {
    warning("No files found for year ", yr)
    return(tibble())
  }

  purrr::map_dfr(files, read_state_file, yr = yr)
}) %>%
  arrange(year, us_state, mx_state, mx_municipality)

# Load the offial Municipality names data 
official_tbl <- read_inegi_catalog(official_file)

############################################################################
# DATA PROCESSING STEP:
# 
# 1. BATCH IMPORT: 
#    - Loops through years 2010-2024.
#    - Locates subfolders (e.g., "Edos_USA_2010").
#    - Reads every US State .xlsx file within those folders.
#
# 2. CONSOLIDATION:
#    - Aggregates thousands of individual spreadsheets into one 
#      long-format master dataframe ('panel').
#    - Sorts the master data for consistency.
#
# 3. REFERENCE LOADING:
#    - Imports the INEGI municipality catalog ('official_tbl') to act 
#      as the 'source of truth' for geographic validation.
############################################################################

##### 
# official_tbl -- official municipality names at 2026
# panel -- year | state | mx_state | mx_municipality | n_matriculas (dimension: year x us_state x mx_state x mx_municipality = 635318 observations * 5 variables)
#####

observed_pairs <- panel %>%
  distinct(mx_state, mx_municipality) %>%
  mutate(
    state_key = super_clean(mx_state),
    muni_key = super_clean(mx_municipality),
    muni_canon = canonical_municipality(mx_municipality)
  )

exact_match <- observed_pairs %>%
  left_join(
    official_tbl %>%
      select(state_key, muni_key, official_state, official_municipality),
    by = c("state_key", "muni_key")
  ) %>%
  filter(!is.na(official_state)) %>%
  transmute(
    mx_state,
    mx_municipality,
    official_state,
    official_municipality,
    resolution = "exact_same_state"
  )

alias_match <- observed_pairs %>%
  left_join(
    municipality_aliases %>%
      transmute(
        state_key = super_clean(mx_state),
        source_key = super_clean(source),
        alias_official = official
      ),
    by = c("state_key", "muni_key" = "source_key")
  ) %>%
  filter(!is.na(alias_official)) %>%
  left_join(
    official_tbl %>%
      select(state_key, official_state, official_municipality),
    by = c("state_key", "alias_official" = "official_municipality")
  ) %>%
  filter(!is.na(official_state)) %>%
  transmute(
    mx_state,
    mx_municipality,
    official_state,
    official_municipality = alias_official,
    resolution = "safe_alias_same_state"
  )

canonical_same_state_match <- observed_pairs %>%
  left_join(
    official_tbl %>%
      select(state_key, official_state, official_municipality, muni_canon),
    by = c("state_key", "muni_canon")
  ) %>%
  group_by(mx_state, mx_municipality) %>%
  filter(n_distinct(official_municipality) == 1) %>%
  ungroup() %>%
  filter(!is.na(official_state)) %>%
  transmute(
    mx_state,
    mx_municipality,
    official_state,
    official_municipality,
    resolution = "canonical_same_state"
  )

unique_global_exact <- official_tbl %>%
  count(muni_key, name = "global_exact_count") %>%
  filter(global_exact_count == 1) %>%
  inner_join(
    official_tbl %>%
      select(muni_key, official_state, official_municipality),
    by = "muni_key"
  )

unique_global_canonical <- official_tbl %>%
  count(muni_canon, name = "global_canonical_count") %>%
  filter(global_canonical_count == 1) %>%
  inner_join(
    official_tbl %>%
      select(muni_canon, official_state, official_municipality),
    by = "muni_canon"
  )

state_reassignment_exact <- observed_pairs %>%
  left_join(unique_global_exact, by = "muni_key") %>%
  filter(!is.na(official_state)) %>%
  transmute(
    mx_state,
    mx_municipality,
    official_state,
    official_municipality,
    resolution = if_else(mx_state == official_state, "exact_same_state", "inegi_state_reassignment_exact")
  )

state_reassignment_canonical <- observed_pairs %>%
  left_join(unique_global_canonical, by = "muni_canon") %>%
  filter(!is.na(official_state)) %>%
  transmute(
    mx_state,
    mx_municipality,
    official_state,
    official_municipality,
    resolution = if_else(mx_state == official_state, "canonical_same_state", "inegi_state_reassignment_canonical")
  )

mapping_candidates <- bind_rows(
  exact_match,
  alias_match,
  canonical_same_state_match,
  state_reassignment_exact,
  state_reassignment_canonical
) %>%
  mutate(
    resolution_rank = case_when(
      resolution == "exact_same_state" ~ 1L,
      resolution == "safe_alias_same_state" ~ 2L,
      resolution == "canonical_same_state" ~ 3L,
      resolution == "inegi_state_reassignment_exact" ~ 4L,
      resolution == "inegi_state_reassignment_canonical" ~ 5L,
      TRUE ~ 99L
    )
  ) %>%
  arrange(mx_state, mx_municipality, resolution_rank, official_state, official_municipality) %>%
  distinct(mx_state, mx_municipality, .keep_all = TRUE) %>%
  select(-resolution_rank)

candidate_summary_exact <- observed_pairs %>%
  left_join(
    official_tbl %>%
      select(muni_key, candidate_state = official_state),
    by = "muni_key",
    relationship = "many-to-many"
  ) %>%
  group_by(mx_state, mx_municipality) %>%
  summarise(
    exact_candidate_count = n_distinct(candidate_state[!is.na(candidate_state)]),
    exact_candidate_states = paste(sort(unique(candidate_state[!is.na(candidate_state)])), collapse = " | "),
    .groups = "drop"
  ) %>%
  mutate(
    exact_candidate_count = na_if(exact_candidate_count, 0L),
    exact_candidate_states = na_if(exact_candidate_states, "")
  )

candidate_summary_canonical <- observed_pairs %>%
  left_join(
    official_tbl %>%
      select(muni_canon, candidate_state = official_state),
    by = "muni_canon",
    relationship = "many-to-many"
  ) %>%
  group_by(mx_state, mx_municipality) %>%
  summarise(
    canonical_candidate_count = n_distinct(candidate_state[!is.na(candidate_state)]),
    canonical_candidate_states = paste(sort(unique(candidate_state[!is.na(candidate_state)])), collapse = " | "),
    .groups = "drop"
  ) %>%
  mutate(
    canonical_candidate_count = na_if(canonical_candidate_count, 0L),
    canonical_candidate_states = na_if(canonical_candidate_states, "")
  )

mapping_report <- observed_pairs %>%
  left_join(mapping_candidates, by = c("mx_state", "mx_municipality")) %>%
  left_join(candidate_summary_exact, by = c("mx_state", "mx_municipality")) %>%
  left_join(candidate_summary_canonical, by = c("mx_state", "mx_municipality")) %>%
  mutate(
    candidate_count = coalesce(exact_candidate_count, canonical_candidate_count),
    candidate_states = coalesce(exact_candidate_states, canonical_candidate_states),
    suggested_state = if_else(candidate_count == 1, candidate_states, NA_character_),
    state_mismatch_flag = !is.na(suggested_state) & suggested_state != mx_state,
    validation_status = if_else(!is.na(official_state), "resolved", "dropped_unresolved"),
    drop_reason = case_when(
      validation_status == "resolved" ~ NA_character_,
      is.na(candidate_count) ~ "no_inegi_candidate",
      candidate_count == 1 & state_mismatch_flag ~ "expected_reassignment_not_resolved",
      candidate_count == 1 ~ "single_candidate_not_resolved",
      candidate_count > 1 ~ "ambiguous_multiple_candidates",
      TRUE ~ "unresolved"
    )
  ) %>%
  select(
    mx_state,
    mx_municipality,
    validation_status,
    resolution,
    official_state,
    official_municipality,
    candidate_count,
    candidate_states,
    suggested_state,
    state_mismatch_flag,
    drop_reason
  ) %>%
  arrange(validation_status, mx_state, mx_municipality)

resolved_lookup <- mapping_report %>%
  filter(validation_status == "resolved") %>%
  select(mx_state, mx_municipality, official_state, official_municipality, resolution)

unresolved_pairs <- mapping_report %>%
  filter(validation_status != "resolved")

clean_panel <- panel %>%
  inner_join(resolved_lookup, by = c("mx_state", "mx_municipality")) %>%
  transmute(
    year,
    us_state,
    mx_state_original = mx_state,
    mx_municipality_original = mx_municipality,
    mx_state = official_state,
    mx_municipality = official_municipality,
    resolution,
    n_matriculas
  ) %>%
  group_by(year, us_state, mx_state, mx_municipality) %>%
  summarise(
    n_matriculas = sum(n_matriculas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(year, us_state, mx_state, mx_municipality)

dropped_panel_rows <- panel %>%
  anti_join(resolved_lookup, by = c("mx_state", "mx_municipality")) %>%
  arrange(year, us_state, mx_state, mx_municipality)

clean_universe <- clean_panel %>%
  distinct(mx_state, mx_municipality) %>%
  arrange(mx_state, mx_municipality)

us_state_universe <- sort(unique(panel$us_state))

# outputs: 

# us_state_universe -- sorted vector of all US states observed in the panel data (length: 51, including "District Of Columbia")

# clean_universe -- dataframe of unique (mx_state, mx_municipality) pairs that are present in the cleaned panel after mapping to official names (dimension: 2443 rows x 2 variables)

# clean_panel -- the main cleaned panel dataframe with official municipality names and original names for reference (dimension: 634,000+ rows x 7 variables)

# mapping_report -- a detailed report of the mapping process for each unique (mx_state, mx_municipality) pair, including resolution status and candidate states (dimension: 2443 rows x 11 variables)

# unresolved_pairs -- a subset of mapping_report where validation_status is not "resolved", indicating pairs that could not be confidently mapped to the official catalog (dimension: 123 rows x 11 variables)

#----------------------------------------------------------------------------#

known_missing_states <- tribble(
  ~year, ~us_state,      ~known_missing_reason,
  2013L, "Florida",     "Set to NA because the source workbook has no municipality-level rows",
  2024L, "Connecticut", "Set to NA because the source workbook has no municipality-level rows"
)

state_file_audit <- expand_grid(
  year = years,
  us_state = us_state_universe
) %>%
  left_join(
    purrr::map_dfr(years, function(yr) {
      yr_dir <- file.path(base_dir, paste0("Edos_USA_", yr))
      files <- list.files(yr_dir, pattern = "\\.xlsx$", full.names = TRUE, ignore.case = TRUE)
      tibble(
        year = yr,
        us_state = purrr::map_chr(files, parse_us_state),
        cleaned_input_file = basename(files)
      )
    }),
    by = c("year", "us_state")
  ) %>%
  left_join(
    panel %>%
      count(year, us_state, name = "panel_rows"),
    by = c("year", "us_state")
  ) %>%
  left_join(
    clean_panel %>%
      count(year, us_state, name = "clean_panel_rows"),
    by = c("year", "us_state")
  ) %>%
  left_join(known_missing_states, by = c("year", "us_state")) %>%
  mutate(
    cleaned_input_file_exists = !is.na(cleaned_input_file),
    panel_rows = replace_na(panel_rows, 0L),
    clean_panel_rows = replace_na(clean_panel_rows, 0L),
    forced_to_na_in_matrix = !is.na(known_missing_reason),
    audit_status = case_when(
      !cleaned_input_file_exists ~ "missing_from_cleaned_input_folder",
      panel_rows == 0 ~ "file_present_but_no_rows_read",
      clean_panel_rows == 0 ~ "rows_read_but_all_dropped_in_validation",
      forced_to_na_in_matrix ~ "present_in_clean_panel_but_forced_to_na",
      TRUE ~ "ok"
    )
  ) %>%
  arrange(year, us_state)

set_known_missing_states_to_na <- function(df, yr) {
  if (yr == 2013 && "Florida" %in% names(df)) {
    df <- mutate(df, Florida = NA_real_)
  }
  if (yr == 2024 && "Connecticut" %in% names(df)) {
    df <- mutate(df, Connecticut = NA_real_)
  }
  df
}

build_year_matrix <- function(yr, panel_df, municipality_universe, us_state_cols) {
  matrix_df <- panel_df %>%
    filter(year == yr) %>%
    select(mx_state, mx_municipality, us_state, n_matriculas) %>%
    pivot_wider(
      names_from = us_state,
      values_from = n_matriculas,
      values_fill = 0,
      values_fn = sum
    ) %>%
    right_join(municipality_universe, by = c("mx_state", "mx_municipality"))

  missing_cols <- setdiff(us_state_cols, names(matrix_df))
  for (col_name in missing_cols) {
    matrix_df[[col_name]] <- 0
  }

  matrix_df <- matrix_df %>%
    mutate(across(all_of(us_state_cols), ~ replace_na(.x, 0))) %>%
    select(mx_state, mx_municipality, all_of(us_state_cols)) %>%
    arrange(mx_state, mx_municipality)

  matrix_df <- set_known_missing_states_to_na(matrix_df, yr)

  matrix_df <- matrix_df %>%
    mutate(Total = rowSums(across(all_of(us_state_cols)), na.rm = TRUE)) %>%
    bind_rows(
      summarise(
        .,
        mx_state = "Total",
        mx_municipality = "Total",
        across(all_of(c(us_state_cols, "Total")), \(x) sum(x, na.rm = TRUE))
      )
    )

  matrix_df
}

message("Building clean yearly matrices...")

matrices <- purrr::map(years, build_year_matrix,
                       panel_df = clean_panel,
                       municipality_universe = clean_universe,
                       us_state_cols = us_state_universe) %>%
  set_names(paste0("MIGRATION_MATRIX_", years))

walk2(matrices, names(matrices), function(df, name) {
  write_xlsx(df, path = file.path(matrix_output_dir, paste0(name, ".xlsx")))
})

write_csv(mapping_report, file.path(validation_dir, "municipality_mapping_report.csv"))
write_csv(unresolved_pairs, file.path(validation_dir, "municipality_unresolved_report.csv"))
write_csv(dropped_panel_rows, file.path(validation_dir, "dropped_panel_rows.csv"))
write_csv(state_file_audit, file.path(validation_dir, "state_file_audit.csv"))
write_xlsx(
  list(
    municipality_mapping_report = mapping_report,
    municipality_unresolved_report = unresolved_pairs,
    dropped_panel_rows = dropped_panel_rows,
    state_file_audit = state_file_audit
  ),
  path = file.path(validation_dir, "municipality_validation_report.xlsx")
)

write_csv(clean_panel, file.path(clean_output_dir, "clean_migration_panel.csv"))
write_xlsx(
  list(clean_migration_panel = clean_panel),
  path = file.path(clean_output_dir, "clean_migration_panel.xlsx")
)
write_csv(clean_universe, file.path(clean_output_dir, "clean_municipality_universe.csv"))

message("Raw municipality-state pairs: ", nrow(observed_pairs))
message("Resolved municipality-state pairs kept: ", nrow(resolved_lookup))
message("Unresolved municipality-state pairs dropped: ", nrow(unresolved_pairs))
message("Final clean panel rows: ", nrow(clean_panel))
message("Dropped panel rows: ", nrow(dropped_panel_rows))
message("Clean panel written to: ", file.path(clean_output_dir, "clean_migration_panel.csv"))
message("Mapping report written to: ", file.path(validation_dir, "municipality_mapping_report.csv"))
