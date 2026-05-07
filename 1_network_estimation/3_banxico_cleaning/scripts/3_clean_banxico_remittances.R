# Clean Banxico remittances inputs and reconcile municipality names to the
# migration-weighting municipality universe.

rm(list = ls())

library(tidyverse)
library(readxl)
library(stringi)

get_script_path <- function() {
  cmd_args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", cmd_args, value = TRUE)

  if (length(file_arg) > 0) {
    return(normalizePath(sub("^--file=", "", file_arg[1]), winslash = "/", mustWork = TRUE))
  }

  this_file <- tryCatch(
    normalizePath(sys.frame(1)$ofile, winslash = "/", mustWork = TRUE),
    error = function(...) NA_character_
  )

  if (!is.na(this_file)) {
    return(this_file)
  }

  normalizePath(
    file.path(
      getwd(),
      "1_network_estimation",
      "3_banxico_cleaning",
      "scripts",
      "3_clean_banxico_remittances.R"
    ),
    winslash = "/",
    mustWork = FALSE
  )
}

script_path <- get_script_path()
repo_root <- normalizePath(file.path(dirname(script_path), "..", "..", ".."), winslash = "/", mustWork = TRUE)

path_in_repo <- function(...) {
  normalizePath(file.path(repo_root, ...), winslash = "/", mustWork = FALSE)
}

normalize_text <- function(x) {
  out <- as.character(x)
  out[is.na(out)] <- NA_character_
  out <- stringi::stri_trans_general(out, "Latin-ASCII")
  out <- stringr::str_replace_all(out, "[[:space:]]+", " ")
  out <- stringr::str_squish(out)
  out <- stringr::str_to_title(out)
  dplyr::na_if(out, "")
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

normalize_mx_state <- function(x) {
  x_clean <- super_clean(x)

  dplyr::case_when(
    is.na(x_clean) ~ NA_character_,
    x_clean %in% c("DISTRITO FEDERAL", "CIUDAD DE MEXICO") ~ "Ciudad De Mexico",
    x_clean %in% c("MEXICO", "ESTADO DE MEXICO") ~ "Estado De Mexico",
    x_clean %in% c("MICHOACAN", "MICHOACAN DE OCAMPO") ~ "Michoacan De Ocampo",
    x_clean %in% c("VERACRUZ", "VERACRUZ DE IGNACIO DE LA LLAVE") ~ "Veracruz De Ignacio De La Llave",
    x_clean %in% c("COAHUILA", "COAHUILA DE ZARAGOZA") ~ "Coahuila De Zaragoza",
    x_clean %in% c("QUERETARO", "QUERETARO DE ARTEAGA") ~ "Queretaro",
    TRUE ~ normalize_text(x)
  )
}

normalize_us_state <- function(x) {
  out <- normalize_text(x)

  dplyr::case_when(
    is.na(out) ~ NA_character_,
    out %in% c("Washington Dc", "Washington D C") ~ "District Of Columbia",
    out == "Luisiana" ~ "Louisiana",
    TRUE ~ out
  )
}

coalesce_first <- function(...) {
  vals <- list(...)
  out <- vals[[1]]

  if (length(vals) == 1) {
    return(out)
  }

  for (idx in 2:length(vals)) {
    replace_idx <- is.na(out) | out == ""
    out[replace_idx] <- vals[[idx]][replace_idx]
  }

  out
}

parse_period_date <- function(x) {
  if (inherits(x, "POSIXt")) {
    return(as.Date(x))
  }

  if (inherits(x, "Date")) {
    return(x)
  }

  numeric_x <- suppressWarnings(as.numeric(x))
  numeric_dates <- suppressWarnings(as.Date(numeric_x, origin = "1899-12-30"))

  char_x <- as.character(x)
  char_dates <- suppressWarnings(lubridate::ymd_hms(char_x, quiet = TRUE))
  char_dates <- as.Date(char_dates)

  missing_char <- is.na(char_dates)
  if (any(missing_char)) {
    char_dates[missing_char] <- suppressWarnings(lubridate::ymd(char_x[missing_char], quiet = TRUE))
  }

  coalesce(numeric_dates, char_dates)
}

aliases <- tribble(
  ~mx_state,                         ~source,                                   ~official,
  "Chihuahua",                       "Batopilas",                               "Batopilas De Manuel Gomez Morin",
  "Coahuila De Zaragoza",            "Cuatrocienegas",                          "Cuatro Cienegas",
  "Durango",                         "Gral Simon Bolivar",                      "General Simon Bolivar",
  "Durango",                         "Gral. Simon Bolivar",                     "General Simon Bolivar",
  "Estado De Mexico",                "Acambay",                                 "Acambay De Ruiz Castaneda",
  "Guanajuato",                      "San Jose Iturbide",                       "San Jose De Iturbide",
  "Guanajuato",                      "Silao",                                   "Silao De La Victoria",
  "Jalisco",                         "San Martin De Hidalgo",                   "San Martin Hidalgo",
  "Jalisco",                         "Tlaquepaque",                             "San Pedro Tlaquepaque",
  "Morelos",                         "Jonacatepec",                             "Jonacatepec De Leandro Valle",
  "Morelos",                         "Tlaltizapan",                             "Tlaltizapan De Zapata",
  "Morelos",                         "Zacualpan",                               "Zacualpan De Amilpas",
  "Nuevo Leon",                      "Carmen",                                  "El Carmen",
  "Nuevo Leon",                      "Gral Escobedo",                           "General Escobedo",
  "Nuevo Leon",                      "Gral. Escobedo",                          "General Escobedo",
  "Nuevo Leon",                      "Gral Zaragoza",                           "General Zaragoza",
  "Nuevo Leon",                      "Gral. Zaragoza",                          "General Zaragoza",
  "Nuevo Leon",                      "Gral Trevino",                            "General Trevino",
  "Nuevo Leon",                      "Gral. Trevino",                           "General Trevino",
  "Oaxaca",                          "Magdalena Apasco",                        "Magdalena Apazco",
  "Oaxaca",                          "Santo Domingo Tonaltepec",                "Santo Domingo Tomaltepec",
  "Oaxaca",                          "Tezoatlan De Segura Y Luna",              "Heroica Villa Tezoatlan De Segura Y Luna, Cuna De La Independencia De Oaxaca",
  "Oaxaca",                          "Villa De Tututepec De Melchor Ocampo",    "Villa De Tututepec",
  "Quintana Roo",                    "Solidaridad",                             "Playa Del Carmen",
  "Tlaxcala",                        "Zitlaltepec De Trinidad Sanchez Santos",  "Ziltlaltepec De Trinidad Sanchez Santos",
  "Veracruz De Ignacio De La Llave", "Medellin",                                "Medellin De Bravo",
  "Veracruz De Ignacio De La Llave", "Tuxpam",                                  "Tuxpan"
) %>%
  mutate(
    mx_state = normalize_mx_state(mx_state),
    source = normalize_text(source),
    official = normalize_text(official)
  )

output_dir <- path_in_repo("1_network_estimation", "3_banxico_cleaning", "output")

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

clean_universe <- readr::read_csv(
  path_in_repo("1_network_estimation", "2_migration_matrix_estimation", "clean final data", "clean_municipality_universe.csv"),
  show_col_types = FALSE
)

universe_lookup <- clean_universe %>%
  transmute(
    official_state = normalize_mx_state(mx_state),
    official_municipality = normalize_text(mx_municipality),
    state_key = super_clean(official_state),
    muni_key = super_clean(official_municipality),
    muni_canon = canonical_municipality(official_municipality)
  ) %>%
  distinct()

remittances_dir <- path_in_repo("Data", "Remittances")
origin_path <- file.path(remittances_dir, "Estado de origen de los ingresos por remesas provenientes de Estados Unidos.xlsx")
municipality_path <- list.files(remittances_dir, pattern = "^Ingresos por remesas.*\\.xlsx$", full.names = TRUE)

if (!file.exists(origin_path)) {
  stop("Could not find Banxico origin-state workbook.")
}

if (length(municipality_path) == 0) {
  stop("Could not find Banxico municipality workbook.")
}

municipality_path <- municipality_path[[1]]

parse_origin_state_remittances <- function(path) {
  raw <- read_excel(path, col_names = FALSE)
  headers <- as.character(raw[10, ] %>% unlist(use.names = FALSE))
  data <- raw[19:nrow(raw), , drop = FALSE]
  names(data) <- headers
  names(data)[1] <- "excel_date"

  data %>%
    mutate(
      period_date = parse_period_date(excel_date),
      year = lubridate::year(period_date),
      quarter = lubridate::quarter(period_date),
      year_quarter = paste0(year, "Q", quarter)
    ) %>%
    filter(!is.na(period_date), dplyr::between(year, 2013, 2024)) %>%
    rename_with(
      ~ stringr::str_remove(.x, "^Estado de origen de los ingresos por remesas provenientes de Estados Unidos, "),
      .cols = starts_with("Estado de origen de los ingresos por remesas provenientes de Estados Unidos, ")
    ) %>%
    pivot_longer(
      cols = -c(excel_date, period_date, year, quarter, year_quarter),
      names_to = "us_state_raw",
      values_to = "remittances_musd"
    ) %>%
    filter(us_state_raw != "Total") %>%
    transmute(
      period_date,
      year,
      quarter,
      year_quarter,
      us_state = normalize_us_state(us_state_raw),
      remittances_musd = suppressWarnings(as.numeric(remittances_musd))
    ) %>%
    arrange(period_date, us_state)
}

parse_municipality_remittances <- function(path) {
  raw <- read_excel(path, col_names = FALSE)
  headers <- as.character(raw[10, ] %>% unlist(use.names = FALSE))
  data <- raw[19:nrow(raw), , drop = FALSE]
  names(data) <- headers
  names(data)[1] <- "excel_date"

  municipality_meta <- tibble(series_title = names(data)) %>%
    filter(stringr::str_detect(series_title, "^Ingresos por Remesas, Distribución por Municipio, ")) %>%
    mutate(payload = stringr::str_remove(series_title, "^Ingresos por Remesas, Distribución por Municipio, ")) %>%
    tidyr::extract(payload, into = c("mx_state_raw", "mx_municipality_raw"), regex = "^(.*), (.*)$", remove = FALSE) %>%
    transmute(
      series_title,
      mx_state = normalize_mx_state(mx_state_raw),
      mx_municipality = normalize_text(mx_municipality_raw)
    ) %>%
    filter(!is.na(mx_state), !is.na(mx_municipality), mx_municipality != "No Identificado")

  data %>%
    mutate(
      period_date = parse_period_date(excel_date),
      year = lubridate::year(period_date),
      quarter = lubridate::quarter(period_date),
      year_quarter = paste0(year, "Q", quarter)
    ) %>%
    filter(!is.na(period_date), dplyr::between(year, 2013, 2024)) %>%
    pivot_longer(
      cols = all_of(municipality_meta$series_title),
      names_to = "series_title",
      values_to = "remittances_musd"
    ) %>%
    left_join(municipality_meta, by = "series_title") %>%
    transmute(
      period_date,
      year,
      quarter,
      year_quarter,
      mx_state,
      mx_municipality,
      remittances_musd = suppressWarnings(as.numeric(remittances_musd))
    ) %>%
    arrange(period_date, mx_state, mx_municipality)
}

resolve_municipality_matches <- function(observed_pairs, universe_lookup, aliases) {
  observed <- observed_pairs %>%
    mutate(
      state_key = super_clean(mx_state),
      muni_key = super_clean(mx_municipality),
      muni_canon = canonical_municipality(mx_municipality)
    )

  exact_match <- observed %>%
    left_join(
      universe_lookup %>%
        select(state_key, muni_key, official_state, official_municipality),
      by = c("state_key", "muni_key")
    ) %>%
    filter(!is.na(official_state)) %>%
    mutate(resolution = "exact_same_state")

  alias_match <- observed %>%
    left_join(
      aliases %>%
        transmute(
          state_key = super_clean(mx_state),
          source_key = super_clean(source),
          alias_official = official
        ),
      by = c("state_key", "muni_key" = "source_key")
    ) %>%
    filter(!is.na(alias_official)) %>%
    left_join(
      universe_lookup %>%
        select(state_key, official_state, official_municipality),
      by = c("state_key", "alias_official" = "official_municipality")
    ) %>%
    filter(!is.na(official_state)) %>%
    transmute(
      mx_state,
      mx_municipality,
      state_key,
      muni_key,
      muni_canon,
      official_state,
      official_municipality = alias_official,
      resolution = "safe_alias_same_state"
    )

  canonical_same_state_match <- observed %>%
    left_join(
      universe_lookup %>%
        select(state_key, muni_canon, official_state, official_municipality),
      by = c("state_key", "muni_canon")
    ) %>%
    group_by(mx_state, mx_municipality) %>%
    filter(n_distinct(official_municipality[!is.na(official_municipality)]) == 1) %>%
    ungroup() %>%
    filter(!is.na(official_state)) %>%
    mutate(resolution = "canonical_same_state")

  mapping_candidates <- bind_rows(
    exact_match,
    alias_match,
    canonical_same_state_match
  ) %>%
    mutate(
      resolution_rank = case_when(
        resolution == "exact_same_state" ~ 1L,
        resolution == "safe_alias_same_state" ~ 2L,
        resolution == "canonical_same_state" ~ 3L,
        TRUE ~ 99L
      )
    ) %>%
    arrange(mx_state, mx_municipality, resolution_rank, official_state, official_municipality) %>%
    distinct(mx_state, mx_municipality, .keep_all = TRUE) %>%
    select(-resolution_rank)

  exact_candidate_summary <- observed %>%
    left_join(
      universe_lookup %>%
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

  canonical_candidate_summary <- observed %>%
    left_join(
      universe_lookup %>%
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

  mapping_report <- observed %>%
    left_join(mapping_candidates, by = c("mx_state", "mx_municipality", "state_key", "muni_key", "muni_canon")) %>%
    left_join(exact_candidate_summary, by = c("mx_state", "mx_municipality")) %>%
    left_join(canonical_candidate_summary, by = c("mx_state", "mx_municipality")) %>%
    mutate(
      candidate_count = coalesce(exact_candidate_count, canonical_candidate_count),
      candidate_states = coalesce_first(exact_candidate_states, canonical_candidate_states),
      validation_status = if_else(!is.na(official_state), "resolved", "dropped_unresolved"),
      drop_reason = case_when(
        validation_status == "resolved" ~ NA_character_,
        is.na(candidate_count) ~ "no_weighting_universe_candidate",
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
      drop_reason
    ) %>%
    arrange(validation_status, mx_state, mx_municipality)

  resolved_lookup <- mapping_report %>%
    filter(validation_status == "resolved") %>%
    select(mx_state, mx_municipality, official_state, official_municipality, resolution)

  list(mapping_report = mapping_report, resolved_lookup = resolved_lookup)
}

origin_state_panel <- parse_origin_state_remittances(origin_path)
raw_municipality_panel <- parse_municipality_remittances(municipality_path)

observed_pairs <- raw_municipality_panel %>%
  distinct(mx_state, mx_municipality) %>%
  arrange(mx_state, mx_municipality)

match_results <- resolve_municipality_matches(observed_pairs, universe_lookup, aliases)
mapping_report <- match_results$mapping_report
resolved_lookup <- match_results$resolved_lookup

unresolved_report <- mapping_report %>%
  filter(validation_status != "resolved")

dropped_rows <- raw_municipality_panel %>%
  anti_join(resolved_lookup, by = c("mx_state", "mx_municipality")) %>%
  arrange(period_date, mx_state, mx_municipality)

clean_municipality_panel <- raw_municipality_panel %>%
  inner_join(resolved_lookup, by = c("mx_state", "mx_municipality")) %>%
  transmute(
    period_date,
    year,
    quarter,
    year_quarter,
    mx_state_original = mx_state,
    mx_municipality_original = mx_municipality,
    mx_state = official_state,
    mx_municipality = official_municipality,
    resolution,
    remittances_musd
  ) %>%
  group_by(period_date, year, quarter, year_quarter, mx_state, mx_municipality) %>%
  summarise(
    remittances_musd = sum(remittances_musd, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(period_date, mx_state, mx_municipality)

summary_report <- tibble(
  metric = c(
    "raw_municipality_state_pairs",
    "resolved_municipality_state_pairs",
    "unresolved_municipality_state_pairs",
    "clean_municipality_panel_rows",
    "clean_origin_state_panel_rows",
    "clean_municipality_total_musd",
    "dropped_municipality_total_musd",
    "dropped_municipality_share"
  ),
  value = c(
    nrow(observed_pairs),
    nrow(resolved_lookup),
    nrow(unresolved_report),
    nrow(clean_municipality_panel),
    nrow(origin_state_panel),
    sum(clean_municipality_panel$remittances_musd, na.rm = TRUE),
    sum(dropped_rows$remittances_musd, na.rm = TRUE),
    sum(dropped_rows$remittances_musd, na.rm = TRUE) /
      (sum(clean_municipality_panel$remittances_musd, na.rm = TRUE) + sum(dropped_rows$remittances_musd, na.rm = TRUE))
  )
)

readr::write_csv(
  origin_state_panel,
  file.path(output_dir, "banxico_origin_state_remittances_2013q1_2024q4.csv")
)
readr::write_csv(
  clean_municipality_panel,
  file.path(output_dir, "banxico_municipality_remittances_2013q1_2024q4.csv")
)
readr::write_csv(
  mapping_report,
  file.path(output_dir, "banxico_municipality_mapping_report.csv")
)
readr::write_csv(
  unresolved_report,
  file.path(output_dir, "banxico_municipality_unresolved_report.csv")
)
readr::write_csv(
  dropped_rows,
  file.path(output_dir, "banxico_municipality_dropped_rows_2013q1_2024q4.csv")
)
readr::write_csv(
  summary_report,
  file.path(output_dir, "banxico_municipality_cleaning_summary.csv")
)

message("Raw municipality-state pairs: ", nrow(observed_pairs))
message("Resolved municipality-state pairs kept: ", nrow(resolved_lookup))
message("Unresolved municipality-state pairs dropped: ", nrow(unresolved_report))
message("Clean municipality panel rows: ", nrow(clean_municipality_panel))
message("Clean origin-state panel rows: ", nrow(origin_state_panel))
