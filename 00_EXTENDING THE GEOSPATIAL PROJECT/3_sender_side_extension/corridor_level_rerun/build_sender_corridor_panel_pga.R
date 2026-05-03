library(data.table)

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
      "00_EXTENDING THE GEOSPATIAL PROJECT",
      "3_sender_side_extension",
      "corridor_level_rerun",
      "build_sender_corridor_panel_pga.R"
    ),
    winslash = "/",
    mustWork = FALSE
  )
}

normalize_text <- function(x) {
  x <- iconv(x, to = "ASCII//TRANSLIT")
  x <- tolower(x)
  x <- gsub("[^a-z0-9]+", " ", x)
  x <- gsub(" +", " ", x)
  trimws(x)
}

normalize_state <- function(x) {
  x <- normalize_text(x)
  x <- sub("^coahuila de zaragoza$", "coahuila", x)
  x <- sub("^michoacan de ocampo$", "michoacan", x)
  x <- sub("^estado de mexico$", "mexico", x)
  x <- sub("^veracruz de ignacio de la llave$", "veracruz", x)
  x <- sub("^queretaro de arteaga$", "queretaro", x)
  x <- sub("^distrito federal$", "ciudad de mexico", x)
  x
}

normalize_municipality <- function(state_key, muni_key) {
  out <- muni_key

  out[state_key == "chihuahua" & muni_key == "batopilas de manuel gomez morin"] <- "batopilas"
  out[state_key == "guanajuato" & muni_key == "san jose de iturbide"] <- "san jose iturbide"
  out[state_key == "morelos" & muni_key == "jonacatepec de leandro valle"] <- "jonacatepec"
  out[state_key == "oaxaca" & muni_key == "constancia del rosario"] <- "rosario"
  out[state_key == "oaxaca" & muni_key == "heroica villa tezoatlan de segura y luna cuna de la independencia de oaxaca"] <- "heroica villa tezoatlan de segura y luna"
  out[state_key == "oaxaca" & muni_key == "magdalena apazco"] <- "magdalena apasco"
  out[state_key == "oaxaca" & muni_key == "san juan mixtepec distr 08"] <- "san juan mixtepec dto 08"
  out[state_key == "oaxaca" & muni_key == "san juan mixtepec distr 26"] <- "san juan mixtepec dto 26"
  out[state_key == "oaxaca" & muni_key == "san pedro mixtepec distr 22"] <- "san pedro mixtepec dto 22"
  out[state_key == "oaxaca" & muni_key == "san pedro mixtepec distr 26"] <- "san pedro mixtepec dto 26"
  out[state_key == "oaxaca" & muni_key == "villa de tututepec"] <- "villa de tututepec de melchor ocampo"
  out[state_key == "quintana roo" & muni_key == "playa del carmen"] <- "solidaridad"
  out[state_key == "veracruz" & muni_key == "medellin de bravo"] <- "medellin"

  out
}

script_path <- get_script_path()
repo_root <- normalizePath(file.path(dirname(script_path), "..", "..", ".."), winslash = "/", mustWork = TRUE)
extension_dir <- file.path(repo_root, "00_EXTENDING THE GEOSPATIAL PROJECT", "3_sender_side_extension", "corridor_level_rerun")
dir.create(file.path(extension_dir, "output"), recursive = TRUE, showWarnings = FALSE)

flow_path <- file.path(
  repo_root,
  "1_network_estimation",
  "4_remittance_calibration",
  "output",
  "calibrated_remittance_flows_master_2013q1_2024q4.csv"
)
exposure_path <- file.path(
  repo_root,
  "00_EXTENDING THE GEOSPATIAL PROJECT",
  "1_cleaned_panels",
  "panel_remittances_pga_calibrated.csv"
)
output_path <- file.path(extension_dir, "output", "panel_sender_corridor_pga_calibrated.csv")
report_path <- file.path(extension_dir, "output", "panel_sender_corridor_pga_calibrated_match_report.csv")

cat("Reading calibrated sender-receiver flow matrix...\n")
flows <- fread(file = flow_path)

cat("Reading recipient-side PGA template...\n")
exposure <- fread(file = exposure_path)[, .(state, municipality, year, quarter, mean_pga, event)]
exposure <- unique(exposure)

flows[, state_key := normalize_state(mx_state)]
flows[, muni_key := normalize_text(mx_municipality)]
flows[, muni_key := normalize_municipality(state_key, muni_key)]

exposure[, state_key := normalize_state(state)]
exposure[, muni_key := normalize_text(municipality)]
exposure[, muni_key := normalize_municipality(state_key, muni_key)]

cat("Merging earthquake exposure onto sender corridors...\n")
panel <- merge(
  flows,
  exposure[, .(state_key, muni_key, year, quarter, mean_pga, event)],
  by = c("state_key", "muni_key", "year", "quarter"),
  all.x = TRUE,
  sort = FALSE
)

setnames(panel, c("mx_state", "mx_municipality"), c("state", "municipality"))

panel[
  ,
  `:=`(
    mean_r = mean(remittances_musd, na.rm = TRUE),
    sd_r = sd(remittances_musd, na.rm = TRUE)
  ),
  by = .(us_state, state, municipality)
]

panel[, outlier := fifelse(
  is.na(remittances_musd) | is.na(mean_r) | is.na(sd_r),
  NA,
  remittances_musd > mean_r + 5 * sd_r
)]
panel[, remittances_kusd := remittances_musd * 1000]
panel[, asinh_remittances := asinh(remittances_kusd)]

panel[, recipient_id := paste(state, municipality, sep = "_")]
panel[, corridor_id := paste(us_state, state, municipality, sep = "_")]

match_report <- panel[
  ,
  .(
    matched_quarters = sum(!is.na(mean_pga)),
    total_quarters = .N,
    fully_matched = all(!is.na(mean_pga)),
    total_remittances_musd = sum(remittances_musd, na.rm = TRUE)
  ),
  by = .(us_state, state, municipality)
][order(fully_matched, us_state, state, municipality)]

setcolorder(
  panel,
  c(
    "period_date",
    "year",
    "quarter",
    "year_quarter",
    "us_state",
    "state",
    "municipality",
    "remittances_musd",
    "mean_r",
    "sd_r",
    "outlier",
    "remittances_kusd",
    "asinh_remittances",
    "mean_pga",
    "event",
    "recipient_id",
    "corridor_id"
  )
)

panel[, c("state_key", "muni_key") := NULL]

cat("Writing corridor-level calibrated panel...\n")
fwrite(panel, output_path)

cat("Writing corridor-level match report...\n")
fwrite(match_report, report_path)

cat("\nBuild complete.\n")
cat("Output:", output_path, "\n")
cat("Report:", report_path, "\n")
cat("Rows:", nrow(panel), "\n")
cat("Matched rows:", sum(!is.na(panel$mean_pga)), "\n")
cat("Unmatched rows:", sum(is.na(panel$mean_pga)), "\n")
