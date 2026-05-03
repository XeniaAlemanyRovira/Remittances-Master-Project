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
      "Project Geospatial",
      "scripts",
      "build_sender_state_panel_remittances_pga.R"
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

safe_weighted_mean <- function(x, w) {
  keep <- is.finite(x) & is.finite(w) & w > 0
  if (!any(keep)) {
    return(NA_real_)
  }
  sum(x[keep] * w[keep]) / sum(w[keep])
}

safe_max <- function(x) {
  keep <- is.finite(x)
  if (!any(keep)) {
    return(NA_real_)
  }
  max(x[keep])
}

script_path <- get_script_path()
repo_root <- normalizePath(file.path(dirname(script_path), "..", ".."), winslash = "/", mustWork = TRUE)

calibrated_path <- file.path(
  repo_root,
  "1_network_estimation",
  "4_remittance_calibration",
  "output",
  "calibrated_remittance_flows_master_2013q1_2024q4.csv"
)
exposure_template_path <- file.path(repo_root, "Project Geospatial", "data", "panel_remittances_pga.csv")
output_path <- file.path(repo_root, "Project Geospatial", "data", "panel_sending_states_pga_calibrated.csv")
report_path <- file.path(repo_root, "Project Geospatial", "data", "panel_sending_states_pga_calibrated_match_report.csv")

cat("Reading calibrated flow matrix...\n")
flows <- fread(calibrated_path)

cat("Reading receiver-side PGA template...\n")
exposure <- fread(
  exposure_template_path,
  select = c("state", "municipality", "year", "quarter", "mean_pga", "event")
)
exposure <- unique(exposure)

flows[, state_key := normalize_state(mx_state)]
flows[, muni_key := normalize_text(mx_municipality)]
flows[, muni_key := normalize_municipality(state_key, muni_key)]

exposure[, state_key := normalize_state(state)]
exposure[, muni_key := normalize_text(municipality)]
exposure[, muni_key := normalize_municipality(state_key, muni_key)]

cat("Merging municipality-quarter earthquake exposure onto flows...\n")
flow_exposure <- merge(
  flows,
  exposure[, .(state_key, muni_key, year, quarter, mean_pga, event)],
  by = c("state_key", "muni_key", "year", "quarter"),
  all.x = TRUE,
  sort = FALSE
)

flow_exposure[, exposure_matched := !is.na(mean_pga)]
flow_exposure[is.na(mean_pga), mean_pga := 0]
flow_exposure[is.na(event), event := ""]

cat("Building sender-state quarter panel...\n")
sender_panel <- flow_exposure[
  ,
  .(
    remittances_musd = sum(remittances_musd, na.rm = TRUE),
    matched_flow_musd = sum(remittances_musd[exposure_matched], na.rm = TRUE),
    unmatched_flow_musd = sum(remittances_musd[!exposure_matched], na.rm = TRUE),
    matched_flow_share = sum(remittances_musd[exposure_matched], na.rm = TRUE) / sum(remittances_musd, na.rm = TRUE),
    mean_pga = safe_weighted_mean(mean_pga, remittances_musd),
    max_pga = safe_max(mean_pga),
    exposed_flow_musd = sum(remittances_musd[mean_pga >= 4], na.rm = TRUE),
    exposed_flow_share = sum(remittances_musd[mean_pga >= 4], na.rm = TRUE) / sum(remittances_musd, na.rm = TRUE),
    light_flow_share = sum(remittances_musd[mean_pga >= 4 & mean_pga < 10], na.rm = TRUE) / sum(remittances_musd, na.rm = TRUE),
    moderate_flow_share = sum(remittances_musd[mean_pga >= 10 & mean_pga <= 20], na.rm = TRUE) / sum(remittances_musd, na.rm = TRUE),
    heavy_flow_share = sum(remittances_musd[mean_pga > 20], na.rm = TRUE) / sum(remittances_musd, na.rm = TRUE),
    recipient_municipalities = uniqueN(paste(mx_state, mx_municipality)),
    exposed_recipient_municipalities = uniqueN(paste(mx_state[mean_pga >= 4], mx_municipality[mean_pga >= 4]))
  ),
  by = .(us_state, year, quarter)
]

event_weights <- flow_exposure[
  event != "" & mean_pga > 0,
  .(event_weight = sum(remittances_musd * mean_pga, na.rm = TRUE)),
  by = .(us_state, year, quarter, event)
][order(us_state, year, quarter, -event_weight, event)]

dominant_event <- event_weights[
  ,
  .SD[1],
  by = .(us_state, year, quarter)
][, .(us_state, year, quarter, event, event_weight)]

sender_panel <- merge(
  sender_panel,
  dominant_event,
  by = c("us_state", "year", "quarter"),
  all.x = TRUE,
  sort = FALSE
)

setnames(sender_panel, "event", "dominant_event")
sender_panel[is.na(dominant_event), dominant_event := ""]
sender_panel[is.na(event_weight), event_weight := 0]

sender_panel[, period_date := as.Date(sprintf("%d-%02d-01", year, (quarter - 1L) * 3L + 1L))]
sender_panel[, year_quarter := sprintf("%dQ%d", year, quarter)]
sender_panel[, remittances_kusd := remittances_musd * 1000]
sender_panel[, asinh_remittances := asinh(remittances_kusd)]

setcolorder(
  sender_panel,
  c(
    "period_date",
    "year",
    "quarter",
    "year_quarter",
    "us_state",
    "remittances_musd",
    "remittances_kusd",
    "asinh_remittances",
    "mean_pga",
    "max_pga",
    "dominant_event",
    "event_weight",
    "matched_flow_musd",
    "unmatched_flow_musd",
    "matched_flow_share",
    "exposed_flow_musd",
    "exposed_flow_share",
    "light_flow_share",
    "moderate_flow_share",
    "heavy_flow_share",
    "recipient_municipalities",
    "exposed_recipient_municipalities"
  )
)

match_report <- flow_exposure[
  ,
  .(
    total_flow_musd = sum(remittances_musd, na.rm = TRUE),
    matched_flow_musd = sum(remittances_musd[exposure_matched], na.rm = TRUE),
    matched_flow_share = sum(remittances_musd[exposure_matched], na.rm = TRUE) / sum(remittances_musd, na.rm = TRUE),
    unmatched_recipient_pairs = uniqueN(paste(mx_state[!exposure_matched], mx_municipality[!exposure_matched])),
    total_recipient_pairs = uniqueN(paste(mx_state, mx_municipality))
  ),
  by = .(us_state)
][order(us_state)]

cat("Writing sender-state panel...\n")
fwrite(sender_panel, output_path)

cat("Writing sender-state match report...\n")
fwrite(match_report, report_path)

cat("\nBuild complete.\n")
cat("Output:", output_path, "\n")
cat("Report:", report_path, "\n")
cat("Rows:", nrow(sender_panel), "\n")
cat("Sender states:", uniqueN(sender_panel$us_state), "\n")
