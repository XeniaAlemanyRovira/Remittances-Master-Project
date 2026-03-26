# ============================================================================
# Script:  Raster_Earthquake.Rs
# Description:
#   Read a USGS ShakeMap HDF5 file, build georeferenced intensity rasters,
#   mask to Mexico, identify affected municipalities, and produce publication-
#   ready ShakeMap plots (MMI + PGA).
#
# Data requirements (place inside geospatial/earthquakes/):
#   - shake_result.hdf   : ShakeMap HDF5 from USGS
#   - mex_admin0.shp     : Mexico country boundary 
#   - mex_admin1.shp     : Mexico state boundaries
#   - mex_admin2.shp     : Mexico municipality boundaries
#
# Outputs (written to geospatial/output/):
#   - shakemap_all_layers.tif                : multi-band GeoTIFF (all IMTs)
#   - shakemap_intensity.tif                 : single-band intensity GeoTIFF
#   - shakemap_intensity_mexico_masked.tif   : intensity masked to Mexico
#   - shakemap_pga_mexico_masked.tif         : PGA (%g) masked to Mexico
#   - municipality_mean_intensity.gpkg/.csv  : municipalities with avg intensity
#   - municipality_mean_intensity_pga.gpkg/.csv
#   - affected_municipalities.gpkg           : municipalities above MMI threshold
#   - affected_municipalities_pga.gpkg       : municipalities above PGA threshold
#   - shakemap_intensity_mexico.png          : MMI map
#   - shakemap_pga_mexico.png                : PGA map
# ============================================================================

# 0) Project root detection ----------------------------------------------------
# Works whether you source() from RStudio, run via Rscript, or knit.
# Expects the working directory to be the project root (where Thesis.Rproj is).

if (requireNamespace("here", quietly = TRUE)) {
  proj_root <- here::here()
} else if (requireNamespace("rstudioapi", quietly = TRUE) &&
           rstudioapi::isAvailable()) {
  proj_root <- rstudioapi::getActiveProject()
  if (is.null(proj_root)) proj_root <- getwd()
} else {
  proj_root <- getwd()
}
cat("Project root:", proj_root, "\n")

# Directory layout: input

data_dir   <- file.path(proj_root, "Project Geospatial","data", "earthquakes")

if (!dir.exists(data_dir))   stop("Data folder not found: ", data_dir,
                                  "\nPlace ShakeMap HDF5 and shapefiles there.")

# 1) Install / load packages ---------------------------------------------------

if (!requireNamespace("BiocManager", quietly = TRUE)) install.packages("BiocManager")
if (!requireNamespace("rhdf5",       quietly = TRUE)) BiocManager::install("rhdf5", update = FALSE, ask = FALSE)
if (!requireNamespace("terra",       quietly = TRUE)) install.packages("terra")
if (!requireNamespace("jsonlite",    quietly = TRUE)) install.packages("jsonlite")

library(rhdf5)
library(terra)
library(jsonlite)

# 2) Earthquake event parameters -----------------------------------------------
# Edit these for each earthquake you want to map.

event_folder  <- "2014_M7.2_coyuquilla_norte"   # change this when switching earthquakes
epicenter_lon <- -100.972
epicenter_lat <-  17.397
eq_magnitude  <-  7.2
eq_depth_km   <-  24.0
eq_date       <- "2014-04-18"
eq_title      <- "M7.2 Coyuquilla Norte, Mexico"

# 3) Input file paths (relative to project root) -------------------------------

hdf_path        <- file.path(data_dir, event_folder, "shake_result.hdf")
admin0_path     <- file.path(data_dir, "mex_admin0/mex_admin0.shp")
admin1_path     <- file.path(data_dir, "mex_admin1/mex_admin1.shp")
municipios_path <- file.path(data_dir, "mex_admin2/mex_admin2.shp")

for (fp in c(hdf_path, admin0_path, admin1_path, municipios_path)) {
  if (!file.exists(fp)) stop("Required file not found: ", fp)
}

# Directory layout: output

output_dir <- file.path(proj_root, "Project Geospatial", "output", event_folder)

if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

# 4) Utility helpers -----------------------------------------------------------

`%||%` <- function(a, b) if (!is.null(a)) a else b

as_num <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA_real_)
  v <- tryCatch(suppressWarnings(as.numeric(unlist(x, use.names = FALSE))),
                error = function(e) NA_real_)
  if (length(v) == 0) return(NA_real_)
  v[1]
}

is_scalar_finite <- function(x) {
  is.numeric(x) && length(x) == 1 && is.finite(x)
}

# Expand center-based min/max to cell-edge extent when possible
edge_extent <- function(min_center, max_center, n, d) {
  if (is_scalar_finite(min_center) && is_scalar_finite(max_center) &&
      is_scalar_finite(n) && n > 1 && is_scalar_finite(d)) {
    implied <- (max_center - min_center) / (n - 1)
    tol <- max(abs(d), 1e-6) * 0.05
    if (abs(implied - d) <= tol) {
      return(c(min_center - d / 2, max_center + d / 2))
    }
  }
  c(min_center, max_center)
}

# 5) Read HDF5 metadata --------------------------------------------------------

hdf_structure <- h5ls(hdf_path)
cat("\n--- HDF5 structure ---\n")
print(hdf_structure)

info_txt <- h5read(hdf_path, "dictionaries/info.json")
# ShakeMap embeds NaN / Infinity which are invalid JSON — replace with null
info_txt <- gsub("\\bNaN\\b",       "null", info_txt)
info_txt <- gsub("\\bInfinity\\b",  "null", info_txt)
info_txt <- gsub("\\b-Infinity\\b", "null", info_txt)
info     <- fromJSON(info_txt)

map_info <- info$output$map_information

xmin_c  <- as_num(map_info$min$longitude)
xmax_c  <- as_num(map_info$max$longitude)
ymin_c  <- as_num(map_info$min$latitude)
ymax_c  <- as_num(map_info$max$latitude)
nx_meta <- as_num(map_info$number_of_columns %||% map_info$nx %||% map_info$nlon)
ny_meta <- as_num(map_info$number_of_rows    %||% map_info$ny %||% map_info$nlat)
dx      <- as_num(map_info$grid_spacing$longitude %||% map_info$grid_spacing$lon %||% map_info$dx)
dy      <- as_num(map_info$grid_spacing$latitude  %||% map_info$grid_spacing$lat %||% map_info$dy)

cat("Center extent: lon [", xmin_c, ",", xmax_c, "] lat [", ymin_c, ",", ymax_c, "]\n")
cat("Grid meta:     nx=", nx_meta, " ny=", ny_meta, " dx=", dx, " dy=", dy, "\n")

# 6) Read intensity layers ------------------------------------------------------

imt_base <- "/arrays/imts/GREATER_OF_TWO_HORIZONTAL"
imt_rows <- hdf_structure[startsWith(hdf_structure$group, imt_base) &
                            hdf_structure$name == "mean", ]
imt_names <- basename(imt_rows$group)
cat("Available IMT layers:", paste(imt_names, collapse = ", "), "\n")
if (length(imt_names) == 0) stop("No IMT mean layers found in HDF5.")

build_best_oriented_raster <- function(mat, layer_name) {
  if (is.null(mat) || length(dim(mat)) != 2) return(NULL)

  mat <- as.matrix(mat)
  mat[!is.finite(mat)] <- NA
  mat[mat <= -999]     <- NA
  mat[mat > 1e6]       <- NA

  nr <- nrow(mat);  nc <- ncol(mat)
  nx <- if (is_scalar_finite(nx_meta)) nx_meta else nc
  ny <- if (is_scalar_finite(ny_meta)) ny_meta else nr

  x_edge <- edge_extent(xmin_c, xmax_c, nx, dx)
  y_edge <- edge_extent(ymin_c, ymax_c, ny, dy)

  make_r <- function(m) {
    rast(m,
         extent = ext(x_edge[1], x_edge[2], y_edge[1], y_edge[2]),
         crs    = "EPSG:4326")
  }

  tmat <- t(mat)
  candidates <- list(
    as_is    = make_r(mat),
    flipud   = make_r(mat[nrow(mat):1, ]),
    fliplr   = make_r(mat[, ncol(mat):1]),
    t_as_is  = make_r(tmat),
    t_flipud = make_r(tmat[nrow(tmat):1, ]),
    t_fliplr = make_r(tmat[, ncol(tmat):1])
  )

  # Score: pick orientation whose peak value is closest to the epicenter

  score <- function(r) {
    v <- values(r, mat = FALSE)
    if (all(is.na(v))) return(Inf)
    cell_max <- which.max(v)
    xy <- xyFromCell(r, cell_max)
    sqrt((xy[1] - epicenter_lon)^2 + (xy[2] - epicenter_lat)^2)
  }

  s <- sapply(candidates, score)
  best_name <- names(which.min(s))
  best <- candidates[[best_name]]
  names(best) <- gsub("[()]", "_", layer_name)

  cat("  Orientation for", layer_name, ":", best_name,
      "(dist to epicenter =", round(min(s), 3), "deg)\n")
  best
}

read_imt_layer <- function(imt_name) {
  path <- paste0("arrays/imts/GREATER_OF_TWO_HORIZONTAL/", imt_name, "/mean")
  mat  <- tryCatch(h5read(hdf_path, path), error = function(e) NULL)
  if (is.null(mat)) return(NULL)
  build_best_oriented_raster(mat, imt_name)
}

# Primary layer: prefer MMI, fall back to PGA
if ("MMI" %in% imt_names) {
  intensity       <- read_imt_layer("MMI")
  intensity_label <- "Modified Mercalli Intensity (MMI)"
  intensity_unit  <- "MMI"
} else {
  intensity       <- read_imt_layer("PGA")
  intensity_label <- "Peak Ground Acceleration"
  intensity_unit  <- "PGA"
}
if (is.null(intensity)) stop("Could not read main intensity layer (MMI/PGA).")

# Build multi-layer stack

# all_layers     <- Filter(Negate(is.null), lapply(imt_names, read_imt_layer))
# if (length(all_layers) == 0) stop("Could not read any IMT layers.")
# shakemap_stack <- do.call(c, all_layers) # full stack

# PGA + MMI only
pga_layer      <- read_imt_layer("PGA")
pga_layer      <- resample(pga_layer, intensity)  # align to MMI grid
shakemap_stack <- c(intensity, pga_layer)


h5closeAll()

cat("Primary layer:", intensity_label, "\n")
cat("All layers:   ", paste(names(shakemap_stack), collapse = ", "), "\n")

# 7) Load shapefiles -----------------------------------------------------------

mex_admin0 <- project(vect(admin0_path),     crs(intensity))
mex_admin1 <- project(vect(admin1_path),      crs(intensity))
mex_admin2 <- project(vect(municipios_path), crs(intensity))

# 8) Export raw GeoTIFFs -------------------------------------------------------

writeRaster(shakemap_stack,
            filename  = file.path(output_dir, "shakemap_all_layers.tif"),
            overwrite = TRUE, gdal = "COMPRESS=LZW", NAflag = -9999)

writeRaster(intensity,
            filename  = file.path(output_dir, "shakemap_intensity.tif"),
            overwrite = TRUE, gdal = "COMPRESS=LZW", NAflag = -9999)

cat("Raw GeoTIFFs saved.\n")

# 9) Build smoothed visualization raster ---------------------------------------

vis_intensity <- disagg(intensity, fact = 8, method = "bilinear")
vis_intensity <- focal(vis_intensity, w = matrix(1, 3, 3),
                       fun = mean, na.policy = "omit")

rng <- minmax(intensity)
vis_intensity <- clamp(vis_intensity,
                       lower = rng[1, 1], upper = rng[2, 1],
                       values = TRUE)

# Mask to Mexico
intensity_mx  <- mask(crop(intensity,     mex_admin0), mex_admin0)
vis_intensity <- mask(crop(vis_intensity, mex_admin0), mex_admin0)

writeRaster(intensity_mx,
            filename  = file.path(output_dir, "shakemap_intensity_mexico_masked.tif"),
            overwrite = TRUE, gdal = "COMPRESS=LZW", NAflag = -9999)

# 10) Identify affected municipalities -----------------------------------------

affected_threshold <- if (intensity_unit == "MMI") 4 else {
  vtmp <- values(intensity_mx, mat = FALSE)
  vtmp <- vtmp[is.finite(vtmp)]
  as.numeric(stats::quantile(vtmp, probs = 0.75, na.rm = TRUE))
}

affected_raster <- intensity_mx >= affected_threshold

# Municipalities touching the shakemap footprint
touch_tbl <- extract(!is.na(intensity_mx), mex_admin2, fun = max, na.rm = TRUE)
touch_ids <- touch_tbl$ID[!is.na(touch_tbl[, 2]) & touch_tbl[, 2] > 0]
muni_in_footprint <- if (length(touch_ids) > 0) mex_admin2[touch_ids, ] else mex_admin2[0]

# Municipality-level average intensity
mean_tbl <- extract(intensity_mx, mex_admin2, fun = mean, na.rm = TRUE, exact = TRUE)
mex_admin2$mean_intensity <- mean_tbl[, 2]
muni_with_mean <- mex_admin2[!is.na(mex_admin2$mean_intensity), ]

writeVector(mex_admin2,
            filename = file.path(output_dir, "municipality_mean_intensity.gpkg"),
            overwrite = TRUE)

muni_df <- tryCatch(as.data.frame(mex_admin2), error = function(e) values(mex_admin2))
write.csv(muni_df,
          file = file.path(output_dir, "municipality_mean_intensity.csv"),
          row.names = FALSE, na = "")

cat("Municipalities with avg intensity:", nrow(muni_with_mean), "\n")

# Affected municipalities (above threshold)
affect_tbl <- extract(affected_raster, mex_admin2, fun = max, na.rm = TRUE)
affect_ids <- affect_tbl$ID[!is.na(affect_tbl[, 2]) & affect_tbl[, 2] > 0]
affected_muni <- if (length(affect_ids) > 0) mex_admin2[affect_ids, ] else mex_admin2[0]

cat("Municipalities in footprint: ", nrow(muni_in_footprint), "\n")
cat("Affected (", intensity_unit, " >= ", round(affected_threshold, 2), "): ",
    nrow(affected_muni), "\n", sep = "")

if (nrow(affected_muni) > 0) {
  writeVector(affected_muni,
              filename = file.path(output_dir, "affected_municipalities.gpkg"),
              overwrite = TRUE)
}

# 11) Plot: Intensity (MMI) map ------------------------------------------------

states_fp  <- crop(mex_admin1, vis_intensity)
muni_fp    <- crop(mex_admin2, vis_intensity)
country_fp <- crop(mex_admin0, vis_intensity)

quake_cols <- colorRampPalette(c(
  "#1A9850", "#66BD63", "#A6D96A", "#FEE08B", "#FDAE61", "#F46D43", "#D73027"
))(180)

vr <- values(vis_intensity, mat = FALSE)
vr <- vr[is.finite(vr)]
if (length(vr) == 0) stop("No finite values in intensity raster after masking.")

legend_at  <- pretty(range(vr), n = 6)
legend_lab <- format(round(legend_at, 2), trim = TRUE)

epicenter <- vect(
  data.frame(lon = epicenter_lon, lat = epicenter_lat),
  geom = c("lon", "lat"), crs = "EPSG:4326"
)

epi_df <- tryCatch(extract(vis_intensity, epicenter), error = function(e) NULL)
epi_v  <- if (!is.null(epi_df) && ncol(epi_df) >= 2) epi_df[1, 2] else NA_real_
if (is.finite(epi_v) && epi_v < stats::median(vr, na.rm = TRUE)) {
  quake_cols <- rev(quake_cols)
  cat("Palette reversed so epicenter maps to warmer colours.\n")
}

pad <- 0.4
plot_ext <- ext(
  xmin(vis_intensity) - pad, xmax(vis_intensity) + pad,
  ymin(vis_intensity) - pad, ymax(vis_intensity) + pad
)

png(file.path(output_dir, "shakemap_intensity_mexico.png"),
    width = 2400, height = 2000, res = 300)

plot(vis_intensity,
     col = quake_cols, zlim = range(vr), alpha = 0.86,
     legend = TRUE, colNA = NA, ext = plot_ext,
     plg = list(at = legend_at, title = intensity_unit, cex = 0.8),
     axes = TRUE, main = "")

if (nrow(muni_fp) > 0)
  lines(muni_fp,    col = grDevices::adjustcolor("gray55", alpha.f = 0.5), lwd = 0.25)
if (nrow(states_fp) > 0)
  lines(states_fp,  col = "gray40", lwd = 0.45)
if (nrow(country_fp) > 0)
  lines(country_fp, col = "black",  lwd = 1.2)
if (nrow(affected_muni) > 0) {
  affected_muni_fp <- crop(affected_muni, vis_intensity)
  if (nrow(affected_muni_fp) > 0) lines(affected_muni_fp, col = "#111111", lwd = 0.9)
}

points(epicenter, pch = 24, col = "black", bg = "yellow", cex = 1.8, lwd = 2)

title(main = paste0(eq_title, "\n", eq_date), cex.main = 1.0, font.main = 2)
sbar(d = 100, xy = "bottomleft", type = "bar", below = "km", divs = 2, cex = 0.6)

legend("topright",
       legend = c(
         paste0("Epicenter (", epicenter_lat, "N, ", abs(epicenter_lon), "W, ", eq_depth_km, " km)"),
         paste0("Magnitude: M", eq_magnitude),
         paste0("Affected municipalities: ", nrow(affected_muni)),
         paste0("Threshold: ", intensity_unit, " >= ", round(affected_threshold, 2))
       ),
       pch = c(24, NA, NA, NA), col = c("black", NA, NA, NA),
       pt.bg = c("yellow", NA, NA, NA), pt.cex = c(1.2, NA, NA, NA),
       cex = 0.7, bg = "white", box.lwd = 0.5)

dev.off()
cat("MMI map saved to:", file.path(output_dir, "shakemap_intensity_mexico.png"), "\n")

# 12) PGA section (auto-converted to %g) --------------------------------------

if ("PGA" %in% imt_names) {
  pga_raw <- read_imt_layer("PGA")

  if (!is.null(pga_raw)) {
    pga_raw_mx   <- mask(crop(pga_raw, mex_admin0), mex_admin0)
    pga_vals_raw <- values(pga_raw_mx, mat = FALSE)
    pga_vals_raw <- pga_vals_raw[is.finite(pga_vals_raw)]

    if (length(pga_vals_raw) == 0) {
      cat("PGA layer has no finite values after masking.\n")
    } else {
      q05 <- as.numeric(stats::quantile(pga_vals_raw, 0.05, na.rm = TRUE))
      q95 <- as.numeric(stats::quantile(pga_vals_raw, 0.95, na.rm = TRUE))

      # Automatic unit detection → always convert to %g
      if (q05 < 0 && q95 <= 5) {
        pga_mx         <- exp(pga_raw_mx) * 100
        pga_conversion <- "ln(g) -> %g"
      } else if (q95 <= 3) {
        pga_mx         <- pga_raw_mx * 100
        pga_conversion <- "g -> %g"
      } else {
        pga_mx         <- pga_raw_mx
        pga_conversion <- "already %g"
      }
      cat("PGA conversion: ", pga_conversion, "\n", sep = "")

      writeRaster(pga_mx,
                  filename  = file.path(output_dir, "shakemap_pga_mexico_masked.tif"),
                  overwrite = TRUE, gdal = "COMPRESS=LZW", NAflag = -9999)

      # Smooth for display only
      vis_pga <- disagg(pga_mx, fact = 8, method = "bilinear")
      vis_pga <- focal(vis_pga, w = matrix(1, 3, 3), fun = mean, na.policy = "omit")
      pga_rng <- minmax(pga_mx)
      vis_pga <- clamp(vis_pga, lower = pga_rng[1, 1], upper = pga_rng[2, 1], values = TRUE)

      # Municipality average PGA (%g)
      pga_mean_tbl <- extract(pga_mx, mex_admin2, fun = mean, na.rm = TRUE, exact = TRUE)
      mex_admin2$mean_pga <- pga_mean_tbl[, 2]
      cat("Municipalities with avg PGA:", sum(is.finite(mex_admin2$mean_pga)), "\n")

      muni_df_all <- tryCatch(as.data.frame(mex_admin2), error = function(e) values(mex_admin2))
      write.csv(muni_df_all,
                file = file.path(output_dir, "municipality_mean_intensity_pga.csv"),
                row.names = FALSE, na = "")
      writeVector(mex_admin2,
                  filename = file.path(output_dir, "municipality_mean_intensity_pga.gpkg"),
                  overwrite = TRUE)

      pga_threshold <- 10  # %g
      pga_affected_raster <- pga_mx >= pga_threshold
      pga_aff_tbl <- extract(pga_affected_raster, mex_admin2, fun = max, na.rm = TRUE)
      pga_aff_ids <- pga_aff_tbl$ID[!is.na(pga_aff_tbl[, 2]) & pga_aff_tbl[, 2] > 0]
      pga_affected_muni <- if (length(pga_aff_ids) > 0) mex_admin2[pga_aff_ids, ] else mex_admin2[0]

      if (nrow(pga_affected_muni) > 0) {
        writeVector(pga_affected_muni,
                    filename = file.path(output_dir, "affected_municipalities_pga.gpkg"),
                    overwrite = TRUE)
      }
      cat("Affected (PGA >= ", round(pga_threshold, 2), " %g): ",
          nrow(pga_affected_muni), "\n", sep = "")

      # --- PGA plot ---
      pga_plot_vals <- values(vis_pga, mat = FALSE)
      pga_plot_vals <- pga_plot_vals[is.finite(pga_plot_vals)]

      if (length(pga_plot_vals) > 0) {
        pga_cols <- colorRampPalette(c(
          "#1A9850", "#66BD63", "#A6D96A", "#FEE08B", "#FDAE61", "#F46D43", "#D73027"
        ))(180)

        pga_legend_at <- pretty(range(pga_plot_vals), n = 6)

        epi_pga   <- tryCatch(extract(vis_pga, epicenter), error = function(e) NULL)
        epi_pga_v <- if (!is.null(epi_pga) && ncol(epi_pga) >= 2) epi_pga[1, 2] else NA_real_
        if (is.finite(epi_pga_v) && epi_pga_v < stats::median(pga_plot_vals, na.rm = TRUE)) {
          pga_cols <- rev(pga_cols)
          cat("PGA palette reversed so epicenter maps to warmer colours.\n")
        }

        pga_ext     <- ext(vis_pga)
        states_pga  <- crop(mex_admin1, vis_pga)
        muni_pga    <- crop(mex_admin2, vis_pga)
        country_pga <- crop(mex_admin0, vis_pga)

        png(file.path(output_dir, "shakemap_pga_mexico.png"),
            width = 2400, height = 2000, res = 300)

        plot(vis_pga,
             col = pga_cols, zlim = range(pga_plot_vals), alpha = 0.88,
             legend = TRUE, colNA = NA, ext = pga_ext,
             plg = list(at = pga_legend_at, title = "PGA (%g)", cex = 0.8),
             axes = TRUE, main = "")

        if (nrow(muni_pga) > 0)
          lines(muni_pga,    col = grDevices::adjustcolor("gray55", alpha.f = 0.45), lwd = 0.22)
        if (nrow(states_pga) > 0)
          lines(states_pga,  col = "gray40", lwd = 0.45)
        if (nrow(country_pga) > 0)
          lines(country_pga, col = "black",  lwd = 1.2)
        if (nrow(pga_affected_muni) > 0) {
          pga_affected_fp <- crop(pga_affected_muni, vis_pga)
          if (nrow(pga_affected_fp) > 0) lines(pga_affected_fp, col = "#111111", lwd = 0.9)
        }

        points(epicenter, pch = 24, col = "#c68a00", bg = "#ffc857", cex = 1.8, lwd = 2)

        title(main = paste0(eq_title, "\n", eq_date, " – PGA (%g)"),
              cex.main = 1.0, font.main = 2)
        sbar(d = 100, xy = "bottomleft", type = "bar", below = "km", divs = 2, cex = 0.6)

        legend("topright",
               legend = c(
                 paste0("Epicenter (", epicenter_lat, "N, ", abs(epicenter_lon), "W, ", eq_depth_km, " km)"),
                 paste0("Magnitude: M", eq_magnitude),
                 paste0("Affected municipalities: ", nrow(pga_affected_muni)),
                 paste0("Threshold: PGA >= ", round(pga_threshold, 2), " %g"),
                 paste0("Conversion: ", pga_conversion)
               ),
               pch = c(24, NA, NA, NA, NA), col = c("#c68a00", NA, NA, NA, NA),
               pt.bg = c("#ffc857", NA, NA, NA, NA), pt.cex = c(1.2, NA, NA, NA, NA),
               cex = 0.7, bg = "white", box.lwd = 0.5)

        dev.off()
        cat("PGA map saved to:", file.path(output_dir, "shakemap_pga_mexico.png"), "\n")
      }
    }
  } else {
    cat("PGA layer listed but could not be read from HDF5.\n")
  }
} else {
  cat("PGA layer not available in IMTs.\n")
}


