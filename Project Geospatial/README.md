# Remittances and Seismic Shocks in Mexico

**Estimating the causal effect of earthquakes on municipal remittance inflows**

---

## Overview

This project investigates whether seismic events affect remittance flows to Mexican municipalities. We exploit plausibly exogenous variation in earthquake exposure, measured by Peak Ground Acceleration (PGA) and Modified Mercalli Intensity (MMI), across municipalities to identify the effect of natural disasters on international transfers.

The pipeline combines USGS ShakeMap raster products with municipal-level remittance records published by Banxico, linking geocoded shake intensity to administrative boundaries at the *municipio* level.

## Repository Structure

```
.
├── geospatial.qmd          # Remittance data ingestion and cleaning
├── Raster_Earthquake.R             # ShakeMap raster processing and municipal exposure
├── data/
│   ├── shake_result.hdf            # USGS ShakeMap raster (2018-02-16 Pinotepa event)
│   ├── mex_admin0/                 # National boundary shapefile
│   ├── mex_admin1/                 # State boundaries shapefile
│   └── mex_admin2/                 # Municipal boundaries shapefile
├── output/
│   ├── affected_municipalities_pga.gpkg
│   ├── affected_municipalities.gpkg
│   ├── municipality_mean_intensity.gpkg
│   ├── municipality_mean_intensity_pga.gpkg
│   ├── shakemap_pga_mexico.png
│   └── shakemap_intensity_mexico.png
└── README.md
```

## Data Sources

| Source | Description | Coverage |
|--------|-------------|----------|
| **Banxico** | Municipal-level remittance inflows | National, quarterly/monthly |
| **USGS ShakeMap** | Gridded PGA and MMI rasters (HDF5) | Event-specific |
| **INEGI / GADM** | Administrative boundary shapefiles (`admin0`–`admin2`) | National |

## Current Status

### Completed

1. **Remittance data preparation.** Raw Banxico remittance records cleaned and geocoded to the municipal level (`geospatial.qmd`).

2. **Earthquake exposure measurement.** For the 2018-02-16 M7.2 Pinotepa de Don Luis event:
   - Constructed PGA and MMI rasters from the USGS ShakeMap HDF5 product.
   - Computed municipality-level mean PGA and mean MMI via zonal statistics over `mex_admin2` polygons.
   - Identified affected municipalities using an arbitrary intensity cutoff; results stored in `affected_municipalities*.gpkg`.
   - Full municipal exposure surfaces (no cutoff) stored in `municipality_mean_intensity*.gpkg`.
   - Produced diagnostic maps: `shakemap_pga_mexico.png`, `shakemap_intensity_mexico.png`.

> **Note:** All earthquake outputs currently correspond to a single event (M7.2, Pinotepa de Don Luis, Oaxaca, 2018-02-16). The processing script in `Raster_Earthquake.R` is designed to generalise: given any ShakeMap HDF5 file, it reproduces the full pipeline. Scaling to multiple events requires only a loop over event-specific `shake_result.hdf` files.

### Remaining Work

- **Multi-event expansion.** Retrieve and process ShakeMap rasters for additional seismic events using the existing pipeline.
- **Urbanisation controls.** Construct or source a municipal-level urbanisation index (e.g., population density, built-up area share, night-light intensity) to serve as a heterogeneity dimension or control variable.
- **Identification strategy.** Implement a difference-in-differences or comparable quasi-experimental estimator to quantify the causal effect of earthquake exposure on remittance inflows, exploiting cross-municipal variation in shake intensity and pre/post event timing.

## Reproducibility

1. Place the relevant ShakeMap HDF5 file in `data/` and the INEGI/GADM shapefiles in their respective subdirectories.
2. Run `initial_geospatial.qmd` to prepare the remittance panel.
3. Run `Raster_Earthquake.R` to generate exposure measures and maps.

Software requirements: **R** (with `sf`, `terra`, `tidyverse`, and related geospatial packages).
