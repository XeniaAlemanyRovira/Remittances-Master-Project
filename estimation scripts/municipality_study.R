################################################################################
# DIFF-IN-DIFF ESTIMATION OF MUNICIPALITY REMITTANCE INFLOWS
################################################################################

# Load libraries
library(tidyverse)
library(readxl)
library(fixest)

# Loading the data
migration <- read_xlsx("Data/final-data/weighting_matrix_rows.xlsx")
remit  <- read.csv("Data/final-data/municipality_inflows.csv")

# Converting remittance data to unit dollars and formatting date variable
remit_clean <- remit %>%
  mutate(
    remittances = remittances_musd * 1000000,
    period_date = as.Date(period_date)
  ) %>%
  select(-remittances_musd, -c(year, quarter))

# Extracting the Florida exposure variable and merging into a final panel
florida_weights <- migration %>%
  select(mx_state, mx_municipality, Florida)
master_panel <- remit_clean %>%
  inner_join(florida_weights, by = c("mx_state", "mx_municipality"))

# Restrict timeline 
shock_date <- as.Date("2022-10-01")
unique_quarters <- sort(unique(master_panel$period_date))
shock_index <- which(unique_quarters == shock_date)

# Restrict timeline to -8/+8 quarters
master_panel <- master_panel %>%
  mutate(
    rel_quarter = match(period_date, unique_quarters) - shock_index
  ) %>%
  filter(rel_quarter >= -8 & rel_quarter <= 8)

# Create IDs and convert florida weights to percentage points
analysis_data <- master_panel %>%
  mutate(
    unit_id = paste(mx_state, mx_municipality, sep = "_"),
    florida_pct = Florida * 100
  )

################################################################################
# ESTIMATION
################################################################################ 

# OLS DiD without Mexican state-time fixed effects
ols_no_state_fe <- feols(
  log(remittances + 1) ~ i(period_date, florida_pct, ref = as.Date("2022-07-01")) | 
    unit_id + period_date,
  data = analysis_data,
  cluster = ~unit_id
)

# OLS DiD with Mexican state-time fixed effects
ols_with_state_fe <- feols(
  log(remittances + 1) ~ i(period_date, florida_pct, ref = as.Date("2022-07-01")) | 
    unit_id + mx_state^period_date,
  data = analysis_data,
  cluster = ~unit_id
)

# PPML DiD with Mexican state-time fixed effects
ppml_with_state_fe <- fepois(
  remittances ~ i(period_date, florida_pct, ref = as.Date("2022-07-01")) | 
    unit_id + mx_state^period_date,
  data = analysis_data,
  cluster = ~unit_id
)

# Table to show results
etable(
  ols_no_state_fe, ols_with_state_fe, ppml_with_state_fe,
  headers = c("OLS: No State-Time FE", "OLS: With State-Time FE", "PPML: With State-Time FE")
)

################################################################################
# VISUALIZATION
################################################################################

# Displaying 3 plots side-by-side
par(mfrow = c(1, 3))

# OLS without Mexican state-time FE
iplot(
  ols_no_state_fe,
  main = "OLS: No State-Time FE",
  xlab = "Quarter", 
  ylab = "Coefficient Effect",
  ylim = c(-0.15, 0.15),
  ref.line = 9,
  ref.line.par = list(col = "firebrick3", lty = 2) 
)

# OLS with Mexican state-time FE
iplot(
  ols_with_state_fe,
  main = "OLS: With State-Time FE",
  xlab = "Quarter", 
  ylab = "Coefficient Effect",
  ylim = c(-0.15, 0.15),
  ref.line = 9,
  ref.line.par = list(col = "firebrick3", lty = 2)
)

# PPML with Mexican state-time FE
iplot(
  ppml_with_state_fe,
  main = "PPML: State-Time FE",
  xlab = "Quarter", 
  ylab = "Coefficient Effect",
  ylim = c(-0.15, 0.15),
  ref.line = 9,
  ref.line.par = list(col = "firebrick3", lty = 2)
)

# Resetting the plotting layout back to standard
par(mfrow = c(1, 1))