# Load libraries
library(tidyverse)
library(fixest)

florida_study_output_dir <- file.path(
  "Shocks",
  "Ian hurricane",
  "outputs",
  "florida_study_outputs"
)
dir.create(florida_study_output_dir, showWarnings = FALSE, recursive = TRUE)

# Load data
df_remit <- read.csv("Data/us_state_outflows.csv", stringsAsFactors = FALSE)
bls_cleaned <- read.csv("Data/bls_structural_shares_2019.csv", stringsAsFactors = FALSE)

# Prepare final panel
clean_panel_all_states <- df_remit %>%
  # Filter out unidentifiable rows 
  filter(us_state != "No Identificado") %>%
  
  # Create timeline markers centered around the Hurricane Ian shock (2022 Q4)
  mutate(is_florida = ifelse(us_state == "Florida", 1, 0)) %>%
  mutate(time_period = paste(year, quarter, sep = "_Q")) %>%
  mutate(quarter_id = (year * 4) + quarter) %>%
  mutate(event_time = quarter_id - ((2022 * 4) + 4)) %>%
  
  # Convert to unit USD and take logs
  mutate(remittances_usd = remittances_musd * 1000000) %>%
  mutate(log_remittances = log(remittances_usd)) %>% 
  
  # Limit windows
  filter(event_time >= -8 & event_time <= 8) %>%
  
  # Join the final panel
  left_join(bls_cleaned, by = "us_state")

# Define high migration states
high_migration_states <- c(
  "Florida", 
  "California", 
  "Texas", 
  "Illinois", 
  "Arizona", 
  "Colorado", 
  "Georgia", 
  "Carolina Del Norte", 
  "Nevada", 
  "Nuevo Mexico", 
  "Nueva York"
)

# Restrict the dataset to high migration states
clean_panel_restricted <- clean_panel_all_states %>%
  filter(us_state %in% high_migration_states)

# Baseline OLS model (no covariates)
event_study_restricted <- feols(
  log_remittances ~ i(event_time, is_florida, ref = -1) | 
    us_state + time_period,
  data = clean_panel_restricted,
  vcov = ~us_state
)

print(summary(event_study_restricted))

png(
  file.path(florida_study_output_dir, "01_ols_event_study_no_covariates.png"),
  width = 2400,
  height = 1500,
  res = 300
)
iplot(event_study_restricted, 
      main = "OLS event study, no covariates",
      xlab = "Quarters Relative to Hurricane Ian (0 = 2022 Q4)",
      ylab = "Estimated Impact on Remittances",
      ylim = c(-0.5, 0.5),
      ref.line = 0,
      pt.join = TRUE)
dev.off()

iplot(event_study_restricted,
      main = "OLS event study, no covariates",
      xlab = "Quarters Relative to Hurricane Ian (0 = 2022 Q4)",
      ylab = "Estimated Impact on Remittances",
      ylim = c(-0.5, 0.5),
      ref.line = 0,
      pt.join = TRUE)

# Augmented OLS model (covariates included)
event_study_restricted <- feols(
  log_remittances ~ i(event_time, is_florida, ref = -1) | 
    us_state + time_period[construction_share_2019] + time_period[hospitality_share_2019], 
  data = clean_panel_restricted,
  vcov = ~us_state
)

print(summary(event_study_restricted))

png(
  file.path(florida_study_output_dir, "02_ols_event_study_covariates.png"),
  width = 2400,
  height = 1500,
  res = 300
)
iplot(event_study_restricted, 
      main = "OLS event study, covariates included",
      xlab = "Quarters Relative to Hurricane Ian (0 = 2022 Q4)",
      ylab = "Estimated Impact on Remittances",
      ylim = c(-0.5, 0.5),
      ref.line = 0,
      pt.join = TRUE)
dev.off()

iplot(event_study_restricted,
      main = "OLS event study, covariates included",
      xlab = "Quarters Relative to Hurricane Ian (0 = 2022 Q4)",
      ylab = "Estimated Impact on Remittances",
      ylim = c(-0.5, 0.5),
      ref.line = 0,
      pt.join = TRUE)

# Baseline PPML model (no covariates)
event_study_restricted <- fepois(
  remittances_usd ~ i(event_time, is_florida, ref = -1) | 
    us_state + time_period,
  data = clean_panel_restricted,
  vcov = ~us_state
)

print(summary(event_study_restricted))

png(
  file.path(florida_study_output_dir, "03_ppml_event_study_no_covariates.png"),
  width = 2400,
  height = 1500,
  res = 300
)
iplot(event_study_restricted, 
      main = "PPML event study, no covariates",
      xlab = "Quarters Relative to Hurricane Ian (0 = 2022 Q4)",
      ylab = "Estimated Impact on Remittances",
      ylim = c(-0.5, 0.5),
      ref.line = 0,
      pt.join = TRUE)
dev.off()

iplot(event_study_restricted,
      main = "PPML event study, no covariates",
      xlab = "Quarters Relative to Hurricane Ian (0 = 2022 Q4)",
      ylab = "Estimated Impact on Remittances",
      ylim = c(-0.5, 0.5),
      ref.line = 0,
      pt.join = TRUE)

# Augmented PPML model (covariates included)
event_study_restricted <- fepois(
  remittances_usd ~ i(event_time, is_florida, ref = -1) | 
    us_state + time_period[construction_share_2019] + time_period[hospitality_share_2019], 
  data = clean_panel_restricted,
  vcov = ~us_state
)

print(summary(event_study_restricted))

png(
  file.path(florida_study_output_dir, "04_ppml_event_study_covariates.png"),
  width = 2400,
  height = 1500,
  res = 300
)
iplot(event_study_restricted, 
      main = "PPML event study, covariates included",
      xlab = "Quarters Relative to Hurricane Ian (0 = 2022 Q4)",
      ylab = "Estimated Impact on Remittances",
      ylim = c(-0.5, 0.5),
      ref.line = 0,
      pt.join = TRUE)
dev.off()

iplot(event_study_restricted,
      main = "PPML event study, covariates included",
      xlab = "Quarters Relative to Hurricane Ian (0 = 2022 Q4)",
      ylab = "Estimated Impact on Remittances",
      ylim = c(-0.5, 0.5),
      ref.line = 0,
      pt.join = TRUE)
