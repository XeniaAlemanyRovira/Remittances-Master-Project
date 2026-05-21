# Load libraries
library(tidyverse)
library(readxl)
library(fixest)

# Load data
shares <- read_xlsx("Data/final-data/weighting_matrix_rows.xlsx")
remit  <- read.csv("Data/final-data/municipality_inflows.csv")

# Express remittances in unit dollars
remit <- remit %>%
  mutate(remittances = remittances_musd * 1000000) %>%
  select(-remittances_musd) %>%
  # Drop useless columns
  select(-c(year, quarter))

# Keep only the Florida column
florida_weights <- shares %>%
  select(mx_state, mx_municipality, Florida)

# Thresholds for top 10% exposure and bottom 10% exposed municipalities
threshold_high <- quantile(florida_weights$Florida, probs = 0.90, na.rm = TRUE)
threshold_low  <- quantile(florida_weights$Florida, probs = 0.10, na.rm = TRUE)

# Create the dataset that classifies based on treatment
municipality_groups <- florida_weights %>%
  mutate(
    group = case_when(
      Florida >= threshold_high ~ "Treated",
      Florida <= threshold_low  ~ "Control",
      TRUE                      ~ "Unused"
    )
  )

# Merge the previous dataset with the remittance one to get the final panel
final_panel <- remit %>%
  inner_join(municipality_groups, by = c("mx_state", "mx_municipality")) %>%
  filter(group != "Unused") %>%
  mutate(period_date = as.Date(period_date))

# Identify the shock date and create a shock index
shock_date <- as.Date("2022-10-01")
unique_quarters <- sort(unique(final_panel$period_date))
shock_index <- which(unique_quarters == shock_date)

# Create the final panel restricting to -8 and +8 quarters from the shock
final_panel <- final_panel %>%
  mutate(
    rel_quarter = match(period_date, unique_quarters) - shock_index
  ) %>%
  filter(rel_quarter >= -8 & rel_quarter <= 8)

# Aggregate total raw remittances by group and quarter, taking logs
group_totals <- final_panel %>%
  group_by(group, period_date) %>%
  summarise(
    log_total_remittances = log(sum(remittances, na.rm = TRUE)),
    .groups = "drop"
  )

# Plot the time series of treated municipalities against control 
ggplot(group_totals, aes(x = period_date, y = log_total_remittances, color = group)) +
  geom_line(linewidth = 1, linetype = "solid") +
  geom_point(size = 1.5) +
  # Add the Hurricane Ian marker
  geom_vline(xintercept = shock_date, color = "black", linetype = "dashed", linewidth = 0.5) +
  theme_minimal(base_size = 14) +
  theme(
    legend.position = "bottom",
    panel.grid.minor = element_blank(),
    plot.title = element_text(face = "bold")
  ) +
  # Labels
  labs(
    title = "Total Nominal Remittances: Treated vs. Control Group",
    subtitle = "Comparing Top 10% (Treated) vs. Bottom 10% (Control) Florida-Exposed Municipalities",
    x = "Quarter",
    y = "Total Quarterly Inflows (Log Scale)",
    color = "Group:"
  )

# Prepare dataset for twfe
reg_data <- final_panel %>%
  mutate(
    unit_id = paste(mx_state, mx_municipality, sep = "_"),
    is_treated = ifelse(group == "Treated", 1, 0),
    is_post = ifelse(period_date >= shock_date, 1, 0),
    treated_post = is_treated * is_post,
    log_remit = log(remittances + 1)
  )

# TWFE model
twfe_static <- feols(
  log_remit ~ treated_post | unit_id + period_date, 
  data = reg_data,
  cluster = ~unit_id
)

print("Static Two-Way Fixed Effects")
summary(twfe_static)

# Dynamic event study model
twfe_dynamic <- feols(
  log_remit ~ i(period_date, is_treated, ref = as.Date("2022-07-01")) | unit_id + period_date,
  data = reg_data,
  cluster = ~unit_id
)

# Event study plot
iplot(twfe_dynamic, 
      main = "Event Study: Impact of Hurricane Ian on Florida-Exposed Municipalities",
      xlab = "Quarter", 
      ylab = "Log Difference in Remittances",
      ylim = c(-5, 5))