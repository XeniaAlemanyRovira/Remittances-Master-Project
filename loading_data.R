################################################################################
#                     Code for the master final project: 
# Damage absorption of a macro-level shock in the US through remittances to Mexico.
################################################################################

# INDEX: 
# 1) Load data from the 'matricula consular' (https://ime.gob.mx/estadisticas)
#     Number of 'matriculas' from each US state from each Mexican municipality
#     Relevant Sheets: Alabama_EDOMEX
# 2) Load data from the 'matricula consular' 

# Load relevant libraries:
library(readxl)
library(janitor)
library(purrr)
library(dplyr)
library(tidyr)

# 1) Load data from the 'matricula consular' (number of 'matriculas')-----------
# We begin by only loading data for the total amount of 'matriculas' issued in 
# each US state to Mexicans from each Mexican municipality. 

matricula_2024_alabama <- read_xlsx('data/matricula_consular/Edos_USA_2024/Alabama 2024.xlsx',
                                    sheet = 3) %>% 
                                    clean_names() # loads the variable names without any special character to avoid future problems

states <- c("Alabama", "Alaska", "Arizona", "Arkansas", "California", 
             "Colorado", "Connecticut", "Delaware", "Florida", "Georgia",
             "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
             "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
             "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
             "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
             "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
             "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina",
             "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
             "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming")

years <- c("2024", "2023", "2022", "2021", "2020", "2019", "2018", "2017", 
           "2016", "2015", "2014", "2013", "2012", "2011", "2010")

# Create empty list to store all data
all_data <- list()

for (year in years) {
  for (state in states) {
    file_path <- paste0('data/matricula_consular/Edos_USA_', year, '/', state, ' ', year, '.xlsx')
    
    # Read and store with unique key
    key <- paste0(state, "_", year)
    all_data[[key]] <- read_xlsx(file_path, sheet = 3) %>% 
      clean_names() %>%
      mutate(state = state, year = year)
  }
}

# Combine all into one dataframe
matricula_completa <- bind_rows(all_data)

matricula_completa <- matricula_completa %>% 
  filter(municipio_origen != 'Total')

matricula_completa_wide <- matricula_completa %>%
  pivot_wider(
    names_from = c(year, state),      # Column to get new column names from
    values_from = c(numero_de_matriculas, porcentaje_de_matriculas)        # Column to get values from
  ) 



# NEXT STEPTS: 
# - Check if the xlsx format of the states match (ex: Conneticut's sheet 3 is not correct and there isn't the information needed)
# Complete the loop for all the years (2024 - 2010) by downloading the datasets