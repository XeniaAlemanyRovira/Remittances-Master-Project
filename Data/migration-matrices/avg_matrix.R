# Load the required packages
library(tidyverse)
library(readxl)
library(writexl)

# Select years
years <- 2014:2021
matrix_list <- list()

# Load all matrices
for (year in years) {
  file_name <- paste0("Data/migration-matrices/WEIGHTING_MATRIX_", year, ".xlsx")
  
  if (file.exists(file_name)) {
    matrix_list[[as.character(year)]] <- read_xlsx(file_name)
  } else {
    warning(paste("File missing:", file_name))
  }
}

# Detach the ID columns to keep only the numeric part
id_cols <- matrix_list[[1]][, c("mx_state", "mx_municipality")]

# Isolate just the numeric columns for all years in the list
numeric_list <- lapply(matrix_list, function(df) select(df, -mx_state, -mx_municipality))

# Add all the matrices together
sum_numeric <- Reduce("+", numeric_list)

# Divide by the number of years to get the average
avg_numeric <- sum_numeric / length(numeric_list)

# Recombine with the mx_state and mx_municipality columns
avg_matrix <- cbind(id_cols, avg_numeric)

# Make matrix with rows summing to 1
row_totals <- rowSums(avg_numeric, na.rm = TRUE)
clean_rows_num <- avg_numeric / ifelse(row_totals == 0, 1, row_totals) 
row_matrix <- cbind(id_cols, clean_rows_num)

# Make matrix with columns summing to 1
col_totals <- colSums(avg_numeric, na.rm = TRUE)
clean_cols_num <- sweep(avg_numeric, MARGIN = 2, STATS = col_totals, FUN = "/")
col_matrix <- cbind(id_cols, clean_cols_num)

# Sanity checks
print(paste("NAs in Average Matrix:", sum(is.na(avg_matrix))))
print(paste("NAs in Row Matrix:", sum(is.na(row_matrix))))
print(paste("NAs in Col Matrix:", sum(is.na(col_matrix))))

# Check that rows actually sum to 1
sanity_check_rows <- row_matrix %>% mutate(row_total = rowSums(across(-c(mx_state, mx_municipality))))
sanity_check_rows

# Check that columns actually sum to 1
sanity_check_cols <- colSums(select(col_matrix, -mx_state, -mx_municipality))
sanity_check_cols

# Export the matrices
write_xlsx(avg_matrix, "Data/final-data/avg_migration_matrix.xlsx")
write_xlsx(row_matrix, "Data/final-data/migration_matrix_rows.xlsx")
write_xlsx(col_matrix, "Data/final-data/migration_matrix_cols.xlsx")