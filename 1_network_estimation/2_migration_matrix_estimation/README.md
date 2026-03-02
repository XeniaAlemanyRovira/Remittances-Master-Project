# WEIGHTING MATRIX ESTIMATION

The scripts contained in this directory produce the final migration weighting matrices

1_migration_matrices takes the files from Data_clean and produces a migration flow matrix for each year, from 2010 to 2024, with **nominal** flows.

2_weighting_matrices takes the files produced by 1_migration_matrices and produces a migration flow matrix for each year with shares over the total migration flow instead of nominal flows. It also produces the final average weighting matrix, which is an average between all of the other matrices excluding the Covid years, 2020 and 2021.