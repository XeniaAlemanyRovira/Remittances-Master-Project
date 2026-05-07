# GENERATING CLEAN PANEL DATA, CLEAN ZUCCA WEIGHTS AND CLEAN YEARLY MIGRATION MATRICES

this is just a copy of whatever we had before, but with some new output and code to obtain the final cleant data matrix. 

specifically, youll find the code to generate clean data in the `1_network_estimation/2_migration_matrix_estimation/Scripts` path. i checked and both `1_migration_matrices` script and `2_weighting_matrices` scripts (both combined generate the final migration matrix w the zucca weights) match and work. 

the cleaned panel generated is to be found in `1_network_estimation/2_migration_matrix_estimation/clean final data`, the changes and decisions taken to obtain this cleant data is found in `1_network_estimation/2_migration_matrix_estimation/validation reports`.

the clean migration weighting matrices are found in `1_network_estimation/2_migration_matrix_estimation/migration_weighting_matrices_2` and applying the resulting zucca weights to the migration data we get the new yearly migration matrices in `1_network_estimation/2_migration_matrix_estimation/yearly_migration_matrices_2`
