# WEIGHTING MATRIX ESTIMATION

The scripts contained in this directory produce the cleaned migration matrices and the final migration weighting matrices.

`1_migration_matrices` takes the cleaned state files, reconciles municipality-state pairs to the official INEGI municipality catalog, drops unresolved rows that cannot be matched safely, and produces a migration flow matrix for each year, from 2010 to 2024, with **nominal** flows.

`2_weighting_matrices` takes the files produced by `1_migration_matrices` and produces a migration flow matrix for each year with shares over the total migration flow instead of nominal flows. It also produces the final average weighting matrix, which is an average between all of the other matrices excluding the Covid years, 2020 and 2021.

Outputs produced by `1_migration_matrices`:

- `yearly_migration_matrices_2/`: yearly municipality-by-US-state migration matrices used by the weighting script
- `clean final data/`: final municipality-level panel and clean municipality universe after INEGI reconciliation
- `validation reports/`: municipality mapping report, unresolved report, and dropped-row diagnostics
