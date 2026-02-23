# Data cleaning for building the network

The scripts contained in the *Utility_scripts* folder take the raw data in the *Data* folder and generate xlsx files in the *Data_clean* folder, standardizing both name and format.

To replicate:

-   First, run *1_folder_cleaning.R*. This script generates the *Data_clean* folder from the *Data* folder. It eliminates the double nesting, corrects typos, and standardizes the name of the folders.
-   Then, run *2_renaming_pulling_xlsx.R*. This scripts pulls the xlsx files, corrects typos and standardizes their names, and pulls the correct sheet from each one.
-   Finally, run *3_format_xlsx.R*. This script edits the xlsx files so that they are all formatted in the same way.
