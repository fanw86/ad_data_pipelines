# Simple runner for folder-based CSV import
rm(list=ls())

source('./data_ingestion/folder_import.R')

# Run the folder import
cat("Starting folder import process...\n\n")

results <- import_data_folder("./data_ingestion/data_to_upload")

cat("\nFolder import process completed.\n")