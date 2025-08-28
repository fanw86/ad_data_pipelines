source('./data_ingestion/pg_import.R')

# Define connection parameters separately
source('./data_ingestion/db_config.r')

# Function to import all CSV files from subfolders into tables named after subfolder
import_csv_from_subfolders <- function(data_folder, connection_params, schema = "public") {
  
  # Get all subfolders in the data directory
  subfolders <- list.dirs(data_folder, recursive = FALSE, full.names = TRUE)
  
  if (length(subfolders) == 0) {
    cat("No subfolders found in", data_folder, "\n")
    return(NULL)
  }
  
  results <- list()
  
  for (subfolder in subfolders) {
    subfolder_name <- basename(subfolder)
    cat("Processing subfolder:", subfolder_name, "\n")
    
    # Get all CSV files in this subfolder
    csv_files <- list.files(subfolder, pattern = "*.csv$", full.names = TRUE, recursive = FALSE)
    
    if (length(csv_files) == 0) {
      cat("  No CSV files found in", subfolder_name, "\n")
      next
    }
    
    # Filter out already imported files
    csv_files_to_process <- c()
    for (csv_file in csv_files) {
      csv_filename <- basename(csv_file)
      success_marker <- file.path(subfolder, paste0(tools::file_path_sans_ext(csv_filename), ".imported"))
      
      if (file.exists(success_marker)) {
        cat("    Skipping", csv_filename, "(already imported)\n")
      } else {
        csv_files_to_process <- c(csv_files_to_process, csv_file)
      }
    }
    
    if (length(csv_files_to_process) == 0) {
      cat("  All CSV files already imported in", subfolder_name, "\n")
      next
    }
    
    cat("  Found", length(csv_files_to_process), "new CSV files to process\n")
    
    # Process each CSV file in the subfolder
    for (csv_file in csv_files_to_process) {
      csv_filename <- basename(csv_file)
      success_marker <- file.path(dirname(csv_file), paste0(tools::file_path_sans_ext(csv_filename), ".imported"))
      
      cat("    Importing", csv_filename, "into table", subfolder_name, "\n")
      
      tryCatch({
        result <- csv_to_postgres(
          csv_file = csv_file,
          table_name = subfolder_name,
          connection_params = connection_params,
          schema = schema,
          optimize_performance = FALSE,
          encoding = "UTF-8",
          decimal_sep = ".",
          thousands_sep = "",
          chunk_size = 10000,
          dedupe = TRUE,
          append_data = TRUE  # Append data if table already exists
        )
        
        # Create success marker file
        writeLines(
          c(
            paste("File:", csv_filename),
            paste("Table:", subfolder_name),
            paste("Schema:", schema),
            paste("Imported at:", Sys.time()),
            paste("Rows processed:", ifelse(is.list(result) && !is.null(result$rows_processed), result$rows_processed, "Unknown"))
          ),
          success_marker
        )
        
        results[[paste(subfolder_name, csv_filename, sep = "_")]] <- result
        cat("    Successfully imported", csv_filename, "and created marker file\n")
        
      }, error = function(e) {
        cat("    Error importing", csv_filename, ":", e$message, "\n")
        results[[paste(subfolder_name, csv_filename, sep = "_")]] <- list(success = FALSE, error = e$message)
        # Don't create marker file on failure
      })
    }
  }
  
  return(results)
}

# Set the data folder path (adjust as needed)
data_folder <- "./data_ingestion/data_to_upload"

# Run the import process
cat("Starting CSV import process from subfolders in:", data_folder, "\n")
import_results <- import_csv_from_subfolders(data_folder, db_config, schema = "public")

# Print summary
if (!is.null(import_results)) {
  cat("\n=== IMPORT SUMMARY ===\n")
  for (name in names(import_results)) {
    if (is.list(import_results[[name]]) && !is.null(import_results[[name]]$success)) {
      status <- if (import_results[[name]]$success) "SUCCESS" else "FAILED"
      cat(name, ":", status, "\n")
      if (!import_results[[name]]$success) {
        cat("  Error:", import_results[[name]]$error, "\n")
      }
    }
  }
}