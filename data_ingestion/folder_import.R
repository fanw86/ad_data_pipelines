# Folder-based CSV Import: Walk through subfolders and import CSVs
rm(list=ls())

# Load the core import function
source('./data_ingestion/pg_import.R')
source('./data_ingestion/db_config.r')

# Folder walk-through function
import_csv_folder <- function(data_folder, upload_chunk_size = 100000, 
                             enable_compression = TRUE, skip_imported = TRUE) {
  
  cat("=== FOLDER CSV IMPORT ===\n")
  cat("Data folder:", data_folder, "\n")
  cat("Skip already imported:", skip_imported, "\n")
  cat("Compression enabled:", enable_compression, "\n\n")
  
  # Check if folder exists
  if (!dir.exists(data_folder)) {
    stop("Data folder does not exist: ", data_folder)
  }
  
  # Get all subfolders
  subfolders <- list.dirs(data_folder, recursive = FALSE, full.names = TRUE)
  
  if (length(subfolders) == 0) {
    cat("No subfolders found in", data_folder, "\n")
    return(NULL)
  }
  
  cat("Found", length(subfolders), "subfolders:\n")
  for(subfolder in subfolders) {
    cat("  -", basename(subfolder), "\n")
  }
  cat("\n")
  
  # Results tracking
  import_results <- list()
  total_files_processed <- 0
  total_files_skipped <- 0
  total_rows_imported <- 0
  
  overall_start_time <- Sys.time()
  
  # Process each subfolder
  for (subfolder in subfolders) {
    subfolder_name <- basename(subfolder)
    cat("=== Processing subfolder:", subfolder_name, "===\n")
    
    # Get all CSV files in this subfolder
    csv_files <- list.files(subfolder, pattern = "*.csv$", full.names = TRUE, 
                           recursive = FALSE, ignore.case = TRUE)
    
    if (length(csv_files) == 0) {
      cat("  No CSV files found in", subfolder_name, "\n\n")
      next
    }
    
    cat("  Found", length(csv_files), "CSV files\n")
    
    # Check for schema cache to determine if this is first file
    schema_cache_file <- file.path(subfolder, paste0(subfolder_name, ".schema"))
    is_first_file <- !file.exists(schema_cache_file)
    
    # Process each CSV file
    for (csv_file in csv_files) {
      csv_filename <- basename(csv_file)
      
      # Check for success marker if skipping is enabled
      success_marker <- file.path(dirname(csv_file), 
                                 paste0(tools::file_path_sans_ext(csv_filename), ".imported"))
      
      if (skip_imported && file.exists(success_marker)) {
        cat("  SKIPPING", csv_filename, "(already imported)\n")
        total_files_skipped <- total_files_skipped + 1
        
        # Try to read row count from marker
        tryCatch({
          marker_lines <- readLines(success_marker)
          rows_line <- grep("Rows:", marker_lines, value = TRUE)
          if(length(rows_line) > 0) {
            rows_imported <- as.numeric(gsub(".*Rows: ([0-9]+).*", "\\1", rows_line[1]))
            if(!is.na(rows_imported)) {
              total_rows_imported <- total_rows_imported + rows_imported
            }
          }
        }, error = function(e) {
          # Ignore marker reading errors
        })
        
        next
      }
      
      # Determine operation type
      if (is_first_file) {
        cat("  CREATING TABLE", subfolder_name, "from", csv_filename, "\n")
      } else {
        cat("  APPENDING", csv_filename, "to existing table", subfolder_name, "\n")
      }
      
      # Import the CSV file
      file_start_time <- Sys.time()
      
      tryCatch({
        rows_imported <- import_csv_simple(
          csv_file = csv_file,
          table_name = subfolder_name,
          upload_chunk_size = upload_chunk_size,
          enable_compression = enable_compression,
          use_schema_cache = TRUE  # Enable schema caching
        )
        
        file_end_time <- Sys.time()
        file_duration <- as.numeric(difftime(file_end_time, file_start_time, units = "secs"))
        
        # Create success marker
        success_info <- c(
          paste("File:", csv_filename),
          paste("Table:", subfolder_name),
          paste("Rows:", rows_imported),
          paste("Mode:", if(is_first_file) "CREATE" else "APPEND"),
          paste("Schema cache:", if(is_first_file) "CREATED" else "USED"),
          paste("Duration:", sprintf("%.1f seconds", file_duration)),
          paste("Upload chunk size:", upload_chunk_size),
          paste("Compression:", enable_compression),
          paste("Imported at:", Sys.time()),
          paste("Import method: folder_import")
        )
        
        writeLines(success_info, success_marker)
        
        # Track results
        import_results[[paste(subfolder_name, csv_filename, sep = "_")]] <- list(
          success = TRUE,
          rows = rows_imported,
          duration = file_duration,
          file = csv_filename,
          table = subfolder_name
        )
        
        total_files_processed <- total_files_processed + 1
        total_rows_imported <- total_rows_imported + rows_imported
        
        # After first successful import, subsequent files will be appends
        if (is_first_file) {
          is_first_file <- FALSE
        }
        
        cat("  ✓ SUCCESS:", csv_filename, "->", subfolder_name, 
            "(", rows_imported, "rows in", sprintf("%.1f", file_duration), "seconds)\n\n")
        
      }, error = function(e) {
        file_end_time <- Sys.time()
        file_duration <- as.numeric(difftime(file_end_time, file_start_time, units = "secs"))
        
        # Track failed result
        import_results[[paste(subfolder_name, csv_filename, sep = "_")]] <- list(
          success = FALSE,
          error = e$message,
          duration = file_duration,
          file = csv_filename,
          table = subfolder_name
        )
        
        total_files_processed <- total_files_processed + 1
        
        cat("  ✗ FAILED:", csv_filename, "->", subfolder_name, "\n")
        cat("    Error:", e$message, "\n\n")
      })
    }
  }
  
  # Summary
  overall_end_time <- Sys.time()
  total_duration <- as.numeric(difftime(overall_end_time, overall_start_time, units = "secs"))
  
  cat("=== FOLDER IMPORT SUMMARY ===\n")
  cat("Total duration:", sprintf("%.1f", total_duration), "seconds\n")
  cat("Files processed:", total_files_processed, "\n")
  cat("Files skipped:", total_files_skipped, "\n")
  cat("Total rows imported:", total_rows_imported, "\n")
  
  if(total_files_processed > 0) {
    cat("Average speed:", round(total_rows_imported / total_duration), "rows/second\n")
  }
  
  cat("\nDETAILED RESULTS:\n")
  
  success_count <- 0
  failure_count <- 0
  
  for (name in names(import_results)) {
    result <- import_results[[name]]
    if (result$success) {
      status <- "SUCCESS"
      success_count <- success_count + 1
      cat(sprintf("  %s -> %s: %s (%d rows, %.1fs)\n", 
                 result$file, result$table, status, result$rows, result$duration))
    } else {
      status <- "FAILED"
      failure_count <- failure_count + 1
      cat(sprintf("  %s -> %s: %s (%.1fs)\n", 
                 result$file, result$table, status, result$duration))
      cat(sprintf("    Error: %s\n", result$error))
    }
  }
  
  cat("\nSUMMARY BY STATUS:\n")
  cat("  Successful imports:", success_count, "\n")
  cat("  Failed imports:", failure_count, "\n")
  cat("  Skipped (already imported):", total_files_skipped, "\n")
  
  return(import_results)
}

# Schema cache utility functions
clear_schema_cache <- function(subfolder_path, table_name) {
  schema_file <- file.path(subfolder_path, paste0(table_name, ".schema"))
  if(file.exists(schema_file)) {
    file.remove(schema_file)
    cat("Cleared schema cache for", table_name, "\n")
    return(TRUE)
  } else {
    cat("No schema cache found for", table_name, "\n")
    return(FALSE)
  }
}

# Clear all schema caches in a folder
clear_all_schema_caches <- function(data_folder) {
  subfolders <- list.dirs(data_folder, recursive = FALSE, full.names = TRUE)
  cleared_count <- 0
  
  for (subfolder in subfolders) {
    subfolder_name <- basename(subfolder)
    schema_file <- file.path(subfolder, paste0(subfolder_name, ".schema"))
    
    if(file.exists(schema_file)) {
      file.remove(schema_file)
      cat("Cleared schema cache for", subfolder_name, "\n")
      cleared_count <- cleared_count + 1
    }
  }
  
  cat("Cleared", cleared_count, "schema cache files\n")
  return(cleared_count)
}

# View schema cache info
show_schema_cache <- function(subfolder_path, table_name) {
  schema_file <- file.path(subfolder_path, paste0(table_name, ".schema"))
  if(file.exists(schema_file)) {
    schema <- readRDS(schema_file)
    cat("Schema cache for", table_name, ":\n")
    cat("  Created:", schema$created, "\n")
    cat("  First file:", schema$first_file, "\n")
    cat("  Columns:", length(schema$column_types), "\n")
    cat("  Column types:\n")
    for(col_name in names(schema$column_types)) {
      cat("    ", col_name, ":", schema$column_types[[col_name]], "\n")
    }
  } else {
    cat("No schema cache found for", table_name, "\n")
  }
}

# Show all schema caches in a folder
show_all_schema_caches <- function(data_folder) {
  subfolders <- list.dirs(data_folder, recursive = FALSE, full.names = TRUE)
  
  for (subfolder in subfolders) {
    subfolder_name <- basename(subfolder)
    schema_file <- file.path(subfolder, paste0(subfolder_name, ".schema"))
    
    if(file.exists(schema_file)) {
      cat("\n=== Schema Cache:", subfolder_name, "===\n")
      show_schema_cache(subfolder, subfolder_name)
    }
  }
}

# Convenience function for common usage
import_data_folder <- function(data_folder = "./data_ingestion/data_to_upload") {
  import_csv_folder(
    data_folder = data_folder,
    upload_chunk_size = 50000,     # 50K rows per chunk
    enable_compression = TRUE,     # Enable compression
    skip_imported = TRUE           # Skip files with .imported markers
  )
}

# Test/Demo
if(FALSE) {
  # Example usage:
  results <- import_data_folder("./data_ingestion/data_to_upload")
  
  # Or with custom settings:
  results <- import_csv_folder(
    data_folder = "./data_ingestion/data_to_upload",
    upload_chunk_size = 100000,
    enable_compression = TRUE,
    skip_imported = FALSE  # Force re-import all files
  )
}