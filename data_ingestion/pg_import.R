# Simple CSV to PostgreSQL: Read all locally first, then upload in chunks
rm(list=ls())

# Auto-install and load packages using pacman
if (!require("pacman")) install.packages("pacman")
pacman::p_load(DBI, RPostgres, data.table, readr, dplyr)



# Simple import: read all data locally first, then upload
import_csv_simple <- function(csv_file, table_name, upload_chunk_size = 100000, 
                             enable_compression = TRUE, use_schema_cache = TRUE) {
  
  cat("=== SIMPLE LOCAL-FIRST IMPORT ===\n")
  cat("File:", csv_file, "\n")
  cat("Table:", table_name, "\n\n")
  
  # Step 1.5: Check for schema cache
  schema_cache_file <- file.path(dirname(csv_file), paste0(table_name, ".schema"))
  use_cached_schema <- use_schema_cache && file.exists(schema_cache_file)
  table_exists <- FALSE
  
  if (use_cached_schema) {
    cat("Step 1.5: Loading cached schema...\n")
    cached_schema <- readRDS(schema_cache_file)
    cat("  ✓ Cached schema loaded from:", basename(schema_cache_file), "\n")
    cat("  Original schema from:", cached_schema$first_file, "\n")
    cat("  Schema created:", cached_schema$created, "\n")
    
    # Read CSV with cached column types for speed
    cat("Step 1: Reading CSV file with cached types...\n")
    start_time <- Sys.time()
    
    dt <- tryCatch({
      fread(csv_file, verbose = FALSE, encoding = "UTF-8", colClasses = cached_schema$column_types)
    }, error = function(e) {
      cat("  UTF-8 read with cached types failed, trying fallback...\n")
      tryCatch({
        fread(csv_file, verbose = FALSE, encoding = "Latin-1", colClasses = cached_schema$column_types)
      }, error = function(e2) {
        stop("File incompatible with cached schema. Column mismatch or type error: ", e2$message)
      })
    })
    
    read_time <- Sys.time() - start_time
    cat("✓ Read", nrow(dt), "rows with", ncol(dt), "columns using cached schema")
    cat(" (", round(as.numeric(read_time, units = "secs"), 1), "seconds)\n\n")
    
    # Skip type detection steps - already done
    skip_type_detection <- TRUE
    
  } else {
    cat("Step 1: Reading entire CSV file locally...\n")
    start_time <- Sys.time()
    
    # Try reading with automatic encoding detection first
    dt <- tryCatch({
      fread(csv_file, verbose = FALSE, encoding = "UTF-8")
    }, error = function(e) {
      cat("  UTF-8 read failed, trying Latin-1 encoding...\n")
      tryCatch({
        fread(csv_file, verbose = FALSE, encoding = "Latin-1")
      }, error = function(e2) {
        cat("  Latin-1 read failed, trying with unknown encoding...\n")
        fread(csv_file, verbose = FALSE, encoding = "unknown")
      })
    })
    
    read_time <- Sys.time() - start_time
    cat("✓ Read", nrow(dt), "rows with", ncol(dt), "columns")
    cat(" (", round(as.numeric(read_time, units = "secs"), 1), "seconds)\n\n")
    
    skip_type_detection <- FALSE
  }
  
  if (!skip_type_detection) {
    # Step 2: Let R's type_convert do the smart type detection
    cat("Step 2: Converting types using R's built-in type_convert...\n")
    
    # Convert data.table to tibble for type_convert, then back
    dt_tibble <- as_tibble(dt)
    dt_typed <- type_convert(dt_tibble, 
                            na = c("", "NA", "NULL", "null", "N/A", "#N/A"),
                            trim_ws = TRUE)
    
    # Convert back to data.table
    dt <- as.data.table(dt_typed)
    
    # Step 2.5: Fix problematic time columns (PostgreSQL doesn't like hours >= 24)
    cat("Step 2.5: Fixing time columns for PostgreSQL compatibility...\n")
    
    time_columns <- names(dt)[sapply(dt, function(x) "hms" %in% class(x))]
    
    if(length(time_columns) > 0) {
      cat("  Found time columns:", paste(time_columns, collapse = ", "), "\n")
      
      for(col_name in time_columns) {
        # Convert hms back to character to avoid PostgreSQL time range issues
        dt[[col_name]] <- as.character(dt[[col_name]])
        cat("    ", col_name, ": converted hms -> character\n")
      }
    }
  
  # Show final types
  cat("  Final column types:\n")
  for(col_name in names(dt)) {
    col_class <- class(dt[[col_name]])[1]
    cat("    ", col_name, ":", col_class, "\n")
  }
  
  cat("✓ Type conversion complete\n\n")
  
  # Step 2.7: Clean up encoding issues for PostgreSQL UTF-8 compatibility
  cat("Step 2.7: Cleaning up encoding issues...\n")
  
  char_columns <- names(dt)[sapply(dt, is.character)]
  
  if(length(char_columns) > 0) {
    for(col_name in char_columns) {
      # Clean problematic characters
      dt[[col_name]] <- iconv(dt[[col_name]], to = "UTF-8", sub = "")
      
      # Remove or replace common problematic bytes
 #     dt[[col_name]] <- gsub("[\u00A0\u0080-\u009F]", " ", dt[[col_name]], perl = TRUE)  # Non-breaking spaces and control chars
 #     dt[[col_name]] <- gsub("[\u0000-\u001F\u007F]", "", dt[[col_name]], perl = TRUE)     # Control characters
    }
    cat("  ✓ Cleaned encoding for", length(char_columns), "character columns\n")
  }
  
  cat("✓ Encoding cleanup complete\n\n")
  
  # Save schema cache for future use (only if we did type detection)
  if (!skip_type_detection) {
    column_types_for_cache <- sapply(dt, function(x) {
      if (is.integer(x)) "integer"
      else if (is.numeric(x)) "numeric"  
      else if (is.logical(x)) "logical"
      else "character"
    })
    
    schema_info <- list(
      table_name = table_name,
      created = Sys.time(),
      first_file = basename(csv_file),
      column_types = column_types_for_cache,
      column_classes = sapply(dt, class)
    )
    
    saveRDS(schema_info, schema_cache_file)
    cat("✓ Schema cache saved to:", basename(schema_cache_file), "\n\n")
  }
  }
  
  # Step 3: Connect to database and create table structure
  cat("Step 3: Creating table structure in PostgreSQL...\n")
  
  con <- connect_db()
  
  tryCatch({
    
    # Check if target table already exists
    table_exists <- dbExistsTable(con, table_name)
    
    if (table_exists && use_cached_schema) {
      # Append mode: table exists and we're using cached schema
      cat("  Table", table_name, "exists - will append data\n")
      use_append_mode <- TRUE
      
    } else {
      # Create mode: either table doesn't exist or this is first file
      if (table_exists) {
        cat("  Table", table_name, "exists but this is first file - will replace\n")
      } else {
        cat("  Table", table_name, "does not exist - will create\n")
      }
      use_append_mode <- FALSE
      
      # Create temporary table for atomic replacement
      temp_table_name <- paste0(table_name, "_temp_", format(Sys.time(), "%Y%m%d_%H%M%S"))
      cat("  Creating temp table:", temp_table_name, "\n")
    }
    
    # Convert first row to data.frame to get table structure (needed for both modes)
    first_row_df <- as.data.frame(dt[1,])
    
    if (!use_append_mode) {
      # Create table with compression (only in create mode)
      dbCreateTable(con, temp_table_name, first_row_df)
      
      # Add table compression for space savings
      if(enable_compression) {
      tryCatch({
        # Try TOAST compression first (PostgreSQL built-in)
        dbExecute(con, sprintf("ALTER TABLE %s SET (toast_tuple_target = 128)", temp_table_name))
        cat("  ✓ TOAST compression enabled\n")
        
        # For text-heavy columns, set specific compression
        char_columns <- names(first_row_df)[sapply(first_row_df, is.character)]
        if(length(char_columns) > 0) {
          for(col in char_columns) {
            dbExecute(con, sprintf('ALTER TABLE %s ALTER COLUMN "%s" SET STORAGE EXTENDED', temp_table_name, col))
          }
          cat("  ✓ Extended compression set for", length(char_columns), "text columns\n")
        }
        
        # Try modern compression if available (PostgreSQL 14+)
        tryCatch({
          dbExecute(con, sprintf("ALTER TABLE %s SET ACCESS METHOD heap2", temp_table_name))
          cat("  ✓ Modern heap2 compression enabled\n")
        }, error = function(e) {
          # Try LZ4 compression if available
          tryCatch({
            dbExecute(con, sprintf("ALTER TABLE %s SET (compression = lz4)", temp_table_name))
            cat("  ✓ LZ4 compression enabled\n")
          }, error = function(e2) {
            cat("  ✓ Using standard TOAST compression\n")
          })
        })
        
      }, error = function(e) {
        cat("  Warning: Could not enable compression:", e$message, "\n")
      })
      }
      
      cat("  ✓ Table structure created\n\n")
    } else {
      cat("  ✓ Using existing table structure\n\n")
    }
    
    # Step 4: Upload data in chunks
    cat("Step 4: Uploading data in chunks...\n")
    
    total_rows <- nrow(dt)
    num_chunks <- ceiling(total_rows / upload_chunk_size)
    uploaded_rows <- 0
    
    start_upload_time <- Sys.time()
    
    for(chunk_num in 1:num_chunks) {
      
      # Calculate chunk boundaries
      start_idx <- (chunk_num - 1) * upload_chunk_size + 1
      end_idx <- min(chunk_num * upload_chunk_size, total_rows)
      
      # Extract chunk
      chunk_dt <- dt[start_idx:end_idx, ]
      chunk_df <- as.data.frame(chunk_dt)
      
      # Upload chunk with retry (to temp table or direct to target)
      target_table <- if(use_append_mode) table_name else temp_table_name
      success <- FALSE
      
      for(attempt in 1:3) {
        tryCatch({
          dbAppendTable(con, target_table, chunk_df)
          success <- TRUE
          break
        }, error = function(e) {
          cat("    Chunk", chunk_num, "attempt", attempt, "failed:", e$message, "\n")
          if(attempt == 3) {
            # Clean up on final failure (only if temp table was created)
            if(!use_append_mode) {
              dbExecute(con, sprintf("DROP TABLE IF EXISTS %s", temp_table_name))
            }
            stop(paste("Failed to upload chunk", chunk_num, ":", e$message))
          }
          Sys.sleep(1)
        })
      }
      
      if(success) {
        uploaded_rows <- uploaded_rows + nrow(chunk_df)
        progress_pct <- round((uploaded_rows / total_rows) * 100, 1)
        cat("  Chunk", chunk_num, "/", num_chunks, ": uploaded", nrow(chunk_df), 
            "rows (", progress_pct, "%)\n")
      }
    }
    
    upload_time <- Sys.time() - start_upload_time
    cat("  ✓ All chunks uploaded (", round(as.numeric(upload_time, units = "secs"), 1), "seconds)\n\n")
    
    if (use_append_mode) {
      # Step 5: Append mode - no table replacement needed
      cat("Step 5: Finalizing append operation...\n")
      cat("  ✓ Data appended directly to", table_name, "\n")
      
    } else {
      # Step 5: Atomic table replacement
      cat("Step 5: Finalizing table...\n")
      
      # Check table size before finalizing
      if(enable_compression) {
        tryCatch({
          size_query <- sprintf("SELECT pg_size_pretty(pg_total_relation_size('%s')) as size", temp_table_name)
          table_size <- dbGetQuery(con, size_query)$size[1]
          cat("  Temp table size:", table_size, "\n")
        }, error = function(e) {
          cat("  Could not check table size\n")
        })
      }
      
      dbExecute(con, "BEGIN TRANSACTION")
      tryCatch({
        # Drop original and rename temp
        dbExecute(con, sprintf("DROP TABLE IF EXISTS %s", table_name))
        dbExecute(con, sprintf("ALTER TABLE %s RENAME TO %s", temp_table_name, table_name))
        dbExecute(con, "COMMIT")
      
      }, error = function(e) {
        dbExecute(con, "ROLLBACK")
        dbExecute(con, sprintf("DROP TABLE IF EXISTS %s", temp_table_name))
        stop(paste("Failed to finalize table:", e$message))
      })
    }
    
    # Apply compression to final table (both modes)
    if(enable_compression) {
      cat("  Applying post-load compression...\n")
      tryCatch({
        # Force VACUUM to apply compression
        dbExecute(con, sprintf("VACUUM FULL %s", table_name))
        
        # Check final size
        size_query <- sprintf("SELECT pg_size_pretty(pg_total_relation_size('%s')) as size", table_name)
        final_size <- dbGetQuery(con, size_query)$size[1]
        cat("  Final table size after compression:", final_size, "\n")
        
      }, error = function(e) {
        cat("  Warning: Could not apply post-load compression:", e$message, "\n")
      })
    }
    
    cat("  ✓ Table", table_name, if(use_append_mode) "updated successfully" else "created successfully", "\n")
    
    # Summary
    total_time <- Sys.time() - start_time
    cat("\n=== IMPORT COMPLETE ===\n")
    cat("Rows imported:", uploaded_rows, "\n")
    cat("Total time:", round(as.numeric(total_time, units = "secs"), 1), "seconds\n")
    cat("Upload speed:", round(uploaded_rows / as.numeric(upload_time, units = "secs")), "rows/second\n")
    
    return(uploaded_rows)
    
  }, finally = {
    dbDisconnect(con)
  })
}

