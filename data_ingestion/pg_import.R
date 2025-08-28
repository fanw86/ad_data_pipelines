# ==============================================================================
# ROBUST CSV TO POSTGRESQL IMPORT FUNCTION
# ==============================================================================

# Required libraries
if (!require("pacman")) install.packages("pacman")
pacman::p_load(RPostgres, DBI, readr, logging, progress, yaml, jsonlite, lubridate)

# ==============================================================================
# MAIN IMPORT FUNCTION
# ==============================================================================

csv_to_postgres <- function(
    csv_file,                    # Path to CSV file
    table_name,                  # Target table name
    connection_params = NULL,    # Database connection parameters
    con = NULL,                  # Existing connection (optional)
    schema = "public",           # Target schema
    chunk_size = 50000,          # Rows per chunk
    max_memory_mb = 1024,        # Memory limit for auto chunk sizing
    type_config = NULL,          # Manual type configuration
    create_table = TRUE,         # Whether to create table if not exists
    append_data = TRUE,          # Append to existing table
    validate_data = TRUE,        # Perform data quality checks
    optimize_performance = TRUE, # Apply performance optimizations
    enable_logging = TRUE,       # Enable detailed logging
    log_file = "import.log",     # Log file path
    progress_file = "import_progress.rds", # Progress tracking file
    max_retries = 3,             # Max retries per chunk
    resume_on_failure = TRUE,    # Resume from last successful chunk
    handle_duplicates = "error", # "error", "skip", "update"
    date_formats = c("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"), # Supported date formats
    datetime_formats = c("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S"),
    decimal_sep = ".",           # Decimal separator
    thousands_sep = ",",         # Thousands separator
    encoding = "UTF-8",          # File encoding
    na_values = c("", "NA", "NULL", "null", "N/A", "#N/A"),
    cleanup_on_success = TRUE,   # Remove progress file on success
    dedupe = FALSE,              # Remove exact duplicate rows (all columns identical)
    show_progress = TRUE         # Show progress bar (disable if causing issues)
) {
  
  # Initialize logging
  if (enable_logging) {
    setup_logging(log_file)
    loginfo("Starting CSV import process")
    loginfo("File: %s, Table: %s.%s", csv_file, schema, table_name)
  }
  
  # Validate inputs
  validate_inputs(csv_file, table_name, connection_params, con)
  
  # Create or use existing connection
  if (is.null(con)) {
    con <- create_robust_connection(connection_params, max_retries)
    close_connection_on_exit <- TRUE
  } else {
    close_connection_on_exit <- FALSE
    con <- ensure_connection_alive(con, connection_params)
  }
  
  tryCatch({
    # Optimize database settings for import
    if (optimize_performance) {
      original_settings <- optimize_db_for_import(con)
    }
    
    # Analyze CSV and determine optimal processing strategy
    file_info <- analyze_csv_file(csv_file, chunk_size, max_memory_mb, encoding, na_values)
    loginfo("CSV analysis complete: %s rows in file, will process in %s chunks of up to %s rows each", 
            file_info$total_rows, file_info$estimated_chunks, file_info$optimal_chunk_size)
    
    # Infer or load column types
    column_types <- determine_column_types(csv_file, type_config, 
                                           date_formats, datetime_formats,
                                           decimal_sep, thousands_sep, na_values, encoding)
    
    # Create table if requested
    if (create_table) {
      create_target_table(con, schema, table_name, column_types, append_data)
    }
    
    # Perform the import
    import_result <- perform_chunked_import(
      csv_file = csv_file,
      table_name = table_name,
      schema = schema,
      con = con,
      column_types = column_types,
      file_info = file_info,
      chunk_size = file_info$optimal_chunk_size,
      max_retries = max_retries,
      progress_file = progress_file,
      resume_on_failure = resume_on_failure,
      handle_duplicates = handle_duplicates,
      validate_data = validate_data,
      date_formats = date_formats,
      datetime_formats = datetime_formats,
      decimal_sep = decimal_sep,
      thousands_sep = thousands_sep,
      encoding = encoding,
      na_values = na_values,
      show_progress = show_progress
    )
    
    # Post-import operations
    post_import_operations(con, schema, table_name, column_types, dedupe)
    
    # Cleanup
    if (cleanup_on_success && file.exists(progress_file)) {
      file.remove(progress_file)
    }
    
    loginfo("Import completed successfully: %s rows imported in this session (total in table may include previous runs)", 
            import_result$session_imported %||% import_result$total_imported)
    return(import_result)
    
  }, error = function(e) {
    logerror("Import failed: %s", e$message)
    stop(sprintf("Import failed: %s", e$message))
    
  }, finally = {
    # Restore database settings
    if (optimize_performance && exists("original_settings")) {
      restore_db_settings(con, original_settings)
    }
    
    # Close connection if we created it
    if (close_connection_on_exit && dbIsValid(con)) {
      dbDisconnect(con)
    }
  })
}

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

setup_logging <- function(log_file) {
  basicConfig(level = "INFO")
  if (!is.null(log_file)) {
    addHandler(writeToFile, file = log_file, level = "DEBUG")
  }
}

validate_inputs <- function(csv_file, table_name, connection_params, con) {
  if (!file.exists(csv_file)) {
    stop("CSV file does not exist: ", csv_file)
  }
  
  if (is.null(con) && is.null(connection_params)) {
    stop("Either 'con' or 'connection_params' must be provided")
  }
  
  if (!grepl("^[a-zA-Z][a-zA-Z0-9_]*$", table_name)) {
    stop("Invalid table name: ", table_name)
  }
}

create_robust_connection <- function(connection_params, max_retries = 5) {
  for (attempt in 1:max_retries) {
    tryCatch({
      con <- do.call(dbConnect, c(list(RPostgres::Postgres()), connection_params))
      dbGetQuery(con, "SELECT 1")
      loginfo("Database connection established (attempt %d)", attempt)
      return(con)
    }, error = function(e) {
      if (attempt == max_retries) {
        stop(sprintf("Failed to connect after %d attempts: %s", max_retries, e$message))
      }
      logwarn("Connection attempt %d failed: %s", attempt, e$message)
      Sys.sleep(2^attempt)
    })
  }
}

ensure_connection_alive <- function(con, connection_params) {
  tryCatch({
    dbGetQuery(con, "SELECT 1")
    return(con)
  }, error = function(e) {
    logwarn("Connection lost, attempting to reconnect...")
    return(create_robust_connection(connection_params))
  })
}

optimize_db_for_import <- function(con) {
  original_settings <- list()
  
  # List of settings to try (some may fail due to permissions)
  settings_to_optimize <- list(
    list(name = "maintenance_work_mem", value = "1GB"),
    list(name = "work_mem", value = "256MB"),
    list(name = "synchronous_commit", value = "OFF"),
    list(name = "checkpoint_completion_target", value = "0.9"),
    list(name = "wal_buffers", value = "16MB"),
    list(name = "max_wal_size", value = "2GB")
  )
  
  successful_settings <- c()
  
  for (setting in settings_to_optimize) {
    tryCatch({
      # Get current value first
      current_value <- dbGetQuery(con, sprintf("SHOW %s", setting$name))[[setting$name]][1]
      original_settings[[setting$name]] <- current_value
      
      # Try to set new value
      dbExecute(con, sprintf("SET %s = '%s'", setting$name, setting$value))
      successful_settings <- c(successful_settings, setting$name)
      
    }, error = function(e) {
      logwarn("Could not optimize %s: %s", setting$name, e$message)
    })
  }
  
  if (length(successful_settings) > 0) {
    loginfo("Database optimized for import. Applied settings: %s", 
            paste(successful_settings, collapse = ", "))
  } else {
    logwarn("No database optimizations could be applied (insufficient permissions)")
  }
  
  return(original_settings)
}

restore_db_settings <- function(con, original_settings) {
  if (length(original_settings) == 0) {
    return()
  }
  
  restored_count <- 0
  
  tryCatch({
    for (setting_name in names(original_settings)) {
      tryCatch({
        dbExecute(con, sprintf("SET %s = '%s'", setting_name, original_settings[[setting_name]]))
        restored_count <- restored_count + 1
      }, error = function(e) {
        logwarn("Could not restore %s: %s", setting_name, e$message)
      })
    }
    
    if (restored_count > 0) {
      loginfo("Restored %d database settings", restored_count)
    }
    
  }, error = function(e) {
    logwarn("Failed to restore some database settings: %s", e$message)
  })
}

analyze_csv_file <- function(csv_file, chunk_size, max_memory_mb, encoding = "UTF-8", na_values = c("", "NA", "NULL")) {
  file_size_bytes <- file.size(csv_file)
  file_size_mb <- file_size_bytes / (1024^2)
  
  # Count total rows
  tryCatch({
    total_rows <- length(readLines(csv_file, encoding = encoding)) - 1  # Subtract header
  }, error = function(e) {
    # Fallback without encoding specification
    total_rows <- length(readLines(csv_file)) - 1
  })
  
  # Estimate optimal chunk size based on memory constraints
  if (file_size_mb > 0 && total_rows > 0) {
    bytes_per_row <- file_size_bytes / (total_rows + 1)  # +1 for header
    optimal_chunk_size <- floor((max_memory_mb * 1024^2) / (bytes_per_row * 3))  # Factor 3 for safety
    optimal_chunk_size <- pmax(pmin(optimal_chunk_size, 100000), 1000)  # Between 1K and 100K
  } else {
    optimal_chunk_size <- chunk_size
  }
  
  return(list(
    file_size_mb = file_size_mb,
    total_rows = total_rows,
    optimal_chunk_size = min(chunk_size, optimal_chunk_size),
    estimated_chunks = ceiling(total_rows / min(chunk_size, optimal_chunk_size))
  ))
}

determine_column_types <- function(csv_file, type_config, date_formats, datetime_formats, 
                                   decimal_sep = ".", thousands_sep = ",", na_values = c("", "NA", "NULL"), 
                                   encoding = "UTF-8") {
  
  if (!is.null(type_config)) {
    loginfo("Using provided type configuration")
    return(type_config)
  }
  
  # Read sample to infer types
  sample_df <- read_csv(csv_file, n_max = 10000, col_types = cols(.default = "c"),
                        locale = locale(decimal_mark = decimal_sep, grouping_mark = thousands_sep, encoding = encoding),
                        na = na_values, show_col_types = FALSE)
  
  column_types <- list()
  pg_types <- list()  # PostgreSQL types for table creation
  
  for (col_name in names(sample_df)) {
    col_data <- sample_df[[col_name]][!is.na(sample_df[[col_name]])]
    
    if (length(col_data) == 0) {
      column_types[[col_name]] <- list(r_type = "character", pg_type = "TEXT")
    } else {
      inferred_type <- infer_column_type(col_data, date_formats, datetime_formats)
      column_types[[col_name]] <- inferred_type
    }
  }
  
  loginfo("Inferred column types for %d columns", length(column_types))
  return(column_types)
}

infer_column_type <- function(col_data, date_formats, datetime_formats) {
  # Remove leading/trailing whitespace
  col_data <- trimws(col_data)
  
  # Check for integers
  if (all(grepl("^-?\\d+$", col_data))) {
    max_val <- max(abs(as.numeric(col_data)), na.rm = TRUE)
    if (max_val <= 2147483647) {
      return(list(r_type = "integer", pg_type = "INTEGER"))
    } else {
      return(list(r_type = "integer", pg_type = "BIGINT"))
    }
  }
  
  # Check for numeric (with decimals)
  if (all(grepl("^-?\\d*\\.?\\d+$", col_data))) {
    return(list(r_type = "numeric", pg_type = "DECIMAL(15,4)"))
  }
  
  # Check for currency/formatted numbers
  if (all(grepl("^-?[$£€¥]?\\d{1,3}(,\\d{3})*(\\.\\d+)?$", col_data))) {
    return(list(r_type = "currency", pg_type = "DECIMAL(15,2)"))
  }
  
  # Check for boolean
  if (all(tolower(col_data) %in% c("true", "false", "t", "f", "1", "0", "yes", "no", "y", "n"))) {
    return(list(r_type = "logical", pg_type = "BOOLEAN"))
  }
  
  # Check for dates
  for (fmt in date_formats) {
    if (all(!is.na(as.Date(col_data, format = fmt, optional = TRUE)))) {
      return(list(r_type = "date", pg_type = "DATE", format = fmt))
    }
  }
  
  # Check for datetimes
  for (fmt in datetime_formats) {
    if (all(!is.na(as.POSIXct(col_data, format = fmt, optional = TRUE)))) {
      return(list(r_type = "datetime", pg_type = "TIMESTAMP", format = fmt))
    }
  }
  
  # Check for JSON
  json_count <- sum(sapply(col_data, function(x) {
    tryCatch({ fromJSON(x); TRUE }, error = function(e) FALSE)
  }))
  if (json_count > length(col_data) * 0.5) {  # If >50% are valid JSON
    return(list(r_type = "json", pg_type = "JSONB"))
  }
  
  # Default to character/text
  max_length <- max(nchar(col_data), na.rm = TRUE)
  if (max_length <= 255) {
    pg_type <- sprintf("VARCHAR(%d)", pmax(max_length * 2, 50))
  } else {
    pg_type <- "TEXT"
  }
  
  return(list(r_type = "character", pg_type = pg_type))
}

create_target_table <- function(con, schema, table_name, column_types, append_data) {
  full_table_name <- paste(schema, table_name, sep = ".")
  
  # Check if table exists
  table_exists <- dbExistsTable(con, Id(schema = schema, table = table_name))
  
  if (table_exists && !append_data) {
    dbExecute(con, sprintf("DROP TABLE %s", full_table_name))
    table_exists <- FALSE
  }
  
  if (!table_exists) {
    # Generate CREATE TABLE statement
    columns_sql <- sapply(names(column_types), function(col_name) {
      sprintf('"%s" %s', col_name, column_types[[col_name]]$pg_type)
    })
    
    create_sql <- sprintf(
      "CREATE TABLE %s (%s)",
      full_table_name,
      paste(columns_sql, collapse = ", ")
    )
    
    dbExecute(con, create_sql)
    loginfo("Created table: %s", full_table_name)
  } else {
    loginfo("Using existing table: %s", full_table_name)
  }
}

perform_chunked_import <- function(csv_file, table_name, schema, con, column_types, file_info,
                                   chunk_size, max_retries, progress_file, resume_on_failure,
                                   handle_duplicates, validate_data, date_formats, datetime_formats,
                                   decimal_sep, thousands_sep, encoding, na_values, show_progress = TRUE) {
  
  # Load previous progress if resuming
  start_chunk <- 1
  total_imported <- 0
  
  if (resume_on_failure && file.exists(progress_file)) {
    progress_data <- readRDS(progress_file)
    start_chunk <- progress_data$last_completed_chunk + 1
    total_imported <- progress_data$total_imported
    loginfo("Resuming import from chunk %s (already imported %s rows)", start_chunk, total_imported)
  }
  
  # Setup progress tracking
  total_chunks <- file_info$estimated_chunks
  loginfo("Starting import: %s total chunks of up to %s rows each", total_chunks, chunk_size)
  
  # Only create progress bar if enabled and we have chunks to process
  pb <- NULL
  if (show_progress && start_chunk <= total_chunks) {
    tryCatch({
      pb <- progress_bar$new(
        format = "Importing [:bar] :percent (:current/:total chunks) ETA: :eta",
        total = total_chunks - start_chunk + 1,
        clear = FALSE
      )
    }, error = function(e) {
      logwarn("Could not create progress bar: %s", e$message)
      pb <<- NULL
    })
  }
  
  if (start_chunk > total_chunks) {
    loginfo("All %s chunks already completed, no new data to import", total_chunks)
    return(list(
      success = TRUE,
      total_imported = total_imported,
      failed_chunks = c(),
      total_chunks = total_chunks
    ))
  }
  
  # Import chunks
  failed_chunks <- c()
  current_session_imported <- 0
  
  for (chunk_num in start_chunk:total_chunks) {
    chunk_result <- import_single_chunk(
      csv_file, table_name, schema, con, column_types, chunk_num, chunk_size,
      max_retries, date_formats, datetime_formats, decimal_sep, thousands_sep,
      encoding, na_values, validate_data
    )
    
    if (chunk_result$success) {
      current_session_imported <- current_session_imported + chunk_result$rows_imported
      total_imported <- total_imported + chunk_result$rows_imported
      
      # Save progress
      progress_data <- list(
        last_completed_chunk = chunk_num,
        total_imported = total_imported,
        timestamp = Sys.time()
      )
      saveRDS(progress_data, progress_file)
      
      # Update progress bar if it exists
      if (!is.null(pb)) {
        tryCatch({
          pb$tick()
        }, error = function(e) {
          logwarn("Progress bar error: %s", e$message)
        })
      }
      
      loginfo("Chunk %s/%s completed: imported %s rows (session total: %s, overall total: %s)", 
              chunk_num, total_chunks, chunk_result$rows_imported, current_session_imported, total_imported)
      
      # Periodic memory cleanup
      if (chunk_num %% 10 == 0) {
        gc()
      }
      
    } else {
      failed_chunks <- c(failed_chunks, chunk_num)
      logwarn("Chunk %s/%s failed: %s", chunk_num, total_chunks, chunk_result$error)
    }
  }
  
  # Finalize progress bar safely
  if (!is.null(pb)) {
    tryCatch({
      if (!pb$finished) {
        pb$terminate()
      }
    }, error = function(e) {
      # Ignore progress bar finalization errors
    })
  }
  
  if (current_session_imported > 0) {
    loginfo("Import session completed: %s rows imported in this session", current_session_imported)
  }
  
  return(list(
    success = length(failed_chunks) == 0,
    total_imported = total_imported,
    session_imported = current_session_imported,
    failed_chunks = failed_chunks,
    total_chunks = total_chunks
  ))
}

import_single_chunk <- function(csv_file, table_name, schema, con, column_types, chunk_num, 
                                chunk_size, max_retries, date_formats, datetime_formats,
                                decimal_sep, thousands_sep, encoding, na_values, validate_data) {
  
  skip_rows <- (chunk_num - 1) * chunk_size
  
  for (attempt in 1:max_retries) {
    tryCatch({
      # Read chunk
      df_chunk <- read_csv(
        csv_file,
        skip = skip_rows,
        n_max = chunk_size,
        col_types = cols(.default = "c"),
        locale = locale(decimal_mark = decimal_sep, grouping_mark = thousands_sep, encoding = encoding),
        na = na_values,
        show_col_types = FALSE
      )
      
      if (nrow(df_chunk) == 0) {
        return(list(success = TRUE, rows_imported = 0))
      }
      
      # Convert data types
      df_chunk <- convert_chunk_types(df_chunk, column_types, date_formats, datetime_formats)
      
      # Validate data if requested
      if (validate_data) {
        df_chunk <- validate_chunk_data(df_chunk, column_types)
      }
      
      # Import to database
      table_id <- Id(schema = schema, table = table_name)
      dbAppendTable(con, table_id, df_chunk)
      
      return(list(success = TRUE, rows_imported = nrow(df_chunk)))
      
    }, error = function(e) {
      if (attempt == max_retries) {
        return(list(success = FALSE, error = e$message, chunk = chunk_num))
      }
      
      Sys.sleep(2^attempt)  # Exponential backoff
      logwarn("Chunk %d attempt %d failed, retrying: %s", chunk_num, attempt, e$message)
    })
  }
}

convert_chunk_types <- function(df, column_types, date_formats, datetime_formats) {
  for (col_name in names(column_types)) {
    if (col_name %in% names(df)) {
      type_info <- column_types[[col_name]]
      
      tryCatch({
        df[[col_name]] <- switch(type_info$r_type,
                                 "integer" = as.integer(df[[col_name]]),
                                 "numeric" = as.numeric(df[[col_name]]),
                                 "currency" = {
                                   # Remove currency symbols and thousands separators
                                   clean_val <- gsub("[$£€¥,]", "", df[[col_name]])
                                   as.numeric(clean_val)
                                 },
                                 "logical" = {
                                   val <- tolower(trimws(df[[col_name]]))
                                   val %in% c("true", "t", "yes", "y", "1")
                                 },
                                 "date" = as.Date(df[[col_name]], format = type_info$format %||% "%Y-%m-%d"),
                                 "datetime" = as.POSIXct(df[[col_name]], format = type_info$format %||% "%Y-%m-%d %H:%M:%S"),
                                 "json" = sapply(df[[col_name]], function(x) {
                                   if (is.na(x) || x == "") return(NA)
                                   tryCatch(toJSON(fromJSON(x), auto_unbox = TRUE), error = function(e) x)
                                 }),
                                 "character" = as.character(df[[col_name]]),
                                 df[[col_name]]  # default: no conversion
        )
      }, error = function(e) {
        logwarn("Failed to convert column '%s' in chunk: %s", col_name, e$message)
      })
    }
  }
  return(df)
}

validate_chunk_data <- function(df, column_types) {
  # Remove rows that are completely empty
  df <- df[rowSums(is.na(df)) != ncol(df), ]
  
  # Additional validation logic can be added here
  # For example, check for required fields, value ranges, etc.
  
  return(df)
}

post_import_operations <- function(con, schema, table_name, column_types, dedupe = FALSE) {
  full_table_name <- paste(schema, table_name, sep = ".")
  
  # Deduplication if requested - removes exact duplicate rows (all columns identical)
  if (dedupe) {
    remove_exact_duplicates(con, full_table_name)
  }
  
  # Create indexes for commonly queried columns
  index_candidates <- names(column_types)[sapply(column_types, function(x) {
    x$r_type %in% c("integer", "date", "datetime") || 
      (x$r_type == "character" && grepl("VARCHAR\\([0-9]+\\)", x$pg_type))
  })]
  
  for (col_name in index_candidates[1:min(5, length(index_candidates))]) {  # Limit to 5 indexes
    tryCatch({
      index_name <- sprintf("idx_%s_%s", gsub("\\.", "_", table_name), col_name)
      index_sql <- sprintf('CREATE INDEX CONCURRENTLY "%s" ON %s ("%s")', 
                           index_name, full_table_name, col_name)
      dbExecute(con, index_sql)
    }, error = function(e) {
      logwarn("Failed to create index on %s: %s", col_name, e$message)
    })
  }
  
  # Update table statistics
  dbExecute(con, paste("ANALYZE", full_table_name))
  
  loginfo("Post-import operations completed")
}

# Simple function to remove exact duplicate rows (all columns identical)
remove_exact_duplicates <- function(con, full_table_name) {
  loginfo("Starting deduplication: checking for exact duplicate rows")
  
  # Get original row count
  original_count <- as.integer(dbGetQuery(con, sprintf("SELECT COUNT(*) as cnt FROM %s", full_table_name))$cnt[1])
  
  if (original_count == 0) {
    loginfo("Table is empty, no deduplication needed")
    return()
  }
  
  loginfo("Table currently contains %s rows total", original_count)
  
  # Check if there are any duplicates first
  distinct_count <- as.integer(dbGetQuery(con, sprintf("SELECT COUNT(*) as cnt FROM (SELECT DISTINCT * FROM %s) as distinct_rows", full_table_name))$cnt[1])
  
  duplicate_count <- original_count - distinct_count
  
  if (duplicate_count == 0) {
    loginfo("No duplicate rows found - table is already clean")
    return()
  }
  
  loginfo("Deduplication needed: %s unique rows, %s duplicates to remove", distinct_count, duplicate_count)
  
  # Parse table name parts properly
  if (grepl("\\.", full_table_name)) {
    table_parts <- strsplit(full_table_name, "\\.")[[1]]
    schema_name <- table_parts[1]
    table_name <- table_parts[2]
  } else {
    schema_name <- "public"
    table_name <- full_table_name
  }
  
  temp_table_name <- sprintf("%s.%s_dedup_temp", schema_name, table_name)
  
  tryCatch({
    # Create temporary table with distinct rows only
    loginfo("Creating clean table with distinct rows...")
    create_distinct_sql <- sprintf(
      "CREATE TABLE %s AS SELECT DISTINCT * FROM %s",
      temp_table_name, full_table_name
    )
    
    dbExecute(con, create_distinct_sql)
    
    # Replace original table with deduplicated version
    loginfo("Replacing original table with clean version...")
    dbExecute(con, sprintf("DROP TABLE %s", full_table_name))
    dbExecute(con, sprintf("ALTER TABLE %s RENAME TO %s", temp_table_name, table_name))
    
    loginfo("Deduplication completed successfully: removed %s duplicate rows, kept %s unique rows", 
            duplicate_count, distinct_count)
    
  }, error = function(e) {
    # Clean up temp table if something went wrong
    tryCatch(dbExecute(con, sprintf("DROP TABLE IF EXISTS %s", temp_table_name)), 
             error = function(e2) {})
    logerror("Deduplication failed: %s", e$message)
    stop(sprintf("Deduplication failed: %s", e$message))
  })
}

# Deduplication function
perform_deduplication <- function(con, full_table_name, dedupe_columns, strategy = "delete_duplicates") {
  
  # If dedupe_columns is "all", get all column names
  if (length(dedupe_columns) == 1 && dedupe_columns == "all") {
    table_parts <- if(grepl("\\.", full_table_name)) {
      c(dirname(full_table_name), basename(full_table_name))
    } else {
      c("public", full_table_name)
    }
    
    # Get all column names from the table
    all_columns_query <- sprintf(
      "SELECT column_name 
             FROM information_schema.columns 
             WHERE table_name = '%s' AND table_schema = '%s'
             ORDER BY ordinal_position",
      table_parts[2], table_parts[1]
    )
    
    all_columns_result <- dbGetQuery(con, all_columns_query)
    dedupe_columns <- all_columns_result$column_name
    
    loginfo("Deduplicating on ALL columns: %s", paste(dedupe_columns, collapse = ", "))
  } else {
    loginfo("Starting deduplication on columns: %s", paste(dedupe_columns, collapse = ", "))
  }
  
  # Count duplicates first - for ALL columns, we look for exact row duplicates
  if (length(dedupe_columns) > 1 && all(dedupe_columns %in% all_columns_result$column_name)) {
    # For all columns, use a simpler approach to find exact duplicate rows
    duplicate_count_sql <- sprintf(
      "SELECT COUNT(*) - COUNT(DISTINCT (%s)) as duplicate_count FROM %s",
      paste(sprintf('"%s"', dedupe_columns), collapse = ", "),
      full_table_name
    )
  } else {
    # For specific columns
    duplicate_count_sql <- sprintf(
      "SELECT COUNT(*) as duplicate_count FROM (
                SELECT %s, COUNT(*) as cnt 
                FROM %s 
                GROUP BY %s 
                HAVING COUNT(*) > 1
            ) duplicates",
      paste(sprintf('"%s"', dedupe_columns), collapse = ", "),
      full_table_name,
      paste(sprintf('"%s"', dedupe_columns), collapse = ", ")
    )
  }
  
  duplicate_count <- dbGetQuery(con, duplicate_count_sql)$duplicate_count[1]
  
  if (duplicate_count == 0) {
    loginfo("No duplicates found")
    return()
  }
  
  loginfo("Found %d duplicate rows", duplicate_count)
  
  # For all columns deduplication, use a simpler deletion strategy
  if (length(dedupe_columns) > 5) {  # Assume this means "all columns"
    delete_exact_duplicates(con, full_table_name)
  } else {
    # Perform deduplication based on strategy for specific columns
    if (strategy == "delete_duplicates") {
      delete_duplicates(con, full_table_name, dedupe_columns)
    } else if (strategy == "keep_latest") {
      keep_latest_duplicates(con, full_table_name, dedupe_columns)
    } else if (strategy == "keep_first") {
      keep_first_duplicates(con, full_table_name, dedupe_columns)
    } else if (strategy == "merge_duplicates") {
      merge_duplicates(con, full_table_name, dedupe_columns)
    } else {
      logwarn("Unknown deduplication strategy: %s", strategy)
      return()
    }
  }
  
  # Verify deduplication
  remaining_duplicates <- dbGetQuery(con, duplicate_count_sql)$duplicate_count[1]
  loginfo("Deduplication completed. Remaining duplicates: %d", remaining_duplicates)
}

# Simple function to delete exact duplicate rows (all columns identical)
delete_exact_duplicates <- function(con, full_table_name) {
  # Use ctid to identify and keep only one instance of each unique row
  dedupe_sql <- sprintf(
    "DELETE FROM %s a USING %s b 
         WHERE a.ctid < b.ctid 
         AND a.* = b.*",
    full_table_name, full_table_name
  )
  
  # Alternative approach that's more reliable across PostgreSQL versions
  # Create a temporary table with distinct rows, then replace the original
  table_parts <- if(grepl("\\.", full_table_name)) {
    c(dirname(full_table_name), basename(full_table_name))
  } else {
    c("public", full_table_name)
  }
  
  temp_table_name <- sprintf("%s.%s_dedup_temp", table_parts[1], table_parts[2])
  
  tryCatch({
    # Create temporary table with distinct rows only
    create_distinct_sql <- sprintf(
      "CREATE TABLE %s AS SELECT DISTINCT * FROM %s",
      temp_table_name, full_table_name
    )
    
    dbExecute(con, create_distinct_sql)
    
    # Get row counts for reporting
    original_count <- dbGetQuery(con, sprintf("SELECT COUNT(*) as cnt FROM %s", full_table_name))$cnt[1]
    distinct_count <- dbGetQuery(con, sprintf("SELECT COUNT(*) as cnt FROM %s", temp_table_name))$cnt[1]
    
    # Replace original table with deduplicated version
    dbExecute(con, sprintf("DROP TABLE %s", full_table_name))
    dbExecute(con, sprintf("ALTER TABLE %s RENAME TO %s", temp_table_name, table_parts[2]))
    
    rows_removed <- original_count - distinct_count
    loginfo("Removed %d exact duplicate rows (kept %d unique rows)", rows_removed, distinct_count)
    
  }, error = function(e) {
    # Clean up temp table if something went wrong
    tryCatch(dbExecute(con, sprintf("DROP TABLE IF EXISTS %s", temp_table_name)), 
             error = function(e2) {})
    logerror("Failed to remove exact duplicates: %s", e$message)
    stop(e)
  })
}

# Delete duplicates keeping one random record per group
delete_duplicates <- function(con, full_table_name, dedupe_columns) {
  # Use ROW_NUMBER() to identify duplicates and keep only the first occurrence
  dedupe_sql <- sprintf(
    "DELETE FROM %s 
         WHERE ctid NOT IN (
             SELECT DISTINCT ON (%s) ctid 
             FROM %s 
             ORDER BY %s
         )",
    full_table_name,
    paste(sprintf('"%s"', dedupe_columns), collapse = ", "),
    full_table_name,
    paste(sprintf('"%s"', dedupe_columns), collapse = ", ")
  )
  
  rows_deleted <- dbExecute(con, dedupe_sql)
  loginfo("Deleted %d duplicate rows using delete_duplicates strategy", rows_deleted)
}

# Keep the latest record based on a timestamp or ID column
keep_latest_duplicates <- function(con, full_table_name, dedupe_columns, 
                                   order_column = NULL) {
  
  # Try to find a suitable ordering column if not provided
  if (is.null(order_column)) {
    # Get table columns and their types
    columns_info <- dbGetQuery(con, sprintf(
      "SELECT column_name, data_type 
             FROM information_schema.columns 
             WHERE table_name = '%s' AND table_schema = '%s'
             ORDER BY ordinal_position",
      basename(full_table_name), 
      if(grepl("\\.", full_table_name)) dirname(full_table_name) else "public"
    ))
    
    # Look for timestamp, date, or ID columns
    timestamp_cols <- columns_info$column_name[grepl("timestamp|datetime", columns_info$data_type, ignore.case = TRUE)]
    date_cols <- columns_info$column_name[grepl("date", columns_info$data_type, ignore.case = TRUE)]
    id_cols <- columns_info$column_name[grepl("id$|_id$", columns_info$column_name, ignore.case = TRUE)]
    
    if (length(timestamp_cols) > 0) {
      order_column <- timestamp_cols[1]
    } else if (length(date_cols) > 0) {
      order_column <- date_cols[1]
    } else if (length(id_cols) > 0) {
      order_column <- id_cols[1]
    } else {
      logwarn("No suitable ordering column found, using ctid")
      order_column <- "ctid"
    }
  }
  
  loginfo("Using column '%s' for ordering latest records", order_column)
  
  dedupe_sql <- sprintf(
    "DELETE FROM %s 
         WHERE ctid NOT IN (
             SELECT DISTINCT ON (%s) ctid 
             FROM %s 
             ORDER BY %s, %s DESC
         )",
    full_table_name,
    paste(sprintf('"%s"', dedupe_columns), collapse = ", "),
    full_table_name,
    paste(sprintf('"%s"', dedupe_columns), collapse = ", "),
    sprintf('"%s"', order_column)
  )
  
  rows_deleted <- dbExecute(con, dedupe_sql)
  loginfo("Deleted %d duplicate rows using keep_latest strategy", rows_deleted)
}

# Keep the first record (by insertion order)
keep_first_duplicates <- function(con, full_table_name, dedupe_columns) {
  dedupe_sql <- sprintf(
    "DELETE FROM %s 
         WHERE ctid NOT IN (
             SELECT DISTINCT ON (%s) ctid 
             FROM %s 
             ORDER BY %s, ctid
         )",
    full_table_name,
    paste(sprintf('"%s"', dedupe_columns), collapse = ", "),
    full_table_name,
    paste(sprintf('"%s"', dedupe_columns), collapse = ", ")
  )
  
  rows_deleted <- dbExecute(con, dedupe_sql)
  loginfo("Deleted %d duplicate rows using keep_first strategy", rows_deleted)
}

# Merge duplicates by aggregating numeric columns
merge_duplicates <- function(con, full_table_name, dedupe_columns) {
  # Get table schema
  table_parts <- if(grepl("\\.", full_table_name)) {
    c(dirname(full_table_name), basename(full_table_name))
  } else {
    c("public", full_table_name)
  }
  
  columns_info <- dbGetQuery(con, sprintf(
    "SELECT column_name, data_type 
         FROM information_schema.columns 
         WHERE table_name = '%s' AND table_schema = '%s'
         ORDER BY ordinal_position",
    table_parts[2], table_parts[1]
  ))
  
  # Separate columns by type
  numeric_cols <- columns_info$column_name[grepl("integer|numeric|decimal|real|double", 
                                                 columns_info$data_type, ignore.case = TRUE)]
  text_cols <- columns_info$column_name[grepl("text|character|varchar", 
                                              columns_info$data_type, ignore.case = TRUE)]
  
  # Remove dedupe columns from aggregation
  numeric_cols <- setdiff(numeric_cols, dedupe_columns)
  text_cols <- setdiff(text_cols, dedupe_columns)
  
  # Build aggregation SQL
  select_parts <- c(
    paste(sprintf('"%s"', dedupe_columns), collapse = ", "),
    if(length(numeric_cols) > 0) paste(sprintf('SUM("%s") as "%s"', numeric_cols, numeric_cols), collapse = ", "),
    if(length(text_cols) > 0) paste(sprintf('STRING_AGG(DISTINCT "%s", \'; \') as "%s"', text_cols, text_cols), collapse = ", ")
  )
  
  # Create temporary table with merged data
  temp_table <- paste0(table_parts[2], "_dedupe_temp")
  
  create_merged_sql <- sprintf(
    "CREATE TABLE %s.%s AS 
         SELECT %s
         FROM %s 
         GROUP BY %s",
    table_parts[1], temp_table,
    paste(select_parts[select_parts != ""], collapse = ", "),
    full_table_name,
    paste(sprintf('"%s"', dedupe_columns), collapse = ", ")
  )
  
  tryCatch({
    # Create merged table
    dbExecute(con, create_merged_sql)
    
    # Replace original table
    dbExecute(con, sprintf("DROP TABLE %s", full_table_name))
    dbExecute(con, sprintf("ALTER TABLE %s.%s RENAME TO %s", 
                           table_parts[1], temp_table, table_parts[2]))
    
    loginfo("Merged duplicates using aggregation strategy")
    
  }, error = function(e) {
    # Clean up temp table if it exists
    tryCatch(dbExecute(con, sprintf("DROP TABLE IF EXISTS %s.%s", table_parts[1], temp_table)), 
             error = function(e2) {})
    logerror("Failed to merge duplicates: %s", e$message)
  })
}

# Utility operator for null coalescing
`%||%` <- function(a, b) if (is.null(a)) b else a

# ==============================================================================
# EXAMPLE USAGE
# ==============================================================================

# Example 1: Basic usage
# result <- csv_to_postgres(
#     csv_file = "customers.csv",
#     table_name = "customers",
#     connection_params = list(
#         host = "localhost",
#         port = 5432,
#         dbname = "appdb",
#         user = "appuser",
#         password = "su7pcP@ssw0rd"
#     )
# )

# Example 2: With deduplication enabled
# result <- csv_to_postgres(
#     csv_file = "customers.csv",
#     table_name = "customers",
#     connection_params = connection_params,
#     schema = "sales", 
#     dedupe = TRUE,              # Remove exact duplicate rows
#     chunk_size = 25000,
#     validate_data = TRUE
# )