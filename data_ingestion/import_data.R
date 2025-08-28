source('./data_ingestion/pg_import.R')

# Define connection parameters separately
source('./data_ingestion/db_config.r')


# Test
if(TRUE) {
  rows_imported <- import_csv_simple(
    csv_file = "./data_ingestion/data_to_upload/ods_avm_trips/avm_trips_2025_jul.csv",
    table_name = "test_table_simple",
    upload_chunk_size = 50000,
    enable_compression = TRUE
  )
}