# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an ad data pipelines project focused on public transit analytics. The codebase processes transit data (AFC/automatic fare collection data) and VDV transit format files to generate passenger analytics and ridership reports.

## Architecture

The project follows a layered data architecture:

- **Data Sources**: AFC (Automatic Fare Collection) data in Parquet format, VDV transit data files (.x10 format), geographic data (GeoJSON, Shapefiles)
- **Data Ingestion Layer** (`data_ingestion/`): R scripts for importing data to PostgreSQL
- **Processing Scripts** (`scripts/`): Python and R scripts for data transformations and analytics
- **Outputs** (`resutls/`): Generated CSV files with analytics results

### Key Components

- `data_ingestion/pg_import.R`: Robust PostgreSQL import function with chunking, error handling, and resume capability
- `data_ingestion/import_data.R`: Main data import orchestration script
- `scripts/`: Contains analytics scripts generating various ADS (Application Data Store) tables:
  - Passenger transfer counts
  - Travel distances and times  
  - Ridership metrics by route and stop
  - Route and stop information

## Development Environment

- **Python 3.10.14** (available)
- **R** (required but not available in current environment)
- **RStudio Project**: `ad_data_pipelines.Rproj` configured with 2-space indentation, UTF-8 encoding

## Database Configuration

The project connects to PostgreSQL with configuration defined in `data_ingestion/import_data.R`:
- Host: 47.130.164.165
- Port: 5432
- Database: itc_source_db
- Schema: public

## File Naming Conventions

- `ads_*`: Analytics Data Store tables (final output layer)
- `dwd_*`: Data Warehouse Detail layer tables
- `dim_*`: Dimension tables
- Files with `_di`, `_mi`, `_mf` suffixes indicate data integration, master information, and master fact tables respectively

## Data Processing Notes

- AFC data processing includes coordinate conversion functions (from custom format to decimal degrees)
- VDV format handling for European transit standard data
- Memory-optimized processing with chunking and garbage collection
- HyperLogLog for approximate distinct counting in large datasets