# CSV to PostgreSQL Import Approach

## Overview

This folder contains a simple, reliable CSV-to-PostgreSQL import system that uses a **local-first** strategy for maximum reliability and performance.

## Architecture

### Core Philosophy: Read Local, Upload Smart

Instead of complex streaming/sampling approaches, we:
1. **Read the entire CSV locally first** (fast with data.table)
2. **Analyze complete dataset** for accurate type detection
3. **Upload in manageable chunks** with retry logic
4. **Use atomic operations** for safety

## File Structure

```
data_ingestion/
├── pg_import.R          # Core import function: import_csv_simple()
├── db_config.r          # Database connection parameters
├── import_data.R        # Test runner / example usage
└── README_APPROACH.md   # This documentation
```

## Key Components

### 1. `import_csv_simple()` Function

**Location:** `pg_import.R`

**Signature:**
```r
import_csv_simple(csv_file, table_name, upload_chunk_size = 100000, enable_compression = TRUE)
```

**Process:**
1. **Local Read**: `data.table::fread()` - extremely fast CSV reading
2. **Type Detection**: `readr::type_convert()` - battle-tested type inference
3. **PostgreSQL Fixes**: Handle edge cases (like 24+ hour times)
4. **Chunked Upload**: Upload in chunks with retry logic
5. **Compression**: Apply TOAST + extended storage compression
6. **Atomic Finalization**: Safe temp table → final table swap

### 2. Database Configuration

**Location:** `db_config.r`

Contains connection parameters externalized for security and flexibility.

### 3. Test Runner

**Location:** `import_data.R`

Simple script that demonstrates usage and can be used for testing.

## Why This Approach Works

### ✅ Advantages

1. **Predictable**: No sampling uncertainty - sees all data
2. **Fast**: data.table reading + readr type detection are highly optimized
3. **Reliable**: Handles edge cases that broke streaming approaches
4. **Safe**: Atomic operations with temp tables
5. **Space Efficient**: Built-in compression support
6. **Simple**: Easy to understand and maintain

### ❌ Previous Approaches That Failed

1. **Complex Sampling**: Random sampling was slow and missed edge cases
2. **Streaming with Type Guessing**: Type mismatches between chunks
3. **Manual Type Detection**: Regex patterns couldn't handle all cases

## Technical Details

### Type Detection Strategy

- Uses `readr::type_convert()` - proven library with extensive edge case handling
- Handles mixed content gracefully (e.g., "N/A (id=920129)" stays as character)
- Fixes PostgreSQL incompatibilities (24+ hour times → character)

### Memory Considerations

- **Pros**: Complete dataset in memory allows perfect type analysis
- **Cons**: Requires sufficient RAM for large files
- **Mitigation**: For files >RAM, could implement streaming version of this pattern

### Compression Features

- **TOAST compression**: Built-in PostgreSQL compression
- **Extended storage**: Maximum compression for text columns
- **Post-load VACUUM**: Ensures compression is applied
- **Size reporting**: Shows actual space savings

### Error Handling

- **Retry logic**: 3 attempts per chunk upload
- **Atomic operations**: Either complete success or complete rollback
- **Cleanup**: Automatic temp table removal on failure
- **Graceful degradation**: Compression failures don't stop import

## Usage Examples

### Basic Usage
```r
source('./data_ingestion/pg_import.R')
source('./data_ingestion/db_config.r')

rows_imported <- import_csv_simple(
  csv_file = "path/to/data.csv",
  table_name = "my_table"
)
```

### With Custom Settings
```r
rows_imported <- import_csv_simple(
  csv_file = "large_file.csv",
  table_name = "big_table",
  upload_chunk_size = 50000,    # Smaller chunks
  enable_compression = TRUE     # Enable compression
)
```

## Performance Characteristics

### Typical Performance
- **Reading**: 1-5 million rows/minute (depends on columns)
- **Type conversion**: Very fast (in-memory operations)
- **Upload**: 50,000-200,000 rows/second (depends on network/server)
- **Compression**: Can achieve 40-70% space savings on text-heavy data

### Scalability
- **Sweet spot**: Files up to several GB
- **Memory requirement**: ~3x CSV file size (for processing overhead)
- **Network efficient**: Chunked uploads with retry

## Future Enhancements

Potential improvements (not currently needed):

1. **Folder walking**: Process multiple CSVs in subfolders
2. **Success markers**: Skip already-imported files
3. **Streaming version**: For files larger than available RAM
4. **Parallel uploads**: Multiple connections for very large datasets
5. **Schema comparison**: Detect schema changes between imports

## Maintenance Notes

- **Dependencies**: pacman auto-installs required packages
- **Database versions**: Works with PostgreSQL 9.6+
- **R versions**: Tested with R 4.0+
- **Error logging**: All output goes to console (could be enhanced)

## Lessons Learned

1. **Simple often wins**: Complex sampling was slower and less reliable
2. **Use proven libraries**: readr/data.table handle edge cases better than custom code
3. **Local-first can be faster**: Network I/O is often the bottleneck, not file I/O
4. **Atomic operations are crucial**: Temp tables prevent data corruption
5. **Compression needs tuning**: TOAST doesn't always compress as expected

---

*This approach was developed through iteration, starting with complex sampling strategies and evolving to this simple, reliable solution.*