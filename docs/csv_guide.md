# Working with CSV Data in ta-lib-in-rust

This guide explains how to effectively work with CSV data when using the ta-lib-in-rust library.

## CSV Format Requirements

The technical indicators in this library expect DataFrame columns with specific lowercase names:
- `open` - Opening price
- `high` - High price
- `low` - Low price
- `close` - Closing price
- `volume` - Trading volume (should be cast to Float64)
- `date` or `timestamp` - Date/time information (optional but recommended)

Many data providers and exported CSV files may use different naming conventions (e.g., uppercase names like "Open", "High", etc.). You'll need to transform these to match the library's expected format.

## Project CSV Organization

In this project:
- Input CSV data is stored in the `examples/` directory (e.g., `examples/AAPL_daily_ohlcv.csv`)
- Output/results are saved to the `examples/csv/` directory (e.g., `examples/csv/AAPL_indicators.csv`)

You can follow this pattern in your own implementations to maintain a clean separation between input and output files.

## Loading and Transforming CSV Data

### Basic CSV Loading

```rust
use polars::prelude::*;

// Load CSV file
let df = CsvReadOptions::default()
    .with_has_header(true)
    .try_into_reader_with_file_path(Some("your_data.csv".into()))?
    .finish()?;

// Print the column names to verify what we're working with
println!("Available columns: {:?}", df.get_column_names());
```

### Handling Case Sensitivity

If your CSV has uppercase column names (like AAPL_daily_ohlcv.csv and similar files), use the following pattern:

```rust
// Convert uppercase column names to lowercase
let df = df.lazy()
    .select([
        col("Symbol").alias("symbol"),
        col("Timestamp").alias("timestamp"),
        col("Open").alias("open"),
        col("High").alias("high"),
        col("Low").alias("low"),
        col("Close").alias("close"),
        col("Volume").cast(DataType::Float64).alias("volume"),
        col("VWAP").alias("vwap"),  // If available
    ])
    .collect()?;
```

### Multiple Time Aliases

Some strategies might require date fields in different formats. You can create multiple aliases:

```rust
let df = df.lazy()
    .select([
        // Other columns...
        col("Timestamp").alias("timestamp").alias("date"),  // Create two aliases for the same column
        // Other columns...
    ])
    .collect()?;
```

## Multi-Stock Analysis

When working with multiple stock data files:

```rust
// Define the tickers to analyze
let tickers = vec!["AAPL", "GOOGL", "MSFT"];

// Process each ticker
for ticker in &tickers {
    println!("Analyzing {}", ticker);
    
    // Load ticker's data
    let file_path = format!("examples/{}_daily_ohlcv.csv", ticker);
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some(file_path.into()))?
        .finish()?;
        
    // Convert column names to lowercase
    let df = df.lazy()
        .select([
            col("Open").alias("open"),
            // Other columns...
        ])
        .collect()?;
    
    // Apply strategies and analyze...
}
```

## Common Issues and Solutions

### Missing Columns Error

If you see errors like `ColumnNotFound` or indicators complaining about missing columns, check:
1. That you've properly renamed all required columns to lowercase
2. That your DataFrame contains all necessary columns for the indicator you're using
3. That column types are appropriate (especially that `volume` is Float64)

### CMF (Chaikin Money Flow) with Small Datasets

The CMF indicator requires a sufficient amount of data based on its window parameter. If you're working with small datasets:

```rust
// Safely handle CMF calculation for any dataset size
let cmf_period = 20;
if df.height() > cmf_period {
    // Safe to calculate CMF
    let cmf = calculate_cmf(&df, cmf_period)?;
    // Use the CMF...
} else {
    println!("Dataset too small for CMF calculation with period {}", cmf_period);
    // Handle the case or skip this indicator
}
```

## Saving Results to CSV

After running your analysis, you can save the results back to CSV:

```rust
// Save results to the examples/csv directory
let output_path = format!("examples/csv/{}_results.csv", ticker);
CsvWriter::new(std::io::BufWriter::new(std::fs::File::create(output_path)?))
    .finish(&mut results_df)?;
``` 