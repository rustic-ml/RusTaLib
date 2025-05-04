# Technical Indicators Examples

This directory contains simple examples showing how to use the technical indicators provided by this library.

## Basic Examples

- `rsi_basic.rs` - Shows how to calculate and interpret the Relative Strength Index
- `macd_basic.rs` - Demonstrates the Moving Average Convergence Divergence indicator
- `bollinger_bands_basic.rs` - Example of Bollinger Bands volatility indicator
- `moving_averages_basic.rs` - Comparison of different moving average types (SMA, EMA, WMA)

## CSV Reading Examples

Several examples demonstrate how to load data from CSV files:

- `compare_minute_indicator_strategies.rs` - Shows how to read minute-level OHLCV data and convert integer volume to float
- `compare_multi_indicator_strategies.rs` - Demonstrates proper CSV reading with column renaming
- `multi_indicator_strategy_for_daily_ohlcv.rs` - Shows CSV reading with a predefined schema

Note that when using Polars 0.46.0, CSV files should be read using the `CsvReadOptions` approach:

```rust
// Read a CSV file with headers
let df = CsvReadOptions::default()
    .with_has_header(true)
    .try_into_reader_with_file_path(Some("path/to/file.csv".into()))?
    .finish()?;
```

## Running the Examples

To run any example, use the following command from the project root:

```bash
cargo run --example <example_name>
```

For instance:

```bash
cargo run --example rsi_basic
```

## Notes

These examples use synthetic data for demonstration purposes. In real-world applications, you would:

- Import actual market data (CSV files, APIs, etc.)
- Apply proper backtesting methodology
- Consider transaction costs and slippage
- Combine multiple indicators for confirmation
- Implement risk management strategies

The examples are designed to be simple and focus on demonstrating the basic usage of each indicator. 