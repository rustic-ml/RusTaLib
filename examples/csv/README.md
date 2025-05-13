# Sample Data Files for Technical Analysis Examples

This directory contains sample financial data files used by the examples in this repository. These files are provided for demonstration purposes and contain historical price data for various stocks.

## Available Data Files

### Daily OHLCV Data

Daily price data (Open, High, Low, Close, Volume) for major tech stocks:

- `AAPL_daily_ohlcv.csv` / `.parquet` - Apple Inc.
- `AMZN_daily_ohlcv.csv` / `.parquet` - Amazon.com Inc.
- `GOOGL_daily_ohlcv.csv` / `.parquet` - Alphabet Inc. (Google)
- `META_daily_ohlcv.csv` / `.parquet` - Meta Platforms Inc. (Facebook)
- `MSFT_daily_ohlcv.csv` / `.parquet` - Microsoft Corporation
- `NVDA_daily_ohlcv.csv` / `.parquet` - NVIDIA Corporation
- `TSLA_daily_ohlcv.csv` / `.parquet` - Tesla Inc.
- `TSM_daily_ohlcv.csv` / `.parquet` - Taiwan Semiconductor Manufacturing Company

### Minute OHLCV Data

Higher frequency intraday data for more detailed analysis:

- `*_minute_ohlcv.csv` / `.parquet` - Same companies as above with minute-level data

### Indicator Files

Generated indicator files from the multi-stock analysis example:

- `*_indicators.csv` - Technical indicators calculated for each stock

## File Formats

- **CSV files** - Text-based format, human-readable, compatible with many tools
- **Parquet files** - Columnar storage format, optimized for performance and size

## Usage Examples

These files are used in examples including:

- `working_with_multi_stock_data.rs` - Demonstrates loading data from these CSV files, calculating indicators, and comparing stocks
- `stock/trend_following.rs` - Uses daily data to test trend following strategies
- `stock/mean_reversion.rs` - Uses daily data to test mean reversion strategies

## Data Structure

The CSV files follow this structure:
- Column 1: Symbol
- Column 2: Timestamp
- Column 3: Open
- Column 4: High
- Column 5: Low
- Column 6: Close
- Column 7: Volume
- Column 8: Adjusted Close

## Note on Data Quality

This data is intended for educational and demonstration purposes only. For real trading applications, you should obtain high-quality data from reliable market data providers. 