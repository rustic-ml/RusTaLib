# Technical Indicators (Rust)

[![crates.io](https://img.shields.io/crates/v/technical-indicators.svg)](https://crates.io/crates/technical-indicators) <!-- Placeholder badge -->
[![docs.rs](https://docs.rs/technical-indicators/badge.svg)](https://docs.rs/technical-indicators) <!-- Placeholder badge -->

A Rust library for calculating common financial technical indicators using the [Polars](https://pola.rs/) DataFrame library.

## Features

This library provides functions to calculate various technical indicators from OHLCV (Open, High, Low, Close, Volume) data stored in Polars DataFrames.

**Implemented Indicators:**

*   **Moving Averages:**
    *   Simple Moving Average (SMA) - `calculate_sma`
    *   Exponential Moving Average (EMA) - `calculate_ema`
    *   Weighted Moving Average (WMA) - `calculate_wma`
*   **Oscillators:**
    *   Relative Strength Index (RSI) - `calculate_rsi`
    *   Moving Average Convergence Divergence (MACD) - `calculate_macd` (returns MACD line and Signal line)
*   **Volatility:**
    *   Bollinger Bands (Middle, Upper, Lower) - `calculate_bollinger_bands`
    *   Bollinger Bands %B - `calculate_bb_b`
    *   Average True Range (ATR) - `calculate_atr`
    *   Garman-Klass Volatility - `calculate_gk_volatility`
*   **Volume:**
    *   On-Balance Volume (OBV) - `calculate_obv`
*   **Other Features:**
    *   Price Returns (`returns`)
    *   Daily Price Range (`price_range`)
    *   Lagged Close Prices (`close_lag_5`, `close_lag_15`, `close_lag_30`)
    *   Rolling Returns (`returns_5min`)
    *   Rolling Volatility (`volatility_15min`)
    *   Cyclical Time Features (if a 'time' column exists)

**Convenience Function:**

*   `add_technical_indicators`: A function that takes a mutable DataFrame and adds a standard set of indicators and features (SMA, EMA, RSI, MACD, Bollinger Bands, %B, ATR, GK Volatility, returns, price range, lags, etc.).

**Planned Indicators (Not Yet Implemented):**

*   Chaikin Money Flow (CMF)
*   Average Directional Index (ADX)
*   Rate of Change (ROC)

## Installation

Add this library to your `Cargo.toml`:

```toml
[dependencies]
technical-indicators = { git = "path/to/your/repo" } # Or version = "x.y.z" if published
polars = { version = "...", features = ["lazy", "dtype-full"] } # Ensure you have polars
```

## Usage

```rust
use polars::prelude::*;
use technical_indicators::{calculate_sma, add_technical_indicators}; // Assuming crate name is technical_indicators

fn main() -> PolarsResult<()> {
    // Assume df is a Polars DataFrame with "close", "high", "low", "open", "volume" columns
    let mut df = DataFrame::new(vec![
        Series::new("close", &[10.0, 11.0, 12.0, 11.5, 12.5]),
        // ... other OHLCV columns
    ])?;

    // Calculate a single indicator
    let sma_10 = calculate_sma(&df, "close", 10)?;
    df.with_column(sma_10)?;

    // Or add a suite of indicators
    df = add_technical_indicators(&mut df)?;

    println!("{}", df);
    Ok(())
}
```

## Running Tests

```bash
cargo test
```

## Contributing

Contributions are welcome! Feel free to open issues or submit pull requests, especially for the planned indicators.

## License
