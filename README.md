# RusTalib, the Crustacean Financial Analyst 🦀

Meet **Rustalib**, your steadfast crustacean companion for navigating the currents of financial markets! This comprehensive Rust library, `rusttalib`, provides a powerful toolkit for calculating technical indicators, all powered by the high-performance [Polars](https://pola.rs/) DataFrame library.

Whether you're charting, backtesting, or building live trading systems, Rustalib is here to help you process market data with speed and precision.

![Technical Indicators Library Icon](images_processed.png) [![crates.io](https://img.shields.io/crates/v/rusttalib.svg)](https://crates.io/crates/rusttalib)
[![docs.rs](https://docs.rs/rusttalib/badge.svg)](https://docs.rs/rusttalib)

---

## Project Overview

**rusttalib** provides a robust, extensible, and efficient toolkit for quantitative finance, algorithmic trading, and data science in Rust. The library is designed for:
- **Fast, vectorized computation** using Polars DataFrames
- **Easy integration** with modern Rust data workflows
- **Modular design**: Use only the indicators you need
- **Extensibility**: Add your own indicators easily

Whether you are backtesting, researching, or building production trading systems, this crate offers a solid foundation for technical analysis in Rust.

---

## Features

- **Wide range of indicators**: Moving averages, oscillators, volatility, volume, trend, momentum, and more
- **Convenience functions**: Add a suite of indicators to your DataFrame in one call
- **CSV and DataFrame workflows**: Read, process, and save data efficiently
- **Well-documented and tested**

### Implemented Indicators

- **Moving Averages**: SMA, EMA, WMA
- **Oscillators**: RSI, MACD (line & signal)
- **Volatility**: Bollinger Bands, %B, ATR, Garman-Klass Volatility
- **Volume**: On-Balance Volume (OBV), Chaikin Money Flow (CMF)
- **Other**: Price returns, daily range, lagged prices, rolling returns/volatility, cyclical time features

---

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
rusttalib = "*" # Or specify a version
polars = { version = "0.46", features = ["lazy", "dtype-full"] }
```

- **Minimum Rust version:** 1.70+
- **Polars compatibility:** 0.46+

---

## Usage Examples

### 1. Calculate a Simple Moving Average (SMA)
```rust
use polars::prelude::*;
use ta_lib_in_rust::indicators::moving_averages::calculate_sma;

fn main() -> PolarsResult<()> {
    let close = Series::new("close", &[10.0, 11.0, 12.0, 11.5, 12.5]);
    let mut df = DataFrame::new(vec![close.into()])?;
    let sma_3 = calculate_sma(&df, "close", 3)?;
    df.with_column(sma_3)?;
    println!("{}", df);
    Ok(())
}
```

### 2. Combine Multiple Indicators
```rust
use polars::prelude::*;
use ta_lib_in_rust::indicators::{
    moving_averages::calculate_ema,
    oscillators::calculate_rsi,
    volatility::calculate_bollinger_bands,
};

fn main() -> PolarsResult<()> {
    let close = Series::new("close", &[100.0, 102.0, 104.0, 103.0, 105.0]);
    let mut df = DataFrame::new(vec![close.clone().into()])?;
    let ema_3 = calculate_ema(&df, "close", 3)?;
    let rsi_3 = calculate_rsi(&df, 3, "close")?;
    let (bb_mid, bb_up, bb_low) = calculate_bollinger_bands(&df, 3, 2.0, "close")?;
    df = df.with_columns([ema_3, rsi_3, bb_mid, bb_up, bb_low])?;
    println!("{}", df);
    Ok(())
}
```

### 3. Reading Data from CSV and Saving Results
```rust
let df = CsvReadOptions::default()
    .with_has_header(true)
    .try_into_reader_with_file_path(Some("data.csv".into()))?
    .finish()?;

// Important: Ensure column names are lowercase for compatibility with indicators
let mut df = df.lazy()
    .select([
        col("Symbol").alias("symbol"),
        col("Timestamp").alias("timestamp"),
        col("Open").alias("open"),
        col("High").alias("high"),
        col("Low").alias("low"),
        col("Close").alias("close"),
        col("Volume").cast(DataType::Float64).alias("volume"),
    ])
    .collect()?;

// ... apply indicators ...
CsvWriter::new(std::io::BufWriter::new(std::fs::File::create("results.csv")?))
    .finish(&mut df)?;
```

---

## Advanced Examples

See the [`examples/`](examples/) directory for:
- **Basic indicator usage** (SMA, EMA, RSI, MACD, Bollinger Bands, etc.)
- **CSV workflows** for real-world data
- **Multi-stock analysis** with cross-asset comparisons
- **Saving and analyzing results**

## Important Notes

### Column Name Sensitivity
This library expects lowercase column names (`open`, `high`, `low`, `close`, `volume`) in DataFrames. When working with CSVs that might have different case formats (e.g., `Open`, `High`, etc.), make sure to rename the columns using Polars' selection and aliasing capabilities as shown in the examples above.

---

## Contributing

Contributions are welcome! Please:
- Open issues for bugs, questions, or feature requests
- Submit pull requests for new indicators or improvements
- Follow Rust best practices and add tests/docs for new code

---

## Links
- [Crates.io](https://crates.io/crates/rusttalib)
- [Documentation (docs.rs)](https://docs.rs/rusttalib)
- [GitHub Repository](https://github.com/rustic-ml/rusttalib)

## License

MIT License. See [LICENSE](LICENSE) for details.
