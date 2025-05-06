# Technical Indicators (Rust)

![Technical Indicators Library Icon](images.png)

[![crates.io](https://img.shields.io/crates/v/ta-lib-in-rust.svg)](https://crates.io/crates/ta-lib-in-rust)
[![docs.rs](https://docs.rs/ta-lib-in-rust/badge.svg)](https://docs.rs/ta-lib-in-rust)

A comprehensive Rust library for calculating financial technical indicators and building trading strategies, leveraging the [Polars](https://pola.rs/) DataFrame library for high-performance data analysis.

---

## Project Overview

**Technical Indicators** aims to provide a robust, extensible, and efficient toolkit for quantitative finance, algorithmic trading, and data science in Rust. The library is designed for:
- **Fast, vectorized computation** using Polars DataFrames
- **Easy integration** with modern Rust data workflows
- **Modular design**: Use only the indicators or strategies you need
- **Extensibility**: Add your own indicators or strategies easily

Whether you are backtesting, researching, or building production trading systems, this crate offers a solid foundation for technical analysis in Rust.

---

## Features

- **Wide range of indicators**: Moving averages, oscillators, volatility, volume, trend, momentum, and more
- **Strategy modules**: Combine indicators into rule-based trading strategies
- **Convenience functions**: Add a suite of indicators to your DataFrame in one call
- **CSV and DataFrame workflows**: Read, process, and save data efficiently
- **Well-documented and tested**

### Implemented Indicators

- **Moving Averages**: SMA, EMA, WMA
- **Oscillators**: RSI, MACD (line & signal)
- **Volatility**: Bollinger Bands, %B, ATR, Garman-Klass Volatility
- **Volume**: On-Balance Volume (OBV)
- **Other**: Price returns, daily range, lagged prices, rolling returns/volatility, cyclical time features

### Planned/Upcoming
- Chaikin Money Flow (CMF)
- Average Directional Index (ADX)
- Rate of Change (ROC)

---

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
ta-lib-in-rust = "*" # Or specify a version
tokio = { version = "1", features = ["full"] } # If using async examples
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

### 3. Run a Strategy and Analyze Results
```rust
use polars::prelude::*;
use ta_lib_in_rust::strategy::minute::enhanced_minute_strategy::{
    run_strategy, calculate_performance, StrategyParams
};

fn main() -> PolarsResult<()> {
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some("examples/AAPL_minute_ohlcv.csv".into()))?
        .finish()?;
    let params = StrategyParams::default();
    let signals = run_strategy(&df, &params)?;
    let (
        final_value, total_return, num_trades, win_rate, max_drawdown, profit_factor, avg_profit_per_trade
    ) = calculate_performance(
        df.column("close")?,
        &signals.buy_signals,
        &signals.sell_signals,
        &signals.stop_levels,
        &signals.target_levels,
        10000.0,
        true,
    );
    println!("Final Value: ${:.2}, Total Return: {:.2}%", final_value, total_return);
    Ok(())
}
```

### 4. Reading Data from CSV and Saving Results
```rust
let df = CsvReadOptions::default()
    .with_has_header(true)
    .try_into_reader_with_file_path(Some("data.csv".into()))?
    .finish()?;
let mut df = df.lazy()
    .with_columns([col("volume").cast(DataType::Float64)])
    .collect()?;
// ... apply indicators or strategies ...
CsvWriter::new(std::io::BufWriter::new(std::fs::File::create("results.csv")?))
    .finish(&mut df)?;
```

---

## Advanced Examples

See the [`examples/`](examples/) directory for:
- **Basic indicator usage** (SMA, EMA, RSI, MACD, Bollinger Bands, etc.)
- **Strategy backtests** (minute and daily)
- **CSV workflows** for real-world data
- **Saving and analyzing results**

---

## Contributing

Contributions are welcome! Please:
- Open issues for bugs, questions, or feature requests
- Submit pull requests for new indicators, strategies, or improvements
- Follow Rust best practices and add tests/docs for new code

---

## Links
- [Crates.io](https://crates.io/crates/ta-lib-in-rust)
- [Documentation (docs.rs)](https://docs.rs/ta-lib-in-rust)
- [GitHub Repository](https://github.com/rustic-ml/ta-lib-in-rust)

## License

MIT License. See [LICENSE](LICENSE) for details.
