# Technical Analysis and Trading Strategy Examples

This directory contains examples showing how to use the technical indicators and trading strategies provided by this library.

## Directory Structure

The examples are organized by category:

- `general/` - Basic technical indicators that apply to all markets
- `stock/` - Stock market specific strategies and indicators
- `options/` - Options trading strategies and volatility analysis
- `csv/` - Sample CSV data files for testing

## General Technical Indicators

Basic examples showing fundamental technical analysis concepts:

- `general/basic_indicators.rs` - Shows how to calculate and interpret various technical indicators including moving averages, oscillators, and Bollinger Bands

## Stock Market Strategies

Stock market specific trading strategies:

- `stock/trend_following.rs` - Demonstrates a trend following strategy using EMAs and RSI
- `stock/mean_reversion.rs` - Shows a mean reversion strategy based on Z-score
- `stock/breakout.rs` - Implements a breakout strategy with volume confirmation
- `stock/volume_based.rs` - Shows volume-based strategies for stock trading

## Options Trading Strategies

Options market specific trading strategies:

- `options/vertical_spreads.rs` - Demonstrates vertical spread strategies (bull put and bear call spreads)
- `options/iron_condor.rs` - Shows how to implement iron condor strategies for range-bound markets
- `options/volatility_strategies.rs` - Demonstrates volatility-based options strategies
- `options/delta_neutral.rs` - Shows delta-neutral options strategies implementation

## Multi-Asset Analysis Examples

Examples for processing and analyzing multiple assets:

- `working_with_multi_stock_data.rs` - Demonstrates how to load, process, and compare technical indicators across multiple stocks. Shows how to handle data from different CSV sources, standardize column formats, calculate key technical indicators, and perform cross-stock comparison analysis.

## Running the Examples

To run any example, use the following command from the project root:

```bash
cargo run --example <folder>/<example_name>
```

For instance:

```bash
cargo run --example general/basic_indicators
cargo run --example stock/trend_following
cargo run --example working_with_multi_stock_data
```

## Notes for Real-World Application

These examples use synthetic data for demonstration purposes. In real-world applications, you should:

- Import actual market data (CSV files, APIs, etc.)
- Apply proper backtesting methodology with realistic assumptions
- Consider transaction costs, slippage, and market impact
- Implement proper position sizing and risk management
- Test strategies across different market conditions
- Consider regulatory and tax implications

Each example provides a simplified implementation to demonstrate the concepts. In production trading systems, more sophisticated implementations would be required. 