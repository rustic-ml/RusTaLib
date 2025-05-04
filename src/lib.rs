//! # Technical Indicators
//!
//! A comprehensive Rust library for calculating financial technical indicators
//! using the [Polars](https://pola.rs/) DataFrame library.
//!
//! This crate provides functions to calculate various technical indicators
//! from OHLCV (Open, High, Low, Close, Volume) data stored in Polars DataFrames.
//!
//! ## Categories
//!
//! The indicators are organized into the following categories:
//!
//! - **Moving Averages**: Trend-following indicators that smooth price data
//! - **Oscillators**: Indicators that fluctuate within a bounded range
//! - **Volatility**: Indicators that measure the rate of price movement
//! - **Volume**: Indicators based on trading volume
//! - **Trend**: Indicators designed to identify market direction
//! - **Momentum**: Indicators that measure the rate of price change
//! - **Strategy**: Trading strategies combining multiple indicators
//!
//! ## Usage Examples
//!
//! ### Basic Indicator Calculation
//!
//! ```rust
//! use polars::prelude::*;
//! use ta_lib_in_rust::indicators::moving_averages::calculate_sma;
//!
//! fn main() -> PolarsResult<()> {
//!     let close_prices = Series::new(
//!         "close".into(),
//!         &[
//!             100.0, 101.0, 102.0, 103.0, 105.0, 104.0, 106.0, 107.0, 109.0, 108.0,
//!             107.0, 109.0, 111.0, 114.0, 113.0, 116.0, 119.0, 120.0, 119.0, 117.0,
//!             118.0, 120.0, 123.0, 122.0, 120.0, 118.0, 119.0, 121.0, 124.0, 125.0,
//!         ],
//!     );
//!     // Create a sample DataFrame with price data
//!     let mut df = DataFrame::new(vec![close_prices.clone().into()])?;
//!
//!     // Calculate a Simple Moving Average
//!     let sma_10 = calculate_sma(&df, "close", 10)?;
//!     df.with_column(sma_10)?;
//!
//!     println!("{}", df);
//!     Ok(())
//! }
//! ```
//!
//! ### Combining Multiple Indicators
//!
//! ```rust
//! use polars::prelude::*;
//! use ta_lib_in_rust::indicators::{
//!     moving_averages::{calculate_sma, calculate_ema},
//!     oscillators::calculate_rsi,
//!     volatility::calculate_bollinger_bands,
//! };
//!
//! fn main() -> PolarsResult<()> {
//!     // Create a DataFrame with price data
//!     let close = Series::new("close", &[100.0, 102.0, 104.0, 103.0, 105.0, 107.0, 108.0,
//!                                        107.0, 106.0, 105.0, 107.0, 108.0, 109.0, 110.0]);
//!     let high = Series::new("high", &[101.0, 103.0, 104.5, 103.5, 106.0, 107.5, 108.5,
//!                                      107.2, 106.5, 105.5, 107.5, 108.5, 109.5, 111.0]);
//!     let low = Series::new("low", &[99.0, 101.5, 103.0, 102.0, 104.0, 106.0, 107.0,
//!                                    106.0, 105.0, 104.0, 106.0, 107.5, 108.5, 109.0]);
//!     let mut df = DataFrame::new(vec![close.clone(), high.clone(), low.clone()])?;
//!     
//!     // Calculate multiple indicators
//!     let sma_5 = calculate_sma(&df, "close", 5)?;
//!     let ema_8 = calculate_ema(&df, "close", 8)?;
//!     let rsi_7 = calculate_rsi(&df, 7, "close")?;
//!     let (bb_middle, bb_upper, bb_lower) = calculate_bollinger_bands(&df, 10, 2.0, "close")?;
//!     
//!     // Add all indicators to the DataFrame
//!     df = df.with_columns([
//!         sma_5, ema_8, rsi_7, bb_middle, bb_upper, bb_lower
//!     ])?;
//!     
//!     // Create custom trading signals
//!     let bullish_signal = df.clone()
//!         .lazy()
//!         .with_columns([
//!             (col("close").gt(col("sma_5"))
//!              & col("close").gt(col("bb_middle"))
//!              & col("rsi_7").gt(lit(50.0)))
//!             .alias("bullish")
//!         ])
//!         .collect()?;
//!     
//!     println!("DataFrame with indicators and signals:");
//!     println!("{}", bullish_signal);
//!     
//!     Ok(())
//! }
//! ```
//!
//! ### Running a Trading Strategy Backtest
//!
//! ```rust
//! use polars::prelude::*;
//! use ta_lib_in_rust::strategy::daily::multi_indicator_daily_1::{
//!     run_strategy, calculate_performance, StrategyParams
//! };
//!
//! fn main() -> PolarsResult<()> {
//!     // Load data from CSV
//!     let df = CsvReadOptions::default()
//!         .with_has_header(true)
//!         .try_into_reader_with_file_path(Some("path/to/ohlcv_data.csv".into()))?
//!         .finish()?;
//!
//!     // Set strategy parameters
//!     let params = StrategyParams {
//!         sma_short_period: 20,
//!         sma_long_period: 50,
//!         rsi_period: 14,
//!         rsi_overbought: 70.0,
//!         rsi_oversold: 30.0,
//!         bb_period: 20,
//!         bb_std_dev: 2.0,
//!         macd_fast: 12,
//!         macd_slow: 26,
//!         macd_signal: 9,
//!         min_signals_for_buy: 3,
//!         min_signals_for_sell: 3,
//!     };
//!
//!     // Run the strategy to generate buy and sell signals
//!     let signals = run_strategy(&df, &params)?;
//!
//!     // Calculate performance metrics
//!     let close_series = df.column("close")?;
//!     let (
//!         final_value,     // Final portfolio value
//!         total_return,    // Total return percentage
//!         num_trades,      // Number of trades executed
//!         win_rate,        // Percentage of winning trades
//!         max_drawdown,    // Maximum drawdown percentage
//!         profit_factor,   // Ratio of gross profit to gross loss
//!     ) = calculate_performance(
//!         close_series,
//!         &signals.buy_signals,
//!         &signals.sell_signals,
//!         10000.0,  // Initial capital
//!     );
//!
//!     println!("Strategy Backtest Results:");
//!     println!("Final Portfolio Value: ${:.2}", final_value);
//!     println!("Total Return: {:.2}%", total_return);
//!     println!("Number of Trades: {}", num_trades);
//!     println!("Win Rate: {:.2}%", win_rate);
//!     println!("Maximum Drawdown: {:.2}%", max_drawdown * 100.0);
//!     println!("Profit Factor: {:.2}", profit_factor);
//!
//!     Ok(())
//! }
//! ```
//!
//! ### Comparing Multiple Strategies
//!
//! ```rust
//! use polars::prelude::*;
//! use ta_lib_in_rust::strategy::minute::{
//!     multi_indicator_minute_1,
//!     multi_indicator_minute_2,
//!     multi_indicator_minute_3
//! };
//!
//! fn main() -> PolarsResult<()> {
//!     // Load minute data from CSV
//!     let mut df = CsvReadOptions::default()
//!         .with_has_header(true)
//!         .try_into_reader_with_file_path(Some("path/to/minute_data.csv".into()))?
//!         .finish()?;
//!
//!     // Cast volume to float if needed
//!     df = df.lazy()
//!         .with_columns([
//!             col("volume").cast(DataType::Float64),
//!         ])
//!         .collect()?;
//!
//!     // Initialize strategies with default parameters
//!     let params1 = multi_indicator_minute_1::StrategyParams::default();
//!     let params2 = multi_indicator_minute_2::StrategyParams::default();
//!     let params3 = multi_indicator_minute_3::StrategyParams::default();
//!
//!     // Run strategies
//!     let signals1 = multi_indicator_minute_1::run_strategy(&df, &params1)?;
//!     let signals2 = multi_indicator_minute_2::run_strategy(&df, &params2)?;
//!     let signals3 = multi_indicator_minute_3::run_strategy(&df, &params3)?;
//!
//!     // Calculate performance for each strategy
//!     let close_prices = df.column("close")?;
//!
//!     let (final_value1, return1, trades1, win_rate1, drawdown1, _, _) =
//!         multi_indicator_minute_1::calculate_performance(
//!             close_prices,
//!             &signals1.buy_signals,
//!             &signals1.sell_signals,
//!             10000.0,
//!             true
//!         );
//!
//!     let (final_value2, return2, trades2, win_rate2, drawdown2, _, _) =
//!         multi_indicator_minute_2::calculate_performance(
//!             close_prices,
//!             &signals2.buy_signals,
//!             &signals2.sell_signals,
//!             &signals2.position_sizes,
//!             10000.0,
//!             true,
//!             None
//!         );
//!
//!     // Compare results
//!     println!("Strategy Comparison:");
//!     println!("Metric      | Strategy 1    | Strategy 2");
//!     println!("------------------------------------");
//!     println!("Return      | {:.2}%        | {:.2}%", return1, return2);
//!     println!("Final Value | ${:.2}      | ${:.2}", final_value1, final_value2);
//!     println!("Win Rate    | {:.2}%        | {:.2}%", win_rate1, win_rate2);
//!     println!("Max Drawdown| {:.2}%        | {:.2}%", drawdown1*100.0, drawdown2*100.0);
//!     
//!     // Determine best performer
//!     if return1 > return2 {
//!         println!("\nStrategy 1 performed better on absolute return");
//!     } else {
//!         println!("\nStrategy 2 performed better on absolute return");
//!     }
//!     
//!     // Risk-adjusted comparison
//!     let risk_adjusted1 = return1 / (drawdown1 * 100.0);
//!     let risk_adjusted2 = return2 / (drawdown2 * 100.0);
//!     
//!     if risk_adjusted1 > risk_adjusted2 {
//!         println!("Strategy 1 performed better on risk-adjusted basis");
//!     } else {
//!         println!("Strategy 2 performed better on risk-adjusted basis");
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! See the documentation for each module for more detailed information and examples.

pub mod indicators;
pub mod strategy;
pub mod util;

// Re-export commonly used items
pub use indicators::*;
pub use strategy::*;

// This is a placeholder function - should be removed before final release
pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
