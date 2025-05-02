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
//!
//! ## Usage Example
//!
//! ```rust
//! use polars::prelude::*;
//! use technical_indicators::indicators::moving_averages::sma::calculate_sma;
//!
//! fn main() -> PolarsResult<()> {
//! let close_prices = Series::new(
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
//! See the documentation for each module for more detailed information and examples.

pub mod indicators;
pub mod util;

// Re-export commonly used items
pub use indicators::*;

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
