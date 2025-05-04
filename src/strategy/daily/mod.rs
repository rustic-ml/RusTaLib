//! # Daily Trading Strategies
//!
//! This module contains different multi-indicator trading strategies designed for daily timeframe analysis.
//!
//! Each strategy represents a different approach to combining technical indicators:
//!
//! - `multi_indicator_daily_1`: Standard multi-indicator strategy with fixed rules
//! - `multi_indicator_daily_2`: Volatility-focused strategy with adaptive position sizing
//! - `multi_indicator_daily_3`: Adaptive trend-filtered strategy with dynamic position sizing
//! - `multi_indicator_daily_4`: Hybrid adaptive strategy with trailing stops and risk management
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use polars::prelude::*;
//! use ta_lib_in_rust::strategy::daily::multi_indicator_daily_1;
//!
//! fn main() -> Result<(), PolarsError> {
//!     // Create or load a DataFrame with OHLCV data
//!     let df = DataFrame::default(); // Replace with actual data loading
//!     
//!     // Use default parameters for the strategy
//!     let params = multi_indicator_daily_1::StrategyParams::default();
//!     
//!     // Run the strategy
//!     let signals = multi_indicator_daily_1::run_strategy(&df, &params)?;
//!     
//!     // Work with the signals
//!     println!("Buy signals: {:?}", signals.buy_signals);
//!     
//!     Ok(())
//! }
//! ```

pub mod multi_indicator_daily_1;
pub mod multi_indicator_daily_2;
pub mod multi_indicator_daily_3;
pub mod multi_indicator_daily_4;

pub use multi_indicator_daily_1::{
    run_strategy as run_strategy_1, StrategyParams as StrategyParams1,
};
pub use multi_indicator_daily_2::{
    run_strategy as run_strategy_222, StrategyParams as StrategyParams222,
};
pub use multi_indicator_daily_3::{
    run_strategy as run_strategy_3, StrategyParams as StrategyParams3,
};
pub use multi_indicator_daily_4::{
    run_strategy as run_strategy_4, StrategyParams as StrategyParams4,
};
