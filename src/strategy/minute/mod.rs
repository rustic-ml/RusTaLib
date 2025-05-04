//! # Minute Trading Strategies
//!
//! This module contains different multi-indicator trading strategies designed for minute timeframe analysis.
//!
//! Each strategy represents a different approach to intraday trading using technical indicators:
//!
//! - `multi_indicator_minute_1`: Standard multi-indicator strategy with fast parameters for intraday trading
//! - `multi_indicator_minute_2`: Volatility-focused strategy with adaptive position sizing
//! - `multi_indicator_minute_3`: Momentum-focused strategy with dynamic thresholds based on market hours
//! - `multi_indicator_minute_4`: Hybrid adaptive strategy with trailing stops and advanced risk management
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use polars::prelude::*;
//! use ta_lib_in_rust::strategy::minute::multi_indicator_minute_1;
//!
//! fn main() -> Result<(), PolarsError> {
//!     // Create or load a DataFrame with minute OHLCV data
//!     let df = DataFrame::default(); // Replace with actual data loading
//!     
//!     // Use default parameters for the strategy
//!     let params = multi_indicator_minute_1::StrategyParams::default();
//!     
//!     // Run the strategy
//!     let signals = multi_indicator_minute_1::run_strategy(&df, &params)?;
//!     
//!     // Work with the signals
//!     println!("Buy signals: {:?}", signals.buy_signals);
//!     
//!     Ok(())
//! }
//! ```

pub mod enhanced_minute_strategy;
pub mod multi_indicator_minute_1;
pub mod multi_indicator_minute_2;
pub mod multi_indicator_minute_3;
pub mod multi_indicator_minute_4;

pub use multi_indicator_minute_1::{
    run_strategy as run_strategy_1, StrategyParams as StrategyParams1,
};
pub use multi_indicator_minute_2::{
    run_strategy as run_strategy_2, StrategyParams as StrategyParams2,
};
pub use multi_indicator_minute_3::{
    run_strategy as run_strategy_3, StrategyParams as StrategyParams3,
};
pub use multi_indicator_minute_4::{
    run_strategy as run_strategy_4, StrategyParams as StrategyParams4,
};
