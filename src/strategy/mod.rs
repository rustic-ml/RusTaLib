//! # Trading Strategies
//!
//! This module provides a collection of trading strategies that combine multiple technical indicators
//! for making trading decisions.
//!
//! ## Available Strategy Types
//!
//! - [`daily`](daily/index.html): Strategies optimized for daily timeframe analysis
//! - [`minute`](minute/index.html): Strategies optimized for minute timeframe (intraday) analysis
//!
//! Each strategy module typically provides:
//!
//! - A `StrategyParams` struct for configuring the strategy parameters
//! - A `run_strategy` function that applies the strategy to price data
//! - A `calculate_performance` function to evaluate the strategy's performance
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use polars::prelude::*;
//! use ta_lib_in_rust::strategy::{run_strategy_1, StrategyParams1};
//!
//! fn main() -> Result<(), PolarsError> {
//!     // Create or load a DataFrame with OHLCV data
//!     let df = DataFrame::default(); // Replace with actual data loading
//!     
//!     // Use default parameters for strategy 1
//!     let params = StrategyParams1::default();
//!     
//!     // Run the strategy
//!     let signals = run_strategy_1(&df, &params)?;
//!     
//!     // Analyze the signals
//!     let close_prices = df.column("close")?;
//!     let (final_value, return_pct, num_trades, win_rate, max_drawdown, profit_factor) =
//!         ta_lib_in_rust::strategy::daily::multi_indicator_daily_1::calculate_performance(
//!             close_prices,
//!             &signals.buy_signals,
//!             &signals.sell_signals,
//!             10000.0 // Initial capital
//!         );
//!     
//!     println!("Strategy return: {:.2}%", return_pct);
//!     println!("Win rate: {:.2}%", win_rate);
//!     
//!     Ok(())
//! }
//! ```

pub mod daily;
pub mod minute;

// Re-export daily strategies
pub use daily::multi_indicator_daily_1::{
    run_strategy as run_strategy_1, StrategyParams as StrategyParams1,
};
pub use daily::multi_indicator_daily_2::{
    run_strategy as run_strategy_222, StrategyParams as StrategyParams222,
};
pub use daily::multi_indicator_daily_3::{
    run_strategy as run_strategy_3, StrategyParams as StrategyParams3,
};
pub use daily::multi_indicator_daily_4::{
    run_strategy as run_strategy_4, StrategyParams as StrategyParams4,
};

// Re-export minute strategies with shorter names for direct access
pub use minute::multi_indicator_minute_1 as minute_1;
pub use minute::multi_indicator_minute_2 as minute_2;
pub use minute::multi_indicator_minute_3 as minute_3;
pub use minute::multi_indicator_minute_4 as minute_4;
pub use minute::enhanced_minute_strategy as enhanced_minute;
