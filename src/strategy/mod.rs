//! # Trading Strategies
//!
//! This module provides specialized trading strategies organized by asset class.
//! Each asset class has its own set of optimized strategies that leverage 
//! asset-specific indicators and characteristics.
//!
//! ## Asset-Specific Strategy Modules
//!
//! - [`stock`](stock/index.html): Strategies for stock markets
//! - [`options`](options/index.html): Options trading strategies
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
//! use ta_lib_in_rust::strategy::stock::trend_following::{run_strategy, StrategyParams};
//!
//! fn main() -> Result<(), PolarsError> {
//!     // Create or load a DataFrame with OHLCV data
//!     let df = DataFrame::default(); // Replace with actual data loading
//!     
//!     // Configure strategy parameters
//!     let params = StrategyParams::default();
//!     
//!     // Run the strategy
//!     let signals = run_strategy(&df, &params)?;
//!     
//!     // Analyze the signals
//!     let close_prices = df.column("close")?;
//!     let (final_value, return_pct, num_trades, win_rate, max_drawdown, profit_factor) =
//!         ta_lib_in_rust::strategy::stock::trend_following::calculate_performance(
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

// Asset-specific strategy modules
pub mod stock;
pub mod options;

// Re-export commonly used stock strategies
pub use stock::trend_following;
pub use stock::mean_reversion;
pub use stock::breakout;
pub use stock::volume_based;

// Re-export commonly used options strategies
pub use options::vertical_spreads;
pub use options::iron_condor;
pub use options::volatility_strategies;
pub use options::delta_neutral;

