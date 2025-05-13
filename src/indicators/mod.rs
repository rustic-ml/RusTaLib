//! # Technical Indicators
//!
//! This module provides technical indicators organized in multiple ways:
//!
//! 1. By asset class - specialized indicators optimized for specific markets
//! 2. By indicator type - traditional categorization of technical indicators
//! 3. By timeframe - indicators optimized for different trading timeframes
//!
//! ## Asset-Specific Indicator Modules
//!
//! - [`stock`](stock/index.html): Indicators for stock/equity markets
//! - [`options`](options/index.html): Indicators for options trading
//!
//! ## Traditional Indicator Categories
//!
//! - [`moving_averages`](moving_averages/index.html): Trend-following indicators that smooth price data
//! - [`oscillators`](oscillators/index.html): Indicators that fluctuate within a bounded range
//! - [`volatility`](volatility/index.html): Indicators that measure the rate of price movement
//! - [`volume`](volume/index.html): Indicators based on trading volume
//! - [`trend`](trend/index.html): Indicators designed to identify market direction
//! - [`momentum`](momentum/index.html): Indicators that measure the rate of price change
//! - [`cycle`](cycle/index.html): Indicators that identify cyclical patterns in price
//! - [`pattern_recognition`](pattern_recognition/index.html): Indicators that identify chart patterns
//! - [`price_transform`](price_transform/index.html): Indicators that transform price data
//! - [`stats`](stats/index.html): Statistical indicators
//! - [`math`](math/index.html): Mathematical utility functions
//!
//! ## Timeframe-Specific Indicator Modules
//!
//! - [`day_trading`](day_trading/index.html): Indicators optimized for intraday trading
//! - [`short_term`](short_term/index.html): Indicators optimized for short-term trading (days to weeks)
//! - [`long_term`](long_term/index.html): Indicators optimized for long-term analysis (weeks to months)

// Asset-specific indicator modules
pub mod options;
pub mod stock;

// Traditional indicator category modules
pub mod cycle;
pub mod math;
pub mod momentum;
pub mod moving_averages;
pub mod oscillators;
pub mod pattern_recognition;
pub mod price_transform;
pub mod stats;
pub mod trend;
pub mod volatility;
pub mod volume;

// Timeframe-specific indicator modules
pub mod day_trading;
pub mod long_term;
pub mod short_term;

// Utility modules
pub mod add_indicators;
pub mod test_util;

// Re-export add_technical_indicators function
pub use add_indicators::add_technical_indicators;

// Re-export commonly used indicators for convenient access
pub use momentum::calculate_roc;
pub use moving_averages::{calculate_ema, calculate_sma, calculate_vwap, calculate_wma};
pub use oscillators::{calculate_macd, calculate_rsi};
pub use volatility::{calculate_atr, calculate_bollinger_bands};
pub use volume::{calculate_cmf, calculate_mfi, calculate_obv};

// Re-export asset-specific indicator modules
pub use options::greeks;
pub use options::implied_volatility;
pub use stock::fundamental;
pub use stock::price_action;
