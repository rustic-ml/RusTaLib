//! # Stock Trading Strategies
//! 
//! This module provides strategies optimized for stock/equity markets.
//! 
//! ## Available Strategies
//! 
//! - [`trend_following`](trend_following/index.html): Strategies that follow market trends using moving averages and momentum
//! - [`mean_reversion`](mean_reversion/index.html): Strategies that capitalize on price reversions to the mean
//! - [`breakout`](breakout/index.html): Strategies that identify and trade price breakouts from consolidation patterns
//! - [`volume_based`](volume_based/index.html): Strategies that use volume analysis as a primary decision factor

pub mod trend_following;
pub mod mean_reversion;
pub mod breakout;
pub mod volume_based;

// Re-export common types and functions for convenient access
pub use trend_following::StrategyParams as TrendFollowingParams;
pub use mean_reversion::StrategyParams as MeanReversionParams;
pub use breakout::StrategyParams as BreakoutParams;
pub use volume_based::StrategyParams as VolumeBasedParams; 