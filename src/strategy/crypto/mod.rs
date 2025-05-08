//! # Cryptocurrency Trading Strategies
//! 
//! This module provides strategies optimized for cryptocurrency markets.
//! 
//! ## Available Strategies
//! 
//! - [`momentum`](momentum/index.html): Momentum-based strategies for crypto markets
//! - [`market_neutral`](market_neutral/index.html): Delta-neutral and market-neutral crypto strategies
//! - [`arbitrage`](arbitrage/index.html): Cross-exchange and cross-chain arbitrage strategies
//! - [`grid_trading`](grid_trading/index.html): Grid trading strategies for volatile crypto markets

pub mod momentum;
pub mod market_neutral;
pub mod arbitrage;
pub mod grid_trading;

// Re-export common types and functions for convenient access
pub use momentum::StrategyParams as MomentumParams;
pub use market_neutral::StrategyParams as MarketNeutralParams;
pub use arbitrage::StrategyParams as ArbitrageParams;
pub use grid_trading::StrategyParams as GridTradingParams; 