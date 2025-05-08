//! # Options Trading Strategies
//! 
//! This module provides strategies optimized for options markets.
//! 
//! ## Available Strategies
//! 
//! - [`vertical_spreads`](vertical_spreads/index.html): Bull and bear spread strategies using vertical call and put spreads
//! - [`iron_condor`](iron_condor/index.html): Market-neutral strategies using iron condors and other multi-leg spreads
//! - [`volatility_strategies`](volatility_strategies/index.html): Strategies that capitalize on volatility movements using straddles and strangles
//! - [`delta_neutral`](delta_neutral/index.html): Delta-neutral strategies that manage directional risk

pub mod vertical_spreads;
pub mod iron_condor;
pub mod volatility_strategies;
pub mod delta_neutral;

// Re-export common types and functions for convenient access
pub use vertical_spreads::StrategyParams as VerticalSpreadParams;
pub use iron_condor::StrategyParams as IronCondorParams;
pub use volatility_strategies::StrategyParams as VolatilityParams;
pub use delta_neutral::StrategyParams as DeltaNeutralParams; 