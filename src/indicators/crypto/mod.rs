//! # Cryptocurrency Market Indicators
//! 
//! This module provides indicators specialized for cryptocurrency markets.
//! 
//! ## Available Indicator Groups
//! 
//! - [`blockchain_metrics`](blockchain_metrics/index.html): Indicators based on on-chain data and blockchain metrics
//! - [`market_sentiment`](market_sentiment/index.html): Indicators measuring crypto market sentiment and fear/greed

pub mod blockchain_metrics;
pub mod market_sentiment;

// Re-export common types and functions for convenient access
pub use blockchain_metrics::OnChainMetrics;
pub use market_sentiment::SentimentIndicators; 