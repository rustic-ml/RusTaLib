// Technical indicators module
//
// This module contains implementations of various technical indicators
// used in financial analysis and algorithmic trading.

pub mod moving_averages;
// Choose one of the oscillators module paths to avoid ambiguity
// Either keep the file or the directory, but not both
pub mod cycle;
pub mod math;
pub mod momentum;
pub mod oscillators; // Using the file version, delete the directory or rename it
pub mod pattern_recognition;
pub mod price_transform;
pub mod stats;
pub mod trend;
pub mod volatility;
pub mod volume;

// Re-export commonly used indicators
pub use cycle::*;
pub use math::*;
pub use momentum::*;
pub use moving_averages::*;
pub use oscillators::*;
pub use pattern_recognition::*;
pub use price_transform::*;
pub use stats::*;
pub use trend::*;
pub use volatility::*;
pub use volume::*;

// Function to add all technical indicators to a DataFrame
pub mod add_indicators;
