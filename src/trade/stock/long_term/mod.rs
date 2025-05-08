//! # Long-Term Trading Indicators for Stocks
//! 
//! This module provides specialized technical indicators optimized for
//! long-term trading (timeframes of months to years), commonly used in position trading.
//!
//! ## Included Indicators
//!
//! * Trend Analysis - Long-term trend identification tools
//! * Cycle Identification - Market cycle detection for position trading
//! * Fundamental Price Ratio - Price to technical indicator ratios
//! * Secular Trend - Multi-year trend momentum
//! * Value Zones - Long-term support/resistance and value areas

use polars::prelude::*;
use crate::indicators::moving_averages::{calculate_ema, calculate_sma};
use crate::indicators::oscillators::calculate_rsi;

mod trend_analysis;
mod cycle_identification;
mod fundamental_price_ratio;
mod secular_trend;
mod value_zones;

// Re-export the public functions
pub use trend_analysis::*;
pub use cycle_identification::*;
pub use fundamental_price_ratio::*;
pub use secular_trend::*;
pub use value_zones::*;

/// Calculate common long-term trading indicators
///
/// Adds a suite of indicators specifically optimized for position trading
/// and other long-term trading approaches.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
///
/// # Returns
///
/// * `PolarsResult<DataFrame>` - DataFrame with added long-term indicators
pub fn add_long_term_indicators(df: &DataFrame) -> PolarsResult<DataFrame> {
    let mut result = df.clone();
    
    // Add trend analysis
    trend_analysis::add_trend_analysis(&mut result, 200)?;
    
    // Add cycle identification
    cycle_identification::add_cycle_analysis(&mut result)?;
    
    // Add fundamental price ratio analysis
    fundamental_price_ratio::add_price_ratio_analysis(&mut result)?;
    
    // Add secular trend analysis
    secular_trend::add_secular_trend_analysis(&mut result)?;
    
    // Add value zones analysis
    value_zones::add_value_zones_analysis(&mut result)?;
    
    Ok(result)
}

/// Generate combined position trading signals
///
/// This function combines signals from various indicators to generate
/// more robust entry and exit points for long-term position trading.
///
/// # Arguments
///
/// * `df` - DataFrame with previously calculated indicators
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with combined signals (2: strong buy, 1: moderate buy,
///                           0: neutral, -1: moderate sell, -2: strong sell)
pub fn generate_position_trading_signals(df: &DataFrame) -> PolarsResult<Series> {
    // Check if necessary indicators exist
    let required_indicators = [
        "trend_direction", "trend_strength", "cycle_phase", 
        "price_to_ma_ratio", "secular_momentum"
    ];
    
    for indicator in required_indicators {
        if !df.schema().contains(indicator) {
            return Err(PolarsError::ComputeError(
                format!("Required indicator '{}' not found", indicator).into(),
            ));
        }
    }
    
    // Extract indicator values
    let trend_direction = df.column("trend_direction")?.i32()?;
    let trend_strength = df.column("trend_strength")?.f64()?;
    let cycle_phase = df.column("cycle_phase")?.i32()?;
    let price_ratio = df.column("price_to_ma_ratio")?.f64()?;
    let secular_momentum = df.column("secular_momentum")?.i32()?;
    
    // Get value rating if available
    let has_value_rating = df.schema().contains("value_rating");
    let value_rating = if has_value_rating {
        Some(df.column("value_rating")?.i32()?)
    } else {
        None
    };
    
    let mut combined_signals = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let direction = trend_direction.get(i).unwrap_or(0);
        let strength = trend_strength.get(i).unwrap_or(f64::NAN);
        let cycle = cycle_phase.get(i).unwrap_or(0);
        let ratio = price_ratio.get(i).unwrap_or(f64::NAN);
        let momentum = secular_momentum.get(i).unwrap_or(0);
        
        // Skip if we don't have valid data
        if strength.is_nan() || ratio.is_nan() {
            combined_signals.push(0);
            continue;
        }
        
        // Count bullish and bearish signals
        let mut bullish_count = 0;
        let mut bearish_count = 0;
        
        // Trend direction (stronger weight)
        if direction > 0 { bullish_count += 2; }
        if direction < 0 { bearish_count += 2; }
        
        // Cycle phase
        if cycle == 1 || cycle == 2 { bullish_count += 1; } // Accumulation or markup phases
        if cycle == 3 || cycle == 4 { bearish_count += 1; } // Distribution or markdown phases
        
        // Price ratio (higher = overvalued, lower = undervalued)
        if ratio < 0.8 { bullish_count += 1; } // Undervalued
        if ratio > 1.2 { bearish_count += 1; } // Overvalued
        
        // Secular momentum
        if momentum > 0 { bullish_count += 1; }
        if momentum < 0 { bearish_count += 1; }
        
        // Factor in value rating if available
        if let Some(vr) = &value_rating {
            let vr_val = vr.get(i).unwrap_or(0);
            if vr_val >= 4 { bullish_count += 1; } // Strong value
            if vr_val <= 2 { bearish_count += 1; } // Poor value
        }
        
        // Generate combined signal
        if bullish_count >= 3 && bearish_count == 0 {
            combined_signals.push(2); // Strong buy
        } else if bullish_count > bearish_count {
            combined_signals.push(1); // Moderate buy
        } else if bearish_count >= 3 && bullish_count == 0 {
            combined_signals.push(-2); // Strong sell
        } else if bearish_count > bullish_count {
            combined_signals.push(-1); // Moderate sell
        } else {
            combined_signals.push(0); // Neutral
        }
    }
    
    Ok(Series::new("position_trading_signal", combined_signals))
}

/// Calculate position sizing based on conviction level
///
/// This function suggests appropriate position sizes based on
/// the conviction level associated with a long-term position trade setup.
///
/// # Arguments
///
/// * `df` - DataFrame with long-term trading indicators
/// * `base_allocation` - Base portfolio allocation percentage per position
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with suggested position sizes (0.0-1.0)
pub fn calculate_position_sizing(
    df: &DataFrame,
    base_allocation: Option<f64>,
) -> PolarsResult<Series> {
    let base_alloc = base_allocation.unwrap_or(10.0) / 100.0; // Convert to decimal
    
    // Get combined signals
    let signals = match generate_position_trading_signals(df) {
        Ok(s) => s,
        Err(_) => {
            // If combined signals not available, check if at least trend direction exists
            if !df.schema().contains("trend_direction") {
                return Err(PolarsError::ComputeError(
                    "No trading signals found for position sizing".into(),
                ));
            }
            df.column("trend_direction")?.clone()
        }
    };
    
    let signal_vals = signals.i32()?;
    
    // Get trend strength if available
    let has_trend_strength = df.schema().contains("trend_strength");
    let trend_strength = if has_trend_strength {
        Some(df.column("trend_strength")?.f64()?)
    } else {
        None
    };
    
    // Get value rating if available
    let has_value_rating = df.schema().contains("value_rating");
    let value_rating = if has_value_rating {
        Some(df.column("value_rating")?.i32()?)
    } else {
        None
    };
    
    let mut position_sizes = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let signal = signal_vals.get(i).unwrap_or(0);
        
        // No position for neutral signals
        if signal == 0 {
            position_sizes.push(0.0);
            continue;
        }
        
        // Calculate base position size based on signal strength
        let mut size = if signal.abs() == 2 {
            base_alloc * 1.0 // Full size for strong signals
        } else {
            base_alloc * 0.75 // Reduced size for moderate signals
        };
        
        // Adjust based on trend strength if available
        if let Some(strength) = &trend_strength {
            let strength_val = strength.get(i).unwrap_or(f64::NAN);
            if !strength_val.is_nan() {
                if strength_val > 50.0 {
                    size *= 1.0 + (strength_val - 50.0) / 100.0; // Increase size for strong trends
                } else if strength_val < 30.0 {
                    size *= 0.8; // Reduce size for weak trends
                }
            }
        }
        
        // Adjust based on value rating if available
        if let Some(vr) = &value_rating {
            let vr_val = vr.get(i).unwrap_or(3);
            match vr_val {
                5 => size *= 1.2, // Excellent value
                4 => size *= 1.1, // Good value
                2 => size *= 0.9, // Poor value
                1 => size *= 0.8, // Very poor value
                _ => {} // No adjustment for neutral value
            }
        }
        
        // Cap at 25% of portfolio to prevent overallocation
        size = size.min(0.25);
        
        position_sizes.push(size);
    }
    
    Ok(Series::new("position_size", position_sizes))
} 