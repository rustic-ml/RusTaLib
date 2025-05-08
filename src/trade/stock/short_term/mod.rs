//! # Short-Term Trading Indicators for Stocks
//! 
//! This module provides specialized technical indicators optimized for
//! short-term trading (timeframes of days to weeks), commonly used in swing trading.
//!
//! ## Included Indicators
//!
//! * Trend Strength Analysis - Enhanced ADX for swing trading
//! * Swing Detection - Identifies potential swing entry points
//! * Multi-Timeframe Analysis - Aligns trends across multiple timeframes
//! * Mean Reversion - Identifies potential reversions to the mean
//! * Support/Resistance Analysis - Finds key levels for swing trades

use polars::prelude::*;
use crate::indicators::moving_averages::calculate_ema;
use crate::indicators::oscillators::calculate_rsi;

mod trend_strength;
mod swing_detection;
mod multi_timeframe;
mod mean_reversion;
mod support_resistance;

// Re-export the public functions
pub use trend_strength::*;
pub use swing_detection::*;
pub use multi_timeframe::*;
pub use mean_reversion::*;
pub use support_resistance::*;

/// Calculate common short-term trading indicators
///
/// Adds a suite of indicators specifically optimized for swing trading
/// and other short-term trading approaches.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
///
/// # Returns
///
/// * `PolarsResult<DataFrame>` - DataFrame with added short-term indicators
pub fn add_short_term_indicators(df: &DataFrame) -> PolarsResult<DataFrame> {
    let mut result = df.clone();
    
    // Add trend strength analysis (enhanced ADX)
    trend_strength::add_trend_strength_analysis(&mut result, 14)?;
    
    // Add swing detection
    swing_detection::add_swing_analysis(&mut result)?;
    
    // Add multi-timeframe analysis
    multi_timeframe::add_multi_timeframe_analysis(&mut result, None, None)?;
    
    // Add mean reversion analysis
    mean_reversion::add_mean_reversion_analysis(&mut result)?;
    
    // Add support and resistance analysis
    support_resistance::add_support_resistance_analysis(&mut result)?;
    
    Ok(result)
}

/// Generate combined swing trading signals
///
/// This function combines signals from various indicators to generate
/// more robust entry and exit points for swing trading.
///
/// # Arguments
///
/// * `df` - DataFrame with previously calculated indicators
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with combined signals (2: strong buy, 1: moderate buy,
///                           0: neutral, -1: moderate sell, -2: strong sell)
pub fn generate_swing_trading_signals(df: &DataFrame) -> PolarsResult<Series> {
    // Check if necessary indicators exist
    let required_indicators = [
        "trend_strength", "trend_classification", "swing_signal", 
        "multi_timeframe_alignment", "mean_reversion_signal"
    ];
    
    for indicator in required_indicators {
        if !df.schema().contains(indicator) {
            return Err(PolarsError::ComputeError(
                format!("Required indicator '{}' not found", indicator).into(),
            ));
        }
    }
    
    // Extract indicator values
    let trend_strength = df.column("trend_strength")?.f64()?;
    let trend_class = df.column("trend_classification")?.i32()?;
    let swing_signal = df.column("swing_signal")?.i32()?;
    let mtf_alignment = df.column("multi_timeframe_alignment")?.i32()?;
    let mean_rev_signal = df.column("mean_reversion_signal")?.i32()?;
    
    // Get risk-reward ratio if available
    let has_risk_reward = df.schema().contains("risk_reward_ratio");
    let risk_reward = if has_risk_reward {
        Some(df.column("risk_reward_ratio")?.f64()?)
    } else {
        None
    };
    
    let mut combined_signals = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let strength = trend_strength.get(i).unwrap_or(f64::NAN);
        let trend = trend_class.get(i).unwrap_or(0);
        let swing = swing_signal.get(i).unwrap_or(0);
        let alignment = mtf_alignment.get(i).unwrap_or(0);
        let mean_rev = mean_rev_signal.get(i).unwrap_or(0);
        
        // Skip if we don't have valid data
        if strength.is_nan() {
            combined_signals.push(0);
            continue;
        }
        
        // Count bullish and bearish signals
        let mut bullish_count = 0;
        let mut bearish_count = 0;
        
        // Trend class (stronger weight)
        if trend > 0 { bullish_count += 2; }
        if trend < 0 { bearish_count += 2; }
        
        // Swing signal
        if swing > 0 { bullish_count += 1; }
        if swing < 0 { bearish_count += 1; }
        
        // MTF alignment
        if alignment > 0 { bullish_count += 1; }
        if alignment < 0 { bearish_count += 1; }
        
        // Mean reversion signal
        if mean_rev > 0 { bullish_count += 1; }
        if mean_rev < 0 { bearish_count += 1; }
        
        // Factor in risk-reward if available
        if let Some(rr) = &risk_reward {
            let rr_val = rr.get(i).unwrap_or(f64::NAN);
            if !rr_val.is_nan() {
                // Add weight for favorable risk-reward ratio
                if rr_val >= 2.0 && bullish_count > bearish_count {
                    bullish_count += 1; // Boost bullish if good risk-reward
                } else if rr_val <= 0.5 && bearish_count > bullish_count {
                    bearish_count += 1; // Boost bearish if poor risk-reward
                }
            }
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
    
    Ok(Series::new("swing_trading_signal", combined_signals))
}

/// Calculate position sizing based on risk level
///
/// This function suggests appropriate position sizes based on
/// the risk level associated with a swing trade setup.
///
/// # Arguments
///
/// * `df` - DataFrame with swing trading indicators
/// * `risk_percentage` - Maximum percentage of portfolio to risk per trade
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with suggested position sizes (0.0-1.0)
pub fn calculate_position_sizing(
    df: &DataFrame,
    risk_percentage: Option<f64>,
) -> PolarsResult<Series> {
    let max_risk = risk_percentage.unwrap_or(2.0) / 100.0; // Convert to decimal
    
    // Check if swing risk level is available
    if !df.schema().contains("swing_risk_level") {
        return Err(PolarsError::ComputeError(
            "swing_risk_level not found. Calculate swing analysis first.".into(),
        ));
    }
    
    // Get combined signals and risk level
    let signals = match generate_swing_trading_signals(df) {
        Ok(s) => s,
        Err(_) => {
            // If combined signals not available, check if at least swing signal exists
            if !df.schema().contains("swing_signal") {
                return Err(PolarsError::ComputeError(
                    "No trading signals found for position sizing".into(),
                ));
            }
            df.column("swing_signal")?.clone()
        }
    };
    
    let signal_vals = signals.i32()?;
    let risk_level = df.column("swing_risk_level")?.i32()?;
    
    // Check if risk-reward ratio is available for additional adjustment
    let has_rr_ratio = df.schema().contains("risk_reward_ratio");
    let rr_ratio = if has_rr_ratio {
        Some(df.column("risk_reward_ratio")?.f64()?)
    } else {
        None
    };
    
    let mut position_sizes = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let signal = signal_vals.get(i).unwrap_or(0);
        let risk = risk_level.get(i).unwrap_or(2);
        
        // No position for neutral signals
        if signal == 0 {
            position_sizes.push(0.0);
            continue;
        }
        
        // Calculate position size based on risk level and signal strength
        let base_size = match risk {
            1 => max_risk * 1.0, // Low risk - full size
            2 => max_risk * 0.75, // Medium risk - 75% size
            3 => max_risk * 0.5, // High risk - 50% size
            _ => max_risk * 0.75, // Default to medium risk
        };
        
        // Adjust for signal strength (stronger signals get more size)
        let mut size = if signal.abs() == 2 {
            base_size * 1.0 // Full size for strong signals
        } else {
            base_size * 0.75 // Reduced size for moderate signals
        };
        
        // Further adjust based on risk-reward ratio if available
        if let Some(rr) = &rr_ratio {
            let rr_val = rr.get(i).unwrap_or(f64::NAN);
            if !rr_val.is_nan() {
                if rr_val >= 3.0 {
                    size *= 1.25; // Increase size for excellent risk-reward
                } else if rr_val >= 2.0 {
                    size *= 1.1; // Slightly increase size for good risk-reward
                } else if rr_val <= 0.5 {
                    size *= 0.5; // Reduce size for poor risk-reward
                }
                
                // Cap at max allowed risk
                size = size.min(max_risk);
            }
        }
        
        position_sizes.push(size);
    }
    
    Ok(Series::new("position_size", position_sizes))
} 