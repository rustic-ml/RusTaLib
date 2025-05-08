//! # Day Trading Indicators for Stocks
//! 
//! This module provides specialized technical indicators optimized for
//! intraday stock trading (timeframes of minutes to hours).
//!
//! ## Included Indicators
//!
//! * VWAP (Volume Weighted Average Price) analysis
//! * Opening Range Breakout detection
//! * Intraday Momentum Index
//! * Adaptive RSI (dynamically adjusts based on volatility)
//! * Rapid MACD (faster-responding MACD variant)
//! * Gap Analysis (detect and trade price gaps)
//! * Market session analysis (morning vs. afternoon patterns)
//! * Volume Profile analysis

use polars::prelude::*;
use crate::indicators::moving_averages::calculate_vwap;

mod vwap_analysis;
mod opening_range;
mod intraday_momentum;
mod adaptive_rsi;
mod rapid_macd;
mod gap_analysis;

// Re-export the public functions
pub use vwap_analysis::*;
pub use opening_range::*;
pub use intraday_momentum::*;
pub use adaptive_rsi::*;
pub use rapid_macd::*;
pub use gap_analysis::*;

/// Calculate common day trading indicators for stocks
///
/// Adds a suite of day trading specific indicators to the DataFrame
/// including VWAP, standard deviations from VWAP, opening range analysis,
/// adaptive RSI, rapid MACD, and intraday momentum index.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `time_col` - Optional name of time column for session analysis
/// * `date_col` - Optional name of date column for gap analysis
///
/// # Returns
///
/// * `PolarsResult<DataFrame>` - DataFrame with added day trading indicators
pub fn add_day_trading_indicators(
    df: &DataFrame,
    time_col: Option<&str>,
    date_col: Option<&str>,
) -> PolarsResult<DataFrame> {
    let mut result = df.clone();
    
    // Calculate VWAP if volume column exists
    if df.schema().contains("volume") {
        // Add VWAP
        let vwap = calculate_vwap(df)?;
        result.with_column(vwap)?;
        
        // Add VWAP deviation bands
        vwap_analysis::add_vwap_bands(&mut result)?;
    }
    
    // Add opening range analysis if time column exists
    if let Some(time_column) = time_col {
        if df.schema().contains(time_column) {
            opening_range::add_opening_range_analysis(&mut result, time_column)?;
        }
    }
    
    // Add intraday momentum index
    intraday_momentum::add_intraday_momentum_index(&mut result, 14)?;
    
    // Add adaptive RSI (short period for day trading)
    adaptive_rsi::add_adaptive_rsi(&mut result, 7)?;
    
    // Add rapid MACD with default parameters
    rapid_macd::add_rapid_macd(&mut result, None, None, None)?;
    
    // Add gap analysis if date column exists
    if let Some(date_column) = date_col {
        if df.schema().contains(date_column) {
            // Use a threshold of 0.5% for significant gaps
            gap_analysis::add_gap_analysis(&mut result, Some(0.5))?;
        }
    }
    
    Ok(result)
}

/// Generate day trading signals based on multiple indicators
///
/// This function combines signals from various day trading indicators
/// to generate more robust entry and exit points.
///
/// # Arguments
///
/// * `df` - DataFrame with day trading indicators
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with combined signals (1 for buy, -1 for sell, 0 for neutral)
pub fn generate_day_trading_signals(df: &DataFrame) -> PolarsResult<Series> {
    // Ensure necessary indicator columns exist
    let required_indicators = [
        "vwap", "adaptive_rsi", "rapid_macd", "intraday_momentum_index"
    ];
    
    for indicator in required_indicators {
        if !df.schema().contains(indicator) {
            return Err(PolarsError::ComputeError(
                format!("Required indicator '{}' not found", indicator).into(),
            ));
        }
    }
    
    // Get individual indicator signals
    let momentum_signals = intraday_momentum::calculate_momentum_reversal_signals(df)?;
    let macd_signals = rapid_macd::calculate_rapid_macd_signals(df)?;
    
    // Get gap signals if available
    let has_gap_signals = df.schema().contains("gap_trade_signal");
    let gap_signals = if has_gap_signals {
        df.column("gap_trade_signal")?.i32()?
    } else {
        // Create empty signals if gap analysis wasn't run
        let mut empty_signals = Vec::with_capacity(df.height());
        for _ in 0..df.height() {
            empty_signals.push(0);
        }
        Series::new("empty_gap_signals", empty_signals).i32()?
    };
    
    // Create combined signals
    let mom_vals = momentum_signals.i32()?;
    let macd_vals = macd_signals.i32()?;
    
    let mut combined_signals = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let mom = mom_vals.get(i).unwrap_or(0);
        let macd = macd_vals.get(i).unwrap_or(0);
        let gap = gap_signals.get(i).unwrap_or(0);
        
        // Count how many bullish/bearish signals we have
        let mut bullish_count = 0;
        let mut bearish_count = 0;
        
        if mom > 0 { bullish_count += 1; }
        if mom < 0 { bearish_count += 1; }
        
        if macd > 0 { bullish_count += 1; }
        if macd < 0 { bearish_count += 1; }
        
        if gap > 0 { bullish_count += 1; }
        if gap < 0 { bearish_count += 1; }
        
        // Generate signal based on majority vote
        if bullish_count >= 2 && bearish_count == 0 {
            combined_signals.push(1); // Strong buy
        } else if bearish_count >= 2 && bullish_count == 0 {
            combined_signals.push(-1); // Strong sell
        } else if bullish_count > bearish_count {
            combined_signals.push(1); // Moderate buy
        } else if bearish_count > bullish_count {
            combined_signals.push(-1); // Moderate sell
        } else {
            combined_signals.push(0); // Neutral
        }
    }
    
    Ok(Series::new("day_trading_signal", combined_signals))
} 