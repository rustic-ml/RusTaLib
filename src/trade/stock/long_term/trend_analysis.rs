use polars::prelude::*;
use crate::indicators::moving_averages::{calculate_sma, calculate_ema};
use crate::indicators::trend::calculate_adx;

/// Calculate Long-Term Trend Strength
///
/// This function measures the strength and persistence of long-term trends
/// using a combination of moving averages and ADX designed for position trading.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `period` - Base period for calculations (default: 200)
/// * `adx_period` - ADX calculation period (default: 50)
/// * `smooth_period` - Smoothing period (default: 20)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series containing trend strength values (0-100)
pub fn calculate_long_term_trend_strength(
    df: &DataFrame,
    period: Option<usize>,
    adx_period: Option<usize>,
    smooth_period: Option<usize>,
) -> PolarsResult<Series> {
    let base_period = period.unwrap_or(200);
    let adx_len = adx_period.unwrap_or(50);
    let smoothing = smooth_period.unwrap_or(20);
    
    // Calculate ADX for trend strength confirmation
    let adx = calculate_adx(df, adx_len)?;
    let adx_values = adx.f64()?;
    
    // Calculate moving averages for multiple timeframes
    let sma_long = calculate_sma(df, "close", base_period)?;
    let sma_medium = calculate_sma(df, "close", base_period / 2)?; // Half of the base period
    let sma_short = calculate_sma(df, "close", base_period / 4)?; // Quarter of the base period
    
    let long_ma_vals = sma_long.f64()?;
    let medium_ma_vals = sma_medium.f64()?;
    let short_ma_vals = sma_short.f64()?;
    
    // Get closing prices
    let close = df.column("close")?.f64()?;
    
    let mut trend_strength = Vec::with_capacity(df.height());
    
    // First values will be NaN until we have enough data points
    let min_periods = base_period.max(adx_len);
    for i in 0..min_periods.min(df.height()) {
        trend_strength.push(f64::NAN);
    }
    
    // Calculate trend strength for each remaining point
    for i in min_periods..df.height() {
        let adx_val = adx_values.get(i).unwrap_or(f64::NAN);
        let long_ma = long_ma_vals.get(i).unwrap_or(f64::NAN);
        let medium_ma = medium_ma_vals.get(i).unwrap_or(f64::NAN);
        let short_ma = short_ma_vals.get(i).unwrap_or(f64::NAN);
        let close_val = close.get(i).unwrap_or(f64::NAN);
        
        if adx_val.is_nan() || long_ma.is_nan() || medium_ma.is_nan() || short_ma.is_nan() || close_val.is_nan() {
            trend_strength.push(f64::NAN);
            continue;
        }
        
        // Start with ADX as base strength value
        let mut strength = adx_val;
        
        // Calculate slopes of moving averages to determine trend persistence
        let long_ma_prev = long_ma_vals.get(i - smoothing).unwrap_or(long_ma);
        let medium_ma_prev = medium_ma_vals.get(i - smoothing).unwrap_or(medium_ma);
        let short_ma_prev = short_ma_vals.get(i - smoothing).unwrap_or(short_ma);
        
        let long_slope = (long_ma - long_ma_prev) / long_ma_prev * 100.0;
        let medium_slope = (medium_ma - medium_ma_prev) / medium_ma_prev * 100.0;
        let short_slope = (short_ma - short_ma_prev) / short_ma_prev * 100.0;
        
        // Calculate MA alignment factor - more weight for better aligned MAs
        let slopes_aligned = if (long_slope > 0.0 && medium_slope > 0.0 && short_slope > 0.0) ||
                              (long_slope < 0.0 && medium_slope < 0.0 && short_slope < 0.0) {
            // All slopes in same direction - strong alignment
            1.25
        } else if (long_slope * medium_slope > 0.0) && (medium_slope * short_slope > 0.0) {
            // Two consecutive pairs aligned - moderate alignment
            1.1
        } else if long_slope * short_slope > 0.0 {
            // Long and short aligned but medium differs - weak alignment
            0.9
        } else {
            // No alignment - potentially changing trend
            0.75
        };
        
        // Calculate price position relative to MAs
        let price_position = if close_val > short_ma && short_ma > medium_ma && medium_ma > long_ma {
            // Perfect uptrend alignment
            1.2
        } else if close_val < short_ma && short_ma < medium_ma && medium_ma < long_ma {
            // Perfect downtrend alignment
            1.2
        } else if (close_val > long_ma && medium_ma > long_ma) || 
                  (close_val < long_ma && medium_ma < long_ma) {
            // Good alignment but not perfect
            1.0
        } else {
            // Mixed signals
            0.8
        };
        
        // Apply adjustments
        strength = strength * slopes_aligned * price_position;
        
        // Smooth the strength using past values
        if i >= min_periods + smoothing {
            let mut sum = strength;
            for j in 1..=smoothing {
                sum += trend_strength[i - j];
            }
            strength = sum / (smoothing as f64 + 1.0);
        }
        
        // Cap at 100
        trend_strength.push(strength.min(100.0));
    }
    
    Ok(Series::new("trend_strength", trend_strength))
}

/// Determine long-term trend direction
///
/// This function determines the direction of the long-term trend
/// using multiple timeframe analysis.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `period` - Base period for calculations (default: 200)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with trend direction (1: uptrend, -1: downtrend, 0: neutral)
pub fn determine_trend_direction(
    df: &DataFrame,
    period: Option<usize>,
) -> PolarsResult<Series> {
    let base_period = period.unwrap_or(200);
    
    // Calculate multiple timeframe moving averages
    let sma_long = calculate_sma(df, "close", base_period)?;
    let sma_medium = calculate_sma(df, "close", base_period / 2)?;
    let sma_short = calculate_sma(df, "close", base_period / 4)?;
    
    let long_ma_vals = sma_long.f64()?;
    let medium_ma_vals = sma_medium.f64()?;
    let short_ma_vals = sma_short.f64()?;
    
    // Get closing prices
    let close = df.column("close")?.f64()?;
    
    let mut trend_direction = Vec::with_capacity(df.height());
    
    // First values will be neutral until we have enough data
    for i in 0..base_period.min(df.height()) {
        trend_direction.push(0);
    }
    
    // Determine trend direction for each point
    for i in base_period..df.height() {
        let long_ma = long_ma_vals.get(i).unwrap_or(f64::NAN);
        let medium_ma = medium_ma_vals.get(i).unwrap_or(f64::NAN);
        let short_ma = short_ma_vals.get(i).unwrap_or(f64::NAN);
        let close_val = close.get(i).unwrap_or(f64::NAN);
        
        if long_ma.is_nan() || medium_ma.is_nan() || short_ma.is_nan() || close_val.is_nan() {
            trend_direction.push(0);
            continue;
        }
        
        // Calculate slopes over last 20 bars
        let lookback = 20.min(i);
        let long_ma_prev = long_ma_vals.get(i - lookback).unwrap_or(long_ma);
        let long_slope = (long_ma - long_ma_prev) / long_ma_prev * 100.0;
        
        // Determine trend based on price vs MAs and MA alignment
        if close_val > long_ma && short_ma > medium_ma && medium_ma > long_ma && long_slope > 0.0 {
            // Strong uptrend - price above 200MA, MAs in bullish alignment, long MA rising
            trend_direction.push(1);
        } else if close_val < long_ma && short_ma < medium_ma && medium_ma < long_ma && long_slope < 0.0 {
            // Strong downtrend - price below 200MA, MAs in bearish alignment, long MA falling
            trend_direction.push(-1);
        } else if close_val > long_ma && short_ma > long_ma && long_slope >= 0.0 {
            // Moderate uptrend
            trend_direction.push(1);
        } else if close_val < long_ma && short_ma < long_ma && long_slope <= 0.0 {
            // Moderate downtrend
            trend_direction.push(-1);
        } else {
            // No clear trend
            trend_direction.push(0);
        }
    }
    
    Ok(Series::new("trend_direction", trend_direction))
}

/// Calculate persistence of long-term trend
///
/// This function measures how long a trend has been in place,
/// which is important for long-term position trading decisions.
///
/// # Arguments
///
/// * `df` - DataFrame with trend_direction already calculated
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with trend persistence in bars
pub fn calculate_trend_persistence(df: &DataFrame) -> PolarsResult<Series> {
    // Check if trend direction is already calculated
    if !df.schema().contains("trend_direction") {
        return Err(PolarsError::ComputeError(
            "trend_direction column not found. Calculate trend direction first.".into(),
        ));
    }
    
    let direction = df.column("trend_direction")?.i32()?;
    let mut persistence = Vec::with_capacity(df.height());
    
    // For the first point, we can't have persistence yet
    if df.height() > 0 {
        persistence.push(0);
    }
    
    // Calculate persistence (count of consecutive same direction)
    for i in 1..df.height() {
        let current_dir = direction.get(i).unwrap_or(0);
        let prev_dir = direction.get(i - 1).unwrap_or(0);
        
        if current_dir == prev_dir && current_dir != 0 {
            // Same direction as previous, increment persistence
            persistence.push(persistence[i - 1] + 1);
        } else {
            // Direction changed or neutral, reset
            persistence.push(0);
        }
    }
    
    Ok(Series::new("trend_persistence", persistence))
}

/// Add trend analysis to DataFrame
///
/// # Arguments
///
/// * `df` - Mutable reference to DataFrame
/// * `period` - Base period for calculations
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_trend_analysis(df: &mut DataFrame, period: usize) -> PolarsResult<()> {
    let trend_strength = calculate_long_term_trend_strength(df, Some(period), None, None)?;
    df.with_column(trend_strength)?;
    
    let trend_direction = determine_trend_direction(df, Some(period))?;
    df.with_column(trend_direction)?;
    
    let trend_persistence = calculate_trend_persistence(df)?;
    df.with_column(trend_persistence)?;
    
    Ok(())
} 