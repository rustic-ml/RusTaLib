use polars::prelude::*;
use crate::indicators::moving_averages::{calculate_sma, calculate_ema};
use crate::indicators::trend::calculate_adx;

/// Calculate Secular Trend Momentum
///
/// This function measures the momentum of long-term secular trends
/// by analyzing multi-year moving averages and their relative positioning.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `long_period` - Long-term moving average period (default: 500)
/// * `medium_period` - Medium-term moving average period (default: 200)
/// * `short_period` - Short-term moving average period (default: 50)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with secular momentum (2: strong up, 1: up,
///                           0: neutral, -1: down, -2: strong down)
pub fn calculate_secular_momentum(
    df: &DataFrame,
    long_period: Option<usize>,
    medium_period: Option<usize>,
    short_period: Option<usize>,
) -> PolarsResult<Series> {
    let long_ma_period = long_period.unwrap_or(500);
    let medium_ma_period = medium_period.unwrap_or(200);
    let short_ma_period = short_period.unwrap_or(50);
    
    // Calculate moving averages
    let long_ma = calculate_sma(df, "close", long_ma_period)?;
    let medium_ma = calculate_sma(df, "close", medium_ma_period)?;
    let short_ma = calculate_sma(df, "close", short_ma_period)?;
    
    let long_ma_vals = long_ma.f64()?;
    let medium_ma_vals = medium_ma.f64()?;
    let short_ma_vals = short_ma.f64()?;
    
    // Get closing prices
    let close = df.column("close")?.f64()?;
    
    let mut secular_momentum = Vec::with_capacity(df.height());
    
    // First values will be undefined until we have enough data
    let min_periods = long_ma_period;
    for i in 0..min_periods.min(df.height()) {
        secular_momentum.push(0);
    }
    
    // Calculate secular momentum for each point
    for i in min_periods..df.height() {
        let long_ma_val = long_ma_vals.get(i).unwrap_or(f64::NAN);
        let medium_ma_val = medium_ma_vals.get(i).unwrap_or(f64::NAN);
        let short_ma_val = short_ma_vals.get(i).unwrap_or(f64::NAN);
        let close_val = close.get(i).unwrap_or(f64::NAN);
        
        if long_ma_val.is_nan() || medium_ma_val.is_nan() || short_ma_val.is_nan() || close_val.is_nan() {
            secular_momentum.push(0);
            continue;
        }
        
        // Calculate slopes of moving averages (multi-year perspective)
        let long_lookback = (long_ma_period / 2).min(i);
        let long_ma_prev = long_ma_vals.get(i - long_lookback).unwrap_or(long_ma_val);
        let long_slope = (long_ma_val - long_ma_prev) / long_ma_prev * 100.0;
        
        let medium_lookback = (medium_ma_period / 2).min(i);
        let medium_ma_prev = medium_ma_vals.get(i - medium_lookback).unwrap_or(medium_ma_val);
        let medium_slope = (medium_ma_val - medium_ma_prev) / medium_ma_prev * 100.0;
        
        // Check MA alignment
        let aligned_bullish = close_val > short_ma_val && short_ma_val > medium_ma_val && medium_ma_val > long_ma_val;
        let aligned_bearish = close_val < short_ma_val && short_ma_val < medium_ma_val && medium_ma_val < long_ma_val;
        
        // Determine secular momentum
        if aligned_bullish && long_slope > 0.5 && medium_slope > 1.0 {
            // Strong uptrend - all MAs aligned bullishly with positive slopes
            secular_momentum.push(2);
        } else if long_slope > 0.0 && medium_slope > 0.0 && medium_ma_val > long_ma_val {
            // Moderate uptrend
            secular_momentum.push(1);
        } else if aligned_bearish && long_slope < -0.5 && medium_slope < -1.0 {
            // Strong downtrend - all MAs aligned bearishly with negative slopes
            secular_momentum.push(-2);
        } else if long_slope < 0.0 && medium_slope < 0.0 && medium_ma_val < long_ma_val {
            // Moderate downtrend
            secular_momentum.push(-1);
        } else {
            // No clear secular trend
            secular_momentum.push(0);
        }
    }
    
    Ok(Series::new("secular_momentum", secular_momentum))
}

/// Calculate Secular Trend Duration
///
/// This function estimates how long the current secular trend
/// has been in place, which is important for long-term position trading.
///
/// # Arguments
///
/// * `df` - DataFrame with secular_momentum already calculated
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with trend duration in bars
pub fn calculate_secular_trend_duration(df: &DataFrame) -> PolarsResult<Series> {
    // Check if secular momentum is already calculated
    if !df.schema().contains("secular_momentum") {
        return Err(PolarsError::ComputeError(
            "secular_momentum column not found. Calculate secular momentum first.".into(),
        ));
    }
    
    let momentum = df.column("secular_momentum")?.i32()?;
    let mut duration = Vec::with_capacity(df.height());
    
    // First point has no previous momentum
    if df.height() > 0 {
        duration.push(0);
    }
    
    // Calculate duration of current secular trend
    for i in 1..df.height() {
        let current = momentum.get(i).unwrap_or(0);
        let previous = duration[i - 1];
        
        // Check if momentum direction is the same sign as previous
        let same_direction = (current > 0 && momentum.get(i - 1).unwrap_or(0) > 0) ||
                            (current < 0 && momentum.get(i - 1).unwrap_or(0) < 0);
        
        if current != 0 && same_direction {
            // Continue counting
            duration.push(previous + 1);
        } else {
            // Reset counter
            duration.push(if current != 0 { 1 } else { 0 });
        }
    }
    
    Ok(Series::new("secular_trend_duration", duration))
}

/// Calculate Secular Trend Strength
///
/// This function measures the strength of a secular trend based on
/// the alignment and momentum of multiple timeframes.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data and secular_momentum
/// * `adx_period` - ADX calculation period (default: 200)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with secular trend strength (0-100)
pub fn calculate_secular_trend_strength(
    df: &DataFrame,
    adx_period: Option<usize>,
) -> PolarsResult<Series> {
    // Check if secular momentum is already calculated
    if !df.schema().contains("secular_momentum") {
        return Err(PolarsError::ComputeError(
            "secular_momentum column not found. Calculate secular momentum first.".into(),
        ));
    }
    
    let period = adx_period.unwrap_or(200);
    
    // Calculate long-term ADX for trend strength
    let adx = calculate_adx(df, period)?;
    let adx_vals = adx.f64()?;
    
    let momentum = df.column("secular_momentum")?.i32()?;
    let mut strength = Vec::with_capacity(df.height());
    
    // First values will be undefined until we have enough data
    for i in 0..period.min(df.height()) {
        strength.push(f64::NAN);
    }
    
    // Calculate strength for each point
    for i in period..df.height() {
        let adx_val = adx_vals.get(i).unwrap_or(f64::NAN);
        let momentum_val = momentum.get(i).unwrap_or(0);
        
        if adx_val.is_nan() {
            strength.push(f64::NAN);
            continue;
        }
        
        // Base strength on ADX
        let mut trend_strength = adx_val;
        
        // Adjust strength based on secular momentum
        let momentum_multiplier = match momentum_val.abs() {
            2 => 1.25, // Strong momentum
            1 => 1.0,  // Moderate momentum
            _ => 0.75, // No clear momentum
        };
        
        trend_strength *= momentum_multiplier;
        
        // Factor in trend duration if available
        if df.schema().contains("secular_trend_duration") {
            let duration = df.column("secular_trend_duration")?.i32()?;
            let duration_val = duration.get(i).unwrap_or(0);
            
            // Long-lasting trends tend to be more reliable
            if duration_val > 500 {
                trend_strength *= 1.2; // Very long trend
            } else if duration_val > 250 {
                trend_strength *= 1.1; // Long trend
            }
        }
        
        // Cap at 100
        strength.push(trend_strength.min(100.0));
    }
    
    Ok(Series::new("secular_trend_strength", strength))
}

/// Identify Multi-Year Support/Resistance Levels
///
/// This function identifies major support and resistance levels
/// formed over multi-year periods.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `lookback_years` - Years to look back (in trading days) (default: 10)
///
/// # Returns
///
/// * `PolarsResult<(Series, Series)>` - (Support levels, Resistance levels)
pub fn identify_secular_levels(
    df: &DataFrame,
    lookback_years: Option<usize>,
) -> PolarsResult<(Series, Series)> {
    let trading_days_per_year = 252;
    let years = lookback_years.unwrap_or(10);
    let lookback = years * trading_days_per_year;
    
    // Get price data
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    
    // Create containers for levels
    let mut support_levels = Vec::with_capacity(df.height());
    let mut resistance_levels = Vec::with_capacity(df.height());
    
    // Minimum prominence required for a significant level (as percentage of price)
    let min_prominence_pct = 0.05; // 5%
    
    // First values will have no levels until we have enough data
    for i in 0..lookback.min(df.height()) {
        support_levels.push(f64::NAN);
        resistance_levels.push(f64::NAN);
    }
    
    // Identify levels for each point
    for i in lookback..df.height() {
        let current_close = close.get(i).unwrap_or(f64::NAN);
        
        if current_close.is_nan() {
            support_levels.push(f64::NAN);
            resistance_levels.push(f64::NAN);
            continue;
        }
        
        // Find significant lows (support)
        let mut best_support = f64::NAN;
        let mut support_strength = 0;
        
        // Find significant highs (resistance)
        let mut best_resistance = f64::NAN;
        let mut resistance_strength = 0;
        
        // Scan historical data for major levels
        // Use a sliding window approach to find significant pivots
        for j in (i - lookback + 60)..i {
            let window_size = 30; // Look at 30 bars around a potential pivot
            
            if j < window_size || j + window_size >= i {
                continue;
            }
            
            // Check if this bar is a potential pivot low
            let current_low = low.get(j).unwrap_or(f64::NAN);
            let mut is_pivot_low = true;
            
            // Check if this is the lowest in the window
            for k in (j - window_size)..(j + window_size) {
                let check_low = low.get(k).unwrap_or(f64::NAN);
                if !check_low.is_nan() && check_low < current_low {
                    is_pivot_low = false;
                    break;
                }
            }
            
            if is_pivot_low && !current_low.is_nan() {
                // Count how many times price approached this level
                let mut touches = 0;
                let level_tolerance = current_low * 0.01; // 1% tolerance
                
                for k in (j + window_size)..i {
                    let check_low = low.get(k).unwrap_or(f64::NAN);
                    if !check_low.is_nan() && (check_low - current_low).abs() <= level_tolerance {
                        touches += 1;
                    }
                }
                
                // If this level has been tested multiple times or is very prominent
                if touches > 2 || (current_close - current_low) / current_close >= min_prominence_pct {
                    // Check if this is better than our current best support
                    if support_strength < touches {
                        best_support = current_low;
                        support_strength = touches;
                    }
                }
            }
            
            // Check if this bar is a potential pivot high
            let current_high = high.get(j).unwrap_or(f64::NAN);
            let mut is_pivot_high = true;
            
            // Check if this is the highest in the window
            for k in (j - window_size)..(j + window_size) {
                let check_high = high.get(k).unwrap_or(f64::NAN);
                if !check_high.is_nan() && check_high > current_high {
                    is_pivot_high = false;
                    break;
                }
            }
            
            if is_pivot_high && !current_high.is_nan() {
                // Count how many times price approached this level
                let mut touches = 0;
                let level_tolerance = current_high * 0.01; // 1% tolerance
                
                for k in (j + window_size)..i {
                    let check_high = high.get(k).unwrap_or(f64::NAN);
                    if !check_high.is_nan() && (check_high - current_high).abs() <= level_tolerance {
                        touches += 1;
                    }
                }
                
                // If this level has been tested multiple times or is very prominent
                if touches > 2 || (current_high - current_close) / current_close >= min_prominence_pct {
                    // Check if this is better than our current best resistance
                    if resistance_strength < touches {
                        best_resistance = current_high;
                        resistance_strength = touches;
                    }
                }
            }
        }
        
        support_levels.push(best_support);
        resistance_levels.push(best_resistance);
    }
    
    Ok((
        Series::new("secular_support", support_levels),
        Series::new("secular_resistance", resistance_levels),
    ))
}

/// Add secular trend analysis to DataFrame
///
/// # Arguments
///
/// * `df` - Mutable reference to DataFrame
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_secular_trend_analysis(df: &mut DataFrame) -> PolarsResult<()> {
    let momentum = calculate_secular_momentum(df, None, None, None)?;
    df.with_column(momentum)?;
    
    let duration = calculate_secular_trend_duration(df)?;
    df.with_column(duration)?;
    
    let strength = calculate_secular_trend_strength(df, None)?;
    df.with_column(strength)?;
    
    let (support, resistance) = identify_secular_levels(df, None)?;
    df.with_column(support)?;
    df.with_column(resistance)?;
    
    Ok(())
} 