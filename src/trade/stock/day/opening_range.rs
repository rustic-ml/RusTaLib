use polars::prelude::*;
use std::str::FromStr;

/// Calculate opening range for a trading day
///
/// The opening range is a key price zone for day traders, defined as the
/// high and low prices established during a specified time window after the market opens.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV and time data
/// * `time_col` - Name of the time column
/// * `range_minutes` - Duration in minutes to define the opening range (default: 30)
/// * `market_open_time` - Time when market opens (default: "09:30")
///
/// # Returns
///
/// * `PolarsResult<(Series, Series)>` - Series for opening range high and low
pub fn calculate_opening_range(
    df: &DataFrame,
    time_col: &str,
    range_minutes: Option<usize>,
    market_open_time: Option<&str>,
) -> PolarsResult<(Series, Series)> {
    let minutes = range_minutes.unwrap_or(30);
    let open_time = market_open_time.unwrap_or("09:30");
    
    // Ensure necessary columns exist
    for col in ["high", "low", time_col].iter() {
        if !df.schema().contains(*col) {
            return Err(PolarsError::ComputeError(
                format!("Required column '{}' not found", col).into(),
            ));
        }
    }
    
    // Get high and low price data
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    
    // Get time data
    let time_data = df.column(time_col)?;
    
    // Find data points within opening range window
    // This implementation assumes time_col can be parsed or compared 
    // In a real implementation, proper time parsing would be needed based on the format
    
    // For simplicity, we'll assume the data is sorted chronologically
    // and the opening range is simply the first 'minutes' data points
    // A more accurate implementation would parse the actual timestamps
    
    let mut opening_range_high = f64::MIN;
    let mut opening_range_low = f64::MAX;
    let mut in_opening_range = false;
    let mut range_end_idx = 0;
    
    // Simplified approach to find opening range
    // In a real implementation, proper time parsing and comparison would be used
    for i in 0..df.height() {
        let time_str = match time_data.dtype() {
            DataType::Utf8 => time_data.str()?.get(i).unwrap_or("").to_string(),
            DataType::Time => format!("{:02}:{:02}", 
                                    time_data.time()?.get(i).unwrap_or(0) / 3600000,
                                    (time_data.time()?.get(i).unwrap_or(0) / 60000) % 60),
            _ => return Err(PolarsError::ComputeError(
                "Time column must be string or time type".into(),
            )),
        };
        
        // Check if we're at market open time or after
        if !in_opening_range && time_str >= open_time {
            in_opening_range = true;
        }
        
        // Process data in opening range
        if in_opening_range {
            let h = high.get(i).unwrap_or(f64::NAN);
            let l = low.get(i).unwrap_or(f64::NAN);
            
            if !h.is_nan() {
                opening_range_high = opening_range_high.max(h);
            }
            
            if !l.is_nan() {
                opening_range_low = opening_range_low.min(l);
            }
            
            range_end_idx += 1;
            
            // Check if we've reached the end of the opening range window
            if range_end_idx >= minutes {
                break;
            }
        }
    }
    
    // Create Series for opening range high and low
    let mut or_high = Vec::with_capacity(df.height());
    let mut or_low = Vec::with_capacity(df.height());
    
    for _ in 0..df.height() {
        or_high.push(opening_range_high);
        or_low.push(opening_range_low);
    }
    
    Ok((
        Series::new("opening_range_high", or_high),
        Series::new("opening_range_low", or_low),
    ))
}

/// Add opening range analysis to DataFrame
///
/// Adds opening range high/low and breakout signals
///
/// # Arguments
///
/// * `df` - Mutable reference to DataFrame
/// * `time_col` - Name of the time column
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_opening_range_analysis(df: &mut DataFrame, time_col: &str) -> PolarsResult<()> {
    // Calculate opening range
    let (or_high, or_low) = calculate_opening_range(df, time_col, None, None)?;
    
    // Add opening range to DataFrame
    df.with_column(or_high.clone())?;
    df.with_column(or_low.clone())?;
    
    // Get closing price
    let close = df.column("close")?.f64()?;
    let or_high_values = or_high.f64()?;
    let or_low_values = or_low.f64()?;
    
    // Calculate breakout signals
    let mut breakout_signals = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let c = close.get(i).unwrap_or(f64::NAN);
        let h = or_high_values.get(i).unwrap_or(f64::NAN);
        let l = or_low_values.get(i).unwrap_or(f64::NAN);
        
        if c.is_nan() || h.is_nan() || l.is_nan() {
            breakout_signals.push(0);
        } else if c > h {
            // Bullish breakout
            breakout_signals.push(1);
        } else if c < l {
            // Bearish breakout
            breakout_signals.push(-1);
        } else {
            // Inside opening range
            breakout_signals.push(0);
        }
    }
    
    df.with_column(Series::new("opening_range_breakout", breakout_signals))?;
    
    Ok(())
}

/// Calculate opening range success rate
///
/// This function analyzes the historical success rate of opening range breakouts
/// to help determine if this pattern is profitable for a specific stock or market condition.
///
/// # Arguments
///
/// * `df` - DataFrame with opening range breakout signals
/// * `forward_bars` - Number of bars to look ahead for measuring success
///
/// # Returns
///
/// * `PolarsResult<f64>` - Success rate percentage
pub fn calculate_opening_range_success_rate(
    df: &DataFrame,
    forward_bars: usize,
) -> PolarsResult<f64> {
    if !df.schema().contains("opening_range_breakout") {
        return Err(PolarsError::ComputeError(
            "opening_range_breakout column not found. Calculate opening range analysis first.".into(),
        ));
    }
    
    let breakout_signals = df.column("opening_range_breakout")?.i32()?;
    let close = df.column("close")?.f64()?;
    
    let mut total_signals = 0;
    let mut successful_signals = 0;
    
    for i in 0..(df.height().saturating_sub(forward_bars)) {
        let signal = breakout_signals.get(i).unwrap_or(0);
        
        // Skip if no signal
        if signal == 0 {
            continue;
        }
        
        let current_close = close.get(i).unwrap_or(f64::NAN);
        let future_close = close.get(i + forward_bars).unwrap_or(f64::NAN);
        
        if current_close.is_nan() || future_close.is_nan() {
            continue;
        }
        
        total_signals += 1;
        
        // Determine if breakout was successful
        if (signal > 0 && future_close > current_close) || 
           (signal < 0 && future_close < current_close) {
            successful_signals += 1;
        }
    }
    
    if total_signals > 0 {
        Ok((successful_signals as f64 / total_signals as f64) * 100.0)
    } else {
        Ok(0.0) // No signals found
    }
} 