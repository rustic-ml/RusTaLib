use polars::prelude::*;
use crate::indicators::momentum::calculate_momentum;

/// Calculate Intraday Momentum Index (IMI)
///
/// This indicator is specifically designed to measure intraday buying/selling pressure
/// and identify potential reversal points during the trading day.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `period` - Look-back period for momentum calculation (default: 14)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series containing IMI values (0-100 range)
pub fn calculate_intraday_momentum_index(
    df: &DataFrame,
    period: Option<usize>,
) -> PolarsResult<Series> {
    let n = period.unwrap_or(14);
    
    // Ensure necessary columns exist
    for col in ["high", "low", "close"].iter() {
        if !df.schema().contains(*col) {
            return Err(PolarsError::ComputeError(
                format!("Required column '{}' not found", col).into(),
            ));
        }
    }
    
    // Extract price data
    let close = df.column("close")?.f64()?;
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    
    let mut imi_values = Vec::with_capacity(df.height());
    
    // First n-1 values will be NaN
    for _ in 0..n.min(df.height()) {
        imi_values.push(f64::NAN);
    }
    
    // Calculate IMI for each bar after the initial period
    for i in n..df.height() {
        let mut up_sum = 0.0;
        let mut down_sum = 0.0;
        
        for j in (i - n + 1)..=i {
            let c = close.get(j).unwrap_or(f64::NAN);
            let h = high.get(j).unwrap_or(f64::NAN);
            let l = low.get(j).unwrap_or(f64::NAN);
            
            if c.is_nan() || h.is_nan() || l.is_nan() {
                continue;
            }
            
            // Distance from close to high and low
            let c_to_h = h - c;
            let c_to_l = c - l;
            
            // Upward and downward price movements
            up_sum += c_to_h;
            down_sum += c_to_l;
        }
        
        // Calculate the IMI value (similar to RSI calculation)
        if up_sum + down_sum > 0.0 {
            let imi = 100.0 * (down_sum / (up_sum + down_sum));
            imi_values.push(imi);
        } else {
            imi_values.push(50.0); // Neutral value when no movement
        }
    }
    
    Ok(Series::new("intraday_momentum_index", imi_values))
}

/// Add Intraday Momentum Index to DataFrame
///
/// # Arguments
///
/// * `df` - Mutable reference to DataFrame
/// * `period` - Look-back period for momentum calculation
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_intraday_momentum_index(df: &mut DataFrame, period: usize) -> PolarsResult<()> {
    let imi = calculate_intraday_momentum_index(df, Some(period))?;
    df.with_column(imi)?;
    
    // Also add overbought/oversold levels for convenience
    let imi_values = df.column("intraday_momentum_index")?.f64()?;
    
    // Overbought when IMI > 70
    let mut overbought = Vec::with_capacity(df.height());
    // Oversold when IMI < 30
    let mut oversold = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let imi_val = imi_values.get(i).unwrap_or(f64::NAN);
        
        if imi_val.is_nan() {
            overbought.push(false);
            oversold.push(false);
        } else {
            overbought.push(imi_val > 70.0);
            oversold.push(imi_val < 30.0);
        }
    }
    
    df.with_column(Series::new("imi_overbought", overbought))?;
    df.with_column(Series::new("imi_oversold", oversold))?;
    
    Ok(())
}

/// Calculate Intraday Momentum Reversal signals
///
/// This function detects potential intraday reversals based on the
/// Intraday Momentum Index reaching extreme levels and then reversing.
///
/// # Arguments
///
/// * `df` - DataFrame containing intraday_momentum_index column
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with reversal signals (1 for bullish, -1 for bearish, 0 for none)
pub fn calculate_momentum_reversal_signals(df: &DataFrame) -> PolarsResult<Series> {
    if !df.schema().contains("intraday_momentum_index") {
        return Err(PolarsError::ComputeError(
            "intraday_momentum_index column not found. Calculate IMI first.".into(),
        ));
    }
    
    let imi = df.column("intraday_momentum_index")?.f64()?;
    let mut signals = Vec::with_capacity(df.height());
    
    // Need at least two periods to identify a reversal
    if df.height() < 2 {
        return Ok(Series::new("imi_reversal_signal", vec![0; df.height()]));
    }
    
    // First value has no signal
    signals.push(0);
    
    for i in 1..df.height() {
        let current = imi.get(i).unwrap_or(f64::NAN);
        let previous = imi.get(i-1).unwrap_or(f64::NAN);
        
        if current.is_nan() || previous.is_nan() {
            signals.push(0);
        } else if previous <= 20.0 && current > 20.0 {
            // Bullish reversal from oversold
            signals.push(1);
        } else if previous >= 80.0 && current < 80.0 {
            // Bearish reversal from overbought
            signals.push(-1);
        } else {
            // No signal
            signals.push(0);
        }
    }
    
    Ok(Series::new("imi_reversal_signal", signals))
} 