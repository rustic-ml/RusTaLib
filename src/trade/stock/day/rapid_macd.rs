use polars::prelude::*;
use crate::indicators::moving_averages::calculate_ema;

/// Calculate Rapid MACD for day trading
///
/// This is a faster-responding MACD variant optimized for intraday trading.
/// It uses shorter periods than the traditional MACD (12,26,9) to respond
/// more quickly to price changes within the trading day.
///
/// # Arguments
///
/// * `df` - DataFrame with price data
/// * `fast_period` - Period for fast EMA (default: 5)
/// * `slow_period` - Period for slow EMA (default: 13)
/// * `signal_period` - Period for signal line EMA (default: 4)
/// * `price_col` - Price column name (default: "close")
///
/// # Returns
///
/// * `PolarsResult<(Series, Series, Series)>` - MACD line, Signal line, and Histogram
pub fn calculate_rapid_macd(
    df: &DataFrame,
    fast_period: Option<usize>,
    slow_period: Option<usize>,
    signal_period: Option<usize>,
    price_col: Option<&str>,
) -> PolarsResult<(Series, Series, Series)> {
    let fast = fast_period.unwrap_or(5);
    let slow = slow_period.unwrap_or(13);
    let signal = signal_period.unwrap_or(4);
    let price_column = price_col.unwrap_or("close");
    
    // Ensure periods make sense
    if fast >= slow {
        return Err(PolarsError::ComputeError(
            "Fast period must be smaller than slow period".into(),
        ));
    }
    
    // Calculate fast and slow EMAs
    let fast_ema = calculate_ema(df, price_column, fast)?;
    let slow_ema = calculate_ema(df, price_column, slow)?;
    
    // Extract values
    let fast_vals = fast_ema.f64()?;
    let slow_vals = slow_ema.f64()?;
    
    // Calculate MACD line (fast EMA - slow EMA)
    let mut macd_line = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let fast_val = fast_vals.get(i).unwrap_or(f64::NAN);
        let slow_val = slow_vals.get(i).unwrap_or(f64::NAN);
        
        if !fast_val.is_nan() && !slow_val.is_nan() {
            macd_line.push(fast_val - slow_val);
        } else {
            macd_line.push(f64::NAN);
        }
    }
    
    // Create a temporary DataFrame to calculate signal line EMA
    let mut temp_df = DataFrame::new(vec![Series::new("macd_line", &macd_line)])?;
    let signal_ema = calculate_ema(&temp_df, "macd_line", signal)?;
    let signal_vals = signal_ema.f64()?;
    
    // Calculate histogram (MACD line - signal line)
    let mut histogram = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let macd_val = macd_line[i];
        let signal_val = signal_vals.get(i).unwrap_or(f64::NAN);
        
        if !macd_val.is_nan() && !signal_val.is_nan() {
            histogram.push(macd_val - signal_val);
        } else {
            histogram.push(f64::NAN);
        }
    }
    
    Ok((
        Series::new("rapid_macd", macd_line),
        Series::new("rapid_macd_signal", signal_vals.into_iter().collect::<Vec<_>>()),
        Series::new("rapid_macd_histogram", histogram),
    ))
}

/// Add Rapid MACD indicator to DataFrame
///
/// # Arguments
///
/// * `df` - Mutable reference to DataFrame
/// * `fast_period` - Optional fast EMA period
/// * `slow_period` - Optional slow EMA period
/// * `signal_period` - Optional signal line period
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_rapid_macd(
    df: &mut DataFrame,
    fast_period: Option<usize>,
    slow_period: Option<usize>,
    signal_period: Option<usize>,
) -> PolarsResult<()> {
    let (macd, signal, histogram) = calculate_rapid_macd(
        df, 
        fast_period, 
        slow_period, 
        signal_period, 
        None
    )?;
    
    df.with_column(macd)?;
    df.with_column(signal)?;
    df.with_column(histogram)?;
    
    Ok(())
}

/// Calculate zero-line crossover signals for Rapid MACD
///
/// # Arguments
///
/// * `df` - DataFrame containing rapid_macd column
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with buy (1), sell (-1), or no (0) signals
pub fn calculate_rapid_macd_signals(df: &DataFrame) -> PolarsResult<Series> {
    if !df.schema().contains("rapid_macd") {
        return Err(PolarsError::ComputeError(
            "rapid_macd column not found. Calculate Rapid MACD first.".into(),
        ));
    }
    
    let macd = df.column("rapid_macd")?.f64()?;
    let mut signals = Vec::with_capacity(df.height());
    
    // First value has no prior for comparison
    signals.push(0);
    
    for i in 1..df.height() {
        let current = macd.get(i).unwrap_or(f64::NAN);
        let previous = macd.get(i-1).unwrap_or(f64::NAN);
        
        if current.is_nan() || previous.is_nan() {
            signals.push(0);
        } else if previous < 0.0 && current >= 0.0 {
            // Bullish zero-line crossover
            signals.push(1);
        } else if previous > 0.0 && current <= 0.0 {
            // Bearish zero-line crossover
            signals.push(-1);
        } else {
            // No signal
            signals.push(0);
        }
    }
    
    Ok(Series::new("rapid_macd_signal", signals))
} 