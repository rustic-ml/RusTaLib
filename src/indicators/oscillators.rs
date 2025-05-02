use polars::prelude::*;
use crate::util::dataframe_utils::check_window_size;
use crate::indicators::moving_averages::calculate_ema;

/// Calculates Relative Strength Index (RSI)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for RSI calculation (typically 14)
/// * `column` - Column name to use for calculations (default "close")
///
/// # Returns
///
/// Returns a PolarsResult containing the RSI Series
pub fn calculate_rsi(df: &DataFrame, window: usize, column: &str) -> PolarsResult<Series> {
    // Check we have enough data
    check_window_size(df, window, "RSI")?;
    
    let close = df.column(column)?.f64()?.clone().into_series();
    let prev_close = close.shift(1);
    
    let mut gains = Vec::new();
    let mut losses = Vec::new();
    
    // Handle first value
    gains.push(0.0);
    losses.push(0.0);
    
    for i in 1..close.len() {
        let curr = close.f64()?.get(i).unwrap_or(0.0);
        let prev = prev_close.f64()?.get(i).unwrap_or(0.0);
        let change = curr - prev;
        
        if change > 0.0 {
            gains.push(change);
            losses.push(0.0);
        } else {
            gains.push(0.0);
            losses.push(-change);
        }
    }
    
    let gains_series = Series::new("gains".into(), gains);
    let losses_series = Series::new("losses".into(), losses);
    
    let avg_gain = gains_series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })?;
    let avg_loss = losses_series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })?;
    
    let mut rsi = Vec::with_capacity(close.len());
    for i in 0..close.len() {
        let g = avg_gain.f64()?.get(i).unwrap_or(0.0);
        let l = avg_loss.f64()?.get(i).unwrap_or(0.0);
        
        let rsi_val = if l == 0.0 {
            100.0
        } else {
            let rs = g / l;
            100.0 - (100.0 / (1.0 + rs))
        };
        rsi.push(rsi_val);
    }
    
    Ok(Series::new("rsi".into(), rsi))
}

/// Calculates Moving Average Convergence Divergence (MACD)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `fast_period` - Fast EMA period (typically 12)
/// * `slow_period` - Slow EMA period (typically 26) 
/// * `signal_period` - Signal line period (typically 9)
/// * `column` - Column name to use for calculations (default "close")
///
/// # Returns
///
/// Returns a PolarsResult containing tuple of (MACD, Signal) Series
pub fn calculate_macd(
    df: &DataFrame, 
    fast_period: usize, 
    slow_period: usize, 
    signal_period: usize,
    column: &str
) -> PolarsResult<(Series, Series)> {
    // Check we have enough data for the longest period (slow_period)
    check_window_size(df, slow_period, "MACD")?;
    
    let ema_fast = calculate_ema(df, column, fast_period)?;
    let ema_slow = calculate_ema(df, column, slow_period)?;
    
    let macd = (&ema_fast - &ema_slow)?;
    
    // Instead of creating a temporary DataFrame, apply EMA calculation directly to the macd series
    // This avoids creating a temporary DataFrame
    let macd_series = macd.clone();
    let signal = macd_series.rolling_mean(RollingOptionsFixedWindow {
        window_size: signal_period,
        min_periods: signal_period,
        center: false,
        weights: None,
        fn_params: None,
    })?;
    
    Ok((macd.with_name("macd".into()), signal.with_name("macd_signal".into())))
} 