use crate::indicators::moving_averages::calculate_ema;
use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

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
    column: &str,
) -> PolarsResult<(Series, Series)> {
    // Check we have enough data for the longest period (slow_period)
    check_window_size(df, slow_period, "MACD")?;

    let ema_fast = calculate_ema(df, column, fast_period)?;
    let ema_slow = calculate_ema(df, column, slow_period)?;

    let macd = (&ema_fast - &ema_slow)?;

    // Create a temporary DataFrame with MACD series for calculating the signal
    let macd_series = macd.clone();
    let temp_df = DataFrame::new(vec![macd_series.with_name(column.into()).into()])?;

    // Calculate the signal line as an EMA of the MACD
    let signal = calculate_ema(&temp_df, column, signal_period)?;

    // Replace NaN values in signal with zeros at positions where MACD has values
    let macd_ca = macd.f64()?;
    let signal_ca = signal.f64()?;

    let mut signal_vec: Vec<f64> = Vec::with_capacity(signal.len());

    for i in 0..signal.len() {
        if i < slow_period - 1 {
            // Keep first slow_period-1 values as NaN to match MACD
            signal_vec.push(f64::NAN);
        } else if i < slow_period - 1 + signal_period {
            // For index positions where signal might be NaN but MACD has values,
            // use non-NaN values or 0.0
            if let Some(macd_val) = macd_ca.get(i) {
                if !macd_val.is_nan() {
                    // Signal might be NaN here, use 0.0 as initial value
                    signal_vec.push(0.0);
                } else {
                    signal_vec.push(f64::NAN);
                }
            } else {
                signal_vec.push(f64::NAN);
            }
        } else {
            // For positions where signal should have valid values
            let val = signal_ca.get(i).unwrap_or(0.0);
            if val.is_nan() && macd_ca.get(i).is_some_and(|v| !v.is_nan()) {
                signal_vec.push(0.0);
            } else {
                signal_vec.push(val);
            }
        }
    }

    let macd_name = format!("macd_{0}_{1}", fast_period, slow_period);
    let signal_name = format!(
        "macd_signal_{0}_{1}_{2}",
        fast_period, slow_period, signal_period
    );

    Ok((
        macd.with_name(macd_name.into()),
        Series::new(signal_name.into(), signal_vec),
    ))
}
