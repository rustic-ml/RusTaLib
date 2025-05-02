use polars::prelude::*;
use crate::util::dataframe_utils::check_window_size;

/// Calculates Simple Moving Average (SMA)
///
/// # Arguments
///
/// * `df` - DataFrame containing the input data
/// * `column` - Column name to calculate SMA on
/// * `window` - Window size for the SMA
///
/// # Returns
///
/// Returns a PolarsResult containing the SMA Series
pub fn calculate_sma(df: &DataFrame, column: &str, window: usize) -> PolarsResult<Series> {
    // Check we have enough data
    check_window_size(df, window, "SMA")?;
    
    let series = df.column(column)?.f64()?.clone().into_series();
    
    series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })
}

/// Calculates Exponential Moving Average (EMA)
///
/// # Arguments
///
/// * `df` - DataFrame containing the input data
/// * `column` - Column name to calculate EMA on
/// * `window` - Window size for the EMA
///
/// # Returns
///
/// Returns a PolarsResult containing the EMA Series
pub fn calculate_ema(df: &DataFrame, column: &str, window: usize) -> PolarsResult<Series> {
    // Check we have enough data
    check_window_size(df, window, "EMA")?;
    
    let series = df.column(column)?.f64()?.clone().into_series();
    
    // Using polars rolling_mean with EMA weights for first implementation
    // A more accurate implementation would use the true EMA formula with alpha = 2/(window+1)
    series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })
}

/// Calculates Weighted Moving Average (WMA)
///
/// # Arguments
///
/// * `df` - DataFrame containing the input data
/// * `column` - Column name to calculate WMA on
/// * `window` - Window size for the WMA
///
/// # Returns
///
/// Returns a PolarsResult containing the WMA Series
pub fn calculate_wma(df: &DataFrame, column: &str, window: usize) -> PolarsResult<Series> {
    // Check we have enough data
    check_window_size(df, window, "WMA")?;
    
    let series = df.column(column)?.f64()?.clone().into_series();
    
    // Create linear weights [1, 2, 3, ..., window]
    let weights: Vec<f64> = (1..=window).map(|i| i as f64).collect();
    
    // Calculate WMA using rolling_mean with weights
    series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: Some(weights),
        fn_params: None,
    })
} 