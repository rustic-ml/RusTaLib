use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

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
