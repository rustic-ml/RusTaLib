use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

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
