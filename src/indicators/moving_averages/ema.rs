use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

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
    let series_ca = series.f64()?;
    let alpha = 2.0 / (window as f64 + 1.0);

    let mut ema_values = Vec::with_capacity(series.len());

    // Initialize with SMA for first window points
    let mut sma_sum = 0.0;
    for i in 0..window {
        let val = series_ca.get(i).unwrap_or(0.0);
        sma_sum += val;

        // Fill with nulls until we have enough data
        if i < window - 1 {
            ema_values.push(f64::NAN);
        }
    }

    // Add the initial SMA value
    let initial_ema = sma_sum / window as f64;
    ema_values.push(initial_ema);

    // Calculate EMA using the recursive formula
    let mut prev_ema = initial_ema;
    for i in window..series.len() {
        let price = series_ca.get(i).unwrap_or(0.0);
        let ema = alpha * price + (1.0 - alpha) * prev_ema;
        ema_values.push(ema);
        prev_ema = ema;
    }

    Ok(Series::new("ema".into(), ema_values))
}
