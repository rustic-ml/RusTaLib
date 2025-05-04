use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Plus Directional Movement (+DM)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data with high, low columns
/// * `window` - Window size for smoothing (typically 14)
///
/// # Returns
///
/// Returns a PolarsResult containing the smoothed +DM Series
pub fn calculate_plus_dm(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    check_window_size(df, window, "+DM")?;

    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;

    let high_prev = high.shift(1);
    let low_prev = low.shift(1);

    let mut dm_plus = Vec::with_capacity(df.height());

    // First value
    dm_plus.push(0.0);

    for i in 1..df.height() {
        let h = high.get(i).unwrap_or(0.0);
        let h_prev = high_prev.get(i).unwrap_or(0.0);
        let l = low.get(i).unwrap_or(0.0);
        let l_prev = low_prev.get(i).unwrap_or(0.0);

        let up_move = h - h_prev;
        let down_move = l_prev - l;

        if up_move > down_move && up_move > 0.0 {
            dm_plus.push(up_move);
        } else {
            dm_plus.push(0.0);
        }
    }

    let dm_plus_series = Series::new("dm_plus".into(), dm_plus);

    // Smooth the +DM
    let smoothed_dm_plus = dm_plus_series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })?;

    Ok(smoothed_dm_plus.with_name("plus_dm".into()))
}
