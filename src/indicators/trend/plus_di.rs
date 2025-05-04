use super::plus_dm::calculate_plus_dm;
use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Plus Directional Indicator (+DI)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data with high, low, close columns
/// * `window` - Window size for calculation (typically 14)
///
/// # Returns
///
/// Returns a PolarsResult containing the +DI Series
pub fn calculate_plus_di(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    check_window_size(df, window, "+DI")?;

    // Calculate +DM
    let plus_dm = calculate_plus_dm(df, window)?;

    // Calculate the true range (TR)
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close_prev = df.column("close")?.f64()?.shift(1);

    let mut tr_values = Vec::with_capacity(df.height());

    // First TR value
    tr_values.push(high.get(0).unwrap_or(0.0) - low.get(0).unwrap_or(0.0));

    for i in 1..df.height() {
        let h = high.get(i).unwrap_or(0.0);
        let l = low.get(i).unwrap_or(0.0);
        let cp = close_prev.get(i).unwrap_or(0.0);

        let tr = (h - l).max((h - cp).abs()).max((l - cp).abs());
        tr_values.push(tr);
    }

    let tr_series = Series::new("tr".into(), tr_values);

    // Calculate smoothed TR
    let atr = tr_series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })?;

    // Calculate +DI as (100 * smoothed +DM) / ATR
    let mut plus_di_values = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        let plus_dm_val = plus_dm.f64()?.get(i).unwrap_or(0.0);
        let atr_val = atr.f64()?.get(i).unwrap_or(1.0); // Avoid division by zero

        if atr_val > 0.0 {
            plus_di_values.push((100.0 * plus_dm_val) / atr_val);
        } else {
            plus_di_values.push(0.0);
        }
    }

    Ok(Series::new("plus_di".into(), plus_di_values))
}
