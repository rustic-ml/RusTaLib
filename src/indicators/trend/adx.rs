use super::minus_di::calculate_minus_di;
use super::plus_di::calculate_plus_di;
use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates the Average Directional Movement Index (ADX)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data with high, low, close columns
/// * `window` - Window size for ADX calculation (typically 14)
///
/// # Returns
///
/// Returns a PolarsResult containing the ADX Series
pub fn calculate_adx(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    check_window_size(df, window, "ADX")?;

    // Calculate +DI and -DI first
    let plus_di = calculate_plus_di(df, window)?;
    let minus_di = calculate_minus_di(df, window)?;

    // Calculate the directional movement index DX
    let mut dx_values = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        let plus_di_val = plus_di.f64()?.get(i).unwrap_or(0.0);
        let minus_di_val = minus_di.f64()?.get(i).unwrap_or(0.0);

        if plus_di_val + minus_di_val > 0.0 {
            let dx = (((plus_di_val - minus_di_val).abs()) / (plus_di_val + minus_di_val)) * 100.0;
            dx_values.push(dx);
        } else {
            dx_values.push(0.0);
        }
    }

    let dx_series = Series::new("dx".into(), dx_values);

    // Apply EMA on DX to get ADX
    let adx = dx_series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })?;

    Ok(adx.with_name("adx".into()))
}
