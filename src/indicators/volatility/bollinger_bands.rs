use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Bollinger Bands
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for the SMA (typically 20)
/// * `num_std` - Number of standard deviations (typically 2.0)
/// * `column` - Column name to use for calculations (default "close")
///
/// # Returns
///
/// Returns a PolarsResult containing a tuple of (middle, upper, lower) bands
pub fn calculate_bollinger_bands(
    df: &DataFrame,
    window: usize,
    num_std: f64,
    column: &str,
) -> PolarsResult<(Series, Series, Series)> {
    check_window_size(df, window, "Bollinger Bands")?;

    let series = df.column(column)?.f64()?.clone().into_series();

    let sma = series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })?;

    let std = series.rolling_std(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })?;

    let mut upper_band = Vec::with_capacity(series.len());
    let mut lower_band = Vec::with_capacity(series.len());

    for i in 0..series.len() {
        let ma = sma.f64()?.get(i).unwrap_or(0.0);
        let std_val = std.f64()?.get(i).unwrap_or(0.0);

        upper_band.push(ma + num_std * std_val);
        lower_band.push(ma - num_std * std_val);
    }

    Ok((
        sma.with_name("bb_middle".into()),
        Series::new("bb_upper".into(), upper_band),
        Series::new("bb_lower".into(), lower_band),
    ))
}
