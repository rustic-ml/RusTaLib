use polars::prelude::*;

/// Calculates Garman-Klass volatility estimator (uses OHLC data)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for smoothing (typically 10)
///
/// # Returns
///
/// Returns a PolarsResult containing the GK volatility Series
pub fn calculate_gk_volatility(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let open = df.column("open")?.f64()?;
    let close = df.column("close")?.f64()?;

    let mut gk_values = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        let h = high.get(i).unwrap_or(0.0);
        let l = low.get(i).unwrap_or(0.0);
        let o = open.get(i).unwrap_or(0.0);
        let c = close.get(i).unwrap_or(0.0);

        if h > 0.0 && l > 0.0 && o > 0.0 {
            let hl = (h / l).ln().powi(2) * 0.5;
            let co = (c / o).ln().powi(2);
            gk_values.push(hl - (2.0 * 0.386) * co);
        } else {
            gk_values.push(0.0);
        }
    }

    let gk_series = Series::new("gk_raw".into(), gk_values);

    // Apply rolling mean to get smoother estimate
    let gk_volatility = gk_series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: 1, // Allow calculation with fewer values
        center: false,
        weights: None,
        fn_params: None,
    })?;

    Ok(gk_volatility.with_name("gk_volatility".into()))
}
