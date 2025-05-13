use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Momentum (MOM)
/// Formula: price - price_n_periods_ago
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for Momentum (typically 10)
/// * `column` - Column name to use for calculations (default "close")
///
/// # Returns
///
/// Returns a PolarsResult containing the Momentum Series
pub fn calculate_mom(df: &DataFrame, window: usize, column: &str) -> PolarsResult<Series> {
    check_window_size(df, window, "Momentum")?;

    let price = df.column(column)?.f64()?;
    let prev_price = price.shift(window as i64);

    let mut mom_values = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        let current = price.get(i).unwrap_or(f64::NAN);
        let prev = prev_price.get(i).unwrap_or(f64::NAN);

        if !current.is_nan() && !prev.is_nan() {
            let mom = current - prev;
            mom_values.push(mom);
        } else {
            mom_values.push(f64::NAN);
        }
    }

    Ok(Series::new("momentum".into(), mom_values))
} 