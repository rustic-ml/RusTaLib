use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Rate of Change Percentage (ROCP)
/// Formula: (price - prev_price) / prev_price
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for the rate of change calculation
/// * `column` - Column name to use for calculations (default "close")
///
/// # Returns
///
/// Returns a PolarsResult containing the ROCP Series
pub fn calculate_rocp(df: &DataFrame, window: usize, column: &str) -> PolarsResult<Series> {
    check_window_size(df, window, "ROCP")?;

    let price = df.column(column)?.f64()?;
    let prev_price = price.shift(window as i64);

    // Calculate ROCP: (price - prev_price) / prev_price
    let mut rocp_values = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        let current = price.get(i).unwrap_or(f64::NAN);
        let previous = prev_price.get(i).unwrap_or(f64::NAN);

        if !current.is_nan() && !previous.is_nan() && previous != 0.0 {
            let rocp = (current - previous) / previous;
            rocp_values.push(rocp);
        } else {
            rocp_values.push(f64::NAN);
        }
    }

    Ok(Series::new("rocp".into(), rocp_values))
} 