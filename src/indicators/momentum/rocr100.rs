use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Rate of Change Ratio * 100 (ROCR100)
/// Formula: (price / prev_price) * 100
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for the calculation
/// * `column` - Column name to use for calculations (default "close")
///
/// # Returns
///
/// Returns a PolarsResult containing the ROCR100 Series
pub fn calculate_rocr100(df: &DataFrame, window: usize, column: &str) -> PolarsResult<Series> {
    check_window_size(df, window, "ROCR100")?;

    let price = df.column(column)?.f64()?;
    let prev_price = price.shift(window as i64);

    // Calculate ROCR100: (price / prev_price) * 100
    let mut rocr100_values = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        let current = price.get(i).unwrap_or(f64::NAN);
        let previous = prev_price.get(i).unwrap_or(f64::NAN);

        if !current.is_nan() && !previous.is_nan() && previous != 0.0 {
            let rocr100 = (current / previous) * 100.0;
            rocr100_values.push(rocr100);
        } else {
            rocr100_values.push(f64::NAN);
        }
    }

    Ok(Series::new("rocr100".into(), rocr100_values))
}
