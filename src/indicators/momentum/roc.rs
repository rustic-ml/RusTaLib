use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Rate of Change (ROC)
/// Formula: ((price / prevPrice) - 1) * 100
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for ROC (typically 10)
/// * `column` - Column name to use for calculations (default "close")
///
/// # Returns
///
/// Returns a PolarsResult containing the ROC Series
pub fn calculate_roc(df: &DataFrame, window: usize, column: &str) -> PolarsResult<Series> {
    check_window_size(df, window, "ROC")?;

    let price = df.column(column)?.f64()?;
    let prev_price = price.shift(window as i64);

    let mut roc_values = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        let current = price.get(i).unwrap_or(0.0);
        let prev = prev_price.get(i).unwrap_or(0.0);

        if prev != 0.0 {
            let roc = ((current / prev) - 1.0) * 100.0;
            roc_values.push(roc);
        } else {
            roc_values.push(f64::NAN);
        }
    }

    Ok(Series::new("roc".into(), roc_values))
}
