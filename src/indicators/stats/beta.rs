use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Beta - regression coefficient between two series
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `price_column` - Column name for the price series (default "close")
/// * `market_column` - Column name for the market/benchmark series
/// * `window` - Window size for the calculation (typically 5)
///
/// # Returns
///
/// Returns a PolarsResult containing the Beta Series
pub fn calculate_beta(
    df: &DataFrame,
    price_column: &str,
    market_column: &str,
    window: usize,
) -> PolarsResult<Series> {
    check_window_size(df, window, "Beta")?;

    if !df.schema().contains(price_column) || !df.schema().contains(market_column) {
        return Err(PolarsError::ComputeError(
            format!("Beta calculation requires {price_column} and {market_column} columns").into(),
        ));
    }

    let price = df.column(price_column)?.f64()?;
    let market = df.column(market_column)?.f64()?;

    let mut beta_values = Vec::with_capacity(df.height());

    // Fill initial values with NaN
    for _ in 0..window - 1 {
        beta_values.push(f64::NAN);
    }

    // Calculate Beta for each window
    for i in window - 1..df.height() {
        let mut sum_xy = 0.0;
        let mut sum_x = 0.0;
        let mut sum_y = 0.0;
        let mut sum_x2 = 0.0;
        let mut count = 0;

        for j in 0..window {
            let x_idx = i - j;
            let x = market.get(x_idx).unwrap_or(f64::NAN);
            let y = price.get(x_idx).unwrap_or(f64::NAN);

            if !x.is_nan() && !y.is_nan() {
                sum_xy += x * y;
                sum_x += x;
                sum_y += y;
                sum_x2 += x * x;
                count += 1;
            }
        }

        if count > 1 {
            // Beta formula: (n*sum_xy - sum_x*sum_y) / (n*sum_x2 - sum_x^2)
            let numerator = (count as f64 * sum_xy) - (sum_x * sum_y);
            let denominator = (count as f64 * sum_x2) - (sum_x * sum_x);

            if denominator != 0.0 {
                beta_values.push(numerator / denominator);
            } else {
                beta_values.push(f64::NAN);
            }
        } else {
            beta_values.push(f64::NAN);
        }
    }

    Ok(Series::new("beta".into(), beta_values))
}
