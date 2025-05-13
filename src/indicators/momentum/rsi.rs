use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Relative Strength Index (RSI)
/// Formula: RSI = 100 - (100 / (1 + RS))
/// where RS = Average Gain / Average Loss
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for RSI (typically 14)
/// * `column` - Column name to use for calculations (default "close")
///
/// # Returns
///
/// Returns a PolarsResult containing the RSI Series
pub fn calculate_rsi(df: &DataFrame, window: usize, column: &str) -> PolarsResult<Series> {
    check_window_size(df, window, "RSI")?;

    let price = df.column(column)?.f64()?;
    let mut rsi_values = Vec::with_capacity(df.height());

    // Fill initial values with NaN until we have enough data
    for _ in 0..window {
        rsi_values.push(f64::NAN);
    }

    if df.height() <= window {
        return Ok(Series::new("rsi".into(), rsi_values));
    }

    // Calculate first differences
    let mut gains = Vec::with_capacity(df.height() - 1);
    let mut losses = Vec::with_capacity(df.height() - 1);

    for i in 1..df.height() {
        let current = price.get(i).unwrap_or(f64::NAN);
        let previous = price.get(i - 1).unwrap_or(f64::NAN);

        if current.is_nan() || previous.is_nan() {
            gains.push(0.0);
            losses.push(0.0);
            continue;
        }

        let change = current - previous;

        if change > 0.0 {
            gains.push(change);
            losses.push(0.0);
        } else {
            gains.push(0.0);
            losses.push(change.abs());
        }
    }

    // Calculate first average gain and loss
    let mut avg_gain = gains.iter().take(window).sum::<f64>() / window as f64;
    let mut avg_loss = losses.iter().take(window).sum::<f64>() / window as f64;

    // Calculate first RSI
    let rs = if avg_loss == 0.0 {
        100.0
    } else {
        avg_gain / avg_loss
    };
    let rsi = 100.0 - (100.0 / (1.0 + rs));
    rsi_values.push(rsi);

    // Calculate remaining RSI values using smoothed averages
    for i in window + 1..df.height() {
        let idx = i - 1;
        let gain = gains[idx - 1];
        let loss = losses[idx - 1];

        // Smoothed averages formula
        avg_gain = ((avg_gain * (window - 1) as f64) + gain) / window as f64;
        avg_loss = ((avg_loss * (window - 1) as f64) + loss) / window as f64;

        let rs = if avg_loss == 0.0 {
            100.0
        } else {
            avg_gain / avg_loss
        };
        let rsi = 100.0 - (100.0 / (1.0 + rs));
        rsi_values.push(rsi);
    }

    Ok(Series::new("rsi".into(), rsi_values))
}
