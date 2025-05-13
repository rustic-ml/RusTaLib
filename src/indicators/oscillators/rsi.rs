use polars::prelude::*;

/// Calculates Relative Strength Index (RSI)
///
/// # Arguments
///
/// * `df` - DataFrame containing price data
/// * `window` - RSI calculation period (typically 14)
/// * `column` - Column name to use for calculations (default "close")
///
/// # Returns
///
/// * `PolarsResult<Series>` - RSI values as a Series
pub fn calculate_rsi(df: &DataFrame, window: usize, column: &str) -> PolarsResult<Series> {
    // Check we have enough data
    if df.height() < window + 1 {
        return Err(PolarsError::ComputeError(
            format!(
                "Not enough data points for RSI calculation with window size {}",
                window
            )
            .into(),
        ));
    }

    // Get price data
    let close = df.column(column)?.f64()?.clone().into_series();

    // Calculate price changes
    let prev_close = close.shift(1);
    let price_diff: Vec<f64> = close
        .f64()?
        .iter()
        .zip(prev_close.f64()?.iter())
        .map(|(curr, prev)| match (curr, prev) {
            (Some(c), Some(p)) => c - p,
            _ => f64::NAN,
        })
        .collect();

    // Separate gains and losses
    let mut gains: Vec<f64> = Vec::with_capacity(df.height());
    let mut losses: Vec<f64> = Vec::with_capacity(df.height());

    // First value is NaN (no previous value to compare)
    gains.push(0.0);
    losses.push(0.0);

    for &diff in &price_diff[1..] {
        if diff.is_nan() {
            gains.push(f64::NAN);
            losses.push(f64::NAN);
        } else if diff > 0.0 {
            gains.push(diff);
            losses.push(0.0);
        } else {
            gains.push(0.0);
            losses.push(diff.abs());
        }
    }

    // Calculate RSI using Wilder's smoothing method
    let mut avg_gain = 0.0;
    let mut avg_loss = 0.0;
    let mut rsi: Vec<f64> = Vec::with_capacity(df.height());

    // Fill initial values with NaN
    for _i in 0..window {
        rsi.push(f64::NAN);
    }

    // First average gain/loss is a simple average
    for i in 1..=window {
        avg_gain += gains[i];
        avg_loss += losses[i];
    }
    avg_gain /= window as f64;
    avg_loss /= window as f64;

    // First RSI value
    let rs = if avg_loss == 0.0 {
        100.0 // Prevent division by zero
    } else {
        avg_gain / avg_loss
    };
    let rsi_val = 100.0 - (100.0 / (1.0 + rs));
    rsi[window - 1] = rsi_val;

    // Calculate smoothed RSI for the rest of the series
    for i in window + 1..df.height() {
        // Update using Wilder's smoothing
        avg_gain = ((avg_gain * (window - 1) as f64) + gains[i]) / window as f64;
        avg_loss = ((avg_loss * (window - 1) as f64) + losses[i]) / window as f64;

        // Calculate RSI
        let rs = if avg_loss == 0.0 {
            100.0 // Prevent division by zero
        } else {
            avg_gain / avg_loss
        };
        let rsi_val = 100.0 - (100.0 / (1.0 + rs));
        rsi.push(rsi_val);
    }

    Ok(Series::new(format!("rsi_{}", window).into(), rsi))
}
