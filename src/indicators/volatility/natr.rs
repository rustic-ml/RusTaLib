use crate::indicators::volatility::calculate_atr;
use polars::prelude::*;

/// Calculates Normalized Average True Range (NATR)
/// NATR is the ATR value divided by the closing price, expressed as a percentage.
/// Formula: (ATR / Close) * 100
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for ATR (typically 14)
///
/// # Returns
///
/// Returns a PolarsResult containing the NATR Series
pub fn calculate_natr(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    // Calculate ATR
    let atr = calculate_atr(df, window)?;

    // Get closing prices
    let close = df.column("close")?.f64()?;

    // Calculate NATR: (ATR / Close) * 100
    let mut natr_values = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        let atr_val = atr.f64()?.get(i).unwrap_or(f64::NAN);
        let close_val = close.get(i).unwrap_or(f64::NAN);

        if !atr_val.is_nan() && !close_val.is_nan() && close_val != 0.0 {
            let natr = (atr_val / close_val) * 100.0;
            natr_values.push(natr);
        } else {
            natr_values.push(f64::NAN);
        }
    }

    Ok(Series::new("natr".into(), natr_values))
}
