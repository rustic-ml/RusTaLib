use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Average True Range (ATR)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for ATR (typically 14)
///
/// # Returns
///
/// Returns a PolarsResult containing the ATR Series
pub fn calculate_atr(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    check_window_size(df, window, "ATR")?;

    let high = df.column("high")?.f64()?.clone().into_series();
    let low = df.column("low")?.f64()?.clone().into_series();
    let close = df.column("close")?.f64()?.clone().into_series();

    let prev_close = close.shift(1);
    let mut tr_values = Vec::with_capacity(df.height());

    let first_tr = {
        let h = high.f64()?.get(0).unwrap_or(0.0);
        let l = low.f64()?.get(0).unwrap_or(0.0);
        h - l
    };
    tr_values.push(first_tr);

    for i in 1..df.height() {
        let h = high.f64()?.get(i).unwrap_or(0.0);
        let l = low.f64()?.get(i).unwrap_or(0.0);
        let pc = prev_close.f64()?.get(i).unwrap_or(0.0);

        let tr = if pc == 0.0 {
            h - l
        } else {
            (h - l).max((h - pc).abs()).max((l - pc).abs())
        };
        tr_values.push(tr);
    }

    // Implement Wilder's smoothing for ATR
    let mut atr_values = Vec::with_capacity(df.height());

    // Fill with NaN for the first window-1 elements
    for _ in 0..(window - 1) {
        atr_values.push(f64::NAN);
    }

    // Initialize ATR with simple average of first window TR values
    let mut atr = 0.0;
    for &tr in tr_values.iter().take(window) {
        atr += tr;
    }
    atr /= window as f64;
    atr_values.push(atr);

    // Apply Wilder's smoothing formula: ATR(t) = ((window-1) * ATR(t-1) + TR(t)) / window
    for &tr in tr_values.iter().skip(window) {
        atr = ((window as f64 - 1.0) * atr + tr) / window as f64;
        atr_values.push(atr);
    }

    Ok(Series::new("atr".into(), atr_values))
}
