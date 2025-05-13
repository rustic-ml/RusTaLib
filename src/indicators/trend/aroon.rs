use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates the Aroon indicator (Aroon Up and Aroon Down)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data with high, low columns
/// * `window` - Window size for calculation (typically 25)
///
/// # Returns
///
/// Returns a PolarsResult containing a tuple of (Aroon Up, Aroon Down) Series
pub fn calculate_aroon(df: &DataFrame, window: usize) -> PolarsResult<(Series, Series)> {
    check_window_size(df, window, "Aroon")?;

    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;

    let mut aroon_up = Vec::with_capacity(df.height());
    let mut aroon_down = Vec::with_capacity(df.height());

    // First values are NaN until we have enough data for window
    for _ in 0..window - 1 {
        aroon_up.push(f64::NAN);
        aroon_down.push(f64::NAN);
    }

    for i in window - 1..df.height() {
        let mut high_idx = 0;
        let mut low_idx = 0;
        let mut high_val = f64::MIN;
        let mut low_val = f64::MAX;

        // Find highest high and lowest low in window
        for j in 0..window {
            let h = high.get(i - j).unwrap_or(0.0);
            let l = low.get(i - j).unwrap_or(0.0);

            if h > high_val {
                high_val = h;
                high_idx = j;
            }

            if l < low_val {
                low_val = l;
                low_idx = j;
            }
        }

        // Calculate Aroon Up and Aroon Down
        let up = 100.0 * ((window as f64 - high_idx as f64) / window as f64);
        let down = 100.0 * ((window as f64 - low_idx as f64) / window as f64);

        aroon_up.push(up);
        aroon_down.push(down);
    }

    Ok((
        Series::new("aroon_up".into(), aroon_up),
        Series::new("aroon_down".into(), aroon_down),
    ))
}
