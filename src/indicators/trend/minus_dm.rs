use polars::prelude::*;
use crate::util::dataframe_utils::check_window_size;

/// Calculates Minus Directional Movement (-DM)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data with high, low columns
/// * `window` - Window size for smoothing (typically 14)
///
/// # Returns
///
/// Returns a PolarsResult containing the smoothed -DM Series
pub fn calculate_minus_dm(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    check_window_size(df, window, "-DM")?;
    
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    
    let high_prev = high.shift(1);
    let low_prev = low.shift(1);
    
    let mut dm_minus = Vec::with_capacity(df.height());
    
    // First value
    dm_minus.push(0.0);
    
    for i in 1..df.height() {
        let h = high.get(i).unwrap_or(0.0);
        let h_prev = high_prev.get(i).unwrap_or(0.0);
        let l = low.get(i).unwrap_or(0.0);
        let l_prev = low_prev.get(i).unwrap_or(0.0);
        
        let up_move = h - h_prev;
        let down_move = l_prev - l;
        
        if down_move > up_move && down_move > 0.0 {
            dm_minus.push(down_move);
        } else {
            dm_minus.push(0.0);
        }
    }
    
    let dm_minus_series = Series::new("dm_minus".into(), dm_minus);
    
    // Smooth the -DM
    let smoothed_dm_minus = dm_minus_series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })?;
    
    Ok(smoothed_dm_minus.with_name("minus_dm".into()))
} 