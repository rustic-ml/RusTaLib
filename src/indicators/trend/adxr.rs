use polars::prelude::*;
use crate::util::dataframe_utils::check_window_size;
use super::adx::calculate_adx;

/// Calculates the Average Directional Movement Index Rating (ADXR)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data with high, low, close columns
/// * `window` - Window size for ADX calculation (typically 14)
///
/// # Returns
///
/// Returns a PolarsResult containing the ADXR Series (average of current ADX and ADX from "window" periods ago)
pub fn calculate_adxr(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    check_window_size(df, window * 2, "ADXR")?;
    
    let adx = calculate_adx(df, window)?;
    let adx_prev = adx.shift(window as i64);
    
    // Calculate ADXR as average of current ADX and the ADX from "window" periods ago
    let adxr = (&adx + &adx_prev)? / 2.0;
    
    Ok(adxr.with_name("adxr".into()))
} 