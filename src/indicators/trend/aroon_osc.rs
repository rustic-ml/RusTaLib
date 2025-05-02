use polars::prelude::*;
use super::aroon::calculate_aroon;

/// Calculates the Aroon Oscillator
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data with high, low columns
/// * `window` - Window size for calculation (typically 25)
///
/// # Returns
///
/// Returns a PolarsResult containing the Aroon Oscillator Series (Aroon Up - Aroon Down)
pub fn calculate_aroon_osc(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    let (aroon_up, aroon_down) = calculate_aroon(df, window)?;
    
    // Aroon Oscillator = Aroon Up - Aroon Down
    let aroon_osc = (&aroon_up - &aroon_down)?;
    
    Ok(aroon_osc.with_name("aroon_osc".into()))
} 