use super::bollinger_bands::calculate_bollinger_bands;
use polars::prelude::*;

/// Calculates Bollinger Band %B indicator
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for Bollinger Bands (typically 20)
/// * `num_std` - Number of standard deviations (typically 2.0)
/// * `column` - Column name to use for calculations (default "close")
///
/// # Returns
///
/// Returns a PolarsResult containing the %B Series
pub fn calculate_bb_b(
    df: &DataFrame,
    window: usize,
    num_std: f64,
    column: &str,
) -> PolarsResult<Series> {
    let (_, bb_upper, bb_lower) = calculate_bollinger_bands(df, window, num_std, column)?;

    let close = df.column(column)?.f64()?;

    // Calculate %B: (Price - Lower Band) / (Upper Band - Lower Band)
    let bb_b = (close - bb_lower.f64()?) / (bb_upper.f64()? - bb_lower.f64()?);

    Ok(bb_b.into_series().with_name("bb_b".into()))
}
