// Trend indicators module

mod adx;
mod adxr;
mod aroon;
mod aroon_osc;
mod minus_di;
mod minus_dm;
mod plus_di;
mod plus_dm;
mod psar;

// Re-export indicators
pub use adx::calculate_adx;
pub use adxr::calculate_adxr;
pub use aroon::calculate_aroon;
pub use aroon_osc::calculate_aroon_osc;
pub use minus_di::calculate_minus_di;
pub use minus_dm::calculate_minus_dm;
pub use plus_di::calculate_plus_di;
pub use plus_dm::calculate_plus_dm;
pub use psar::calculate_psar;

use polars::prelude::*;

/// Add trend indicators to a DataFrame
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLCV data
///
/// # Returns
///
/// * `PolarsResult<DataFrame>` - DataFrame with added trend indicators
///
/// # Example
///
/// ```
/// use polars::prelude::*;
/// use ta_lib_in_rust::indicators::trend::add_trend_indicators;
///
/// // Create or load a DataFrame with OHLCV data
/// let df = DataFrame::default(); // Replace with actual data
///
/// // Add trend indicators
/// let df_with_indicators = add_trend_indicators(&df).unwrap();
/// ```
pub fn add_trend_indicators(df: &DataFrame) -> PolarsResult<DataFrame> {
    let mut result_df = df.clone();

    // Parabolic SAR
    let psar = calculate_psar(df, 0.02, 0.2)?;
    result_df.with_column(psar)?;

    Ok(result_df)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::test_util::create_test_ohlcv_df;

    #[test]
    fn test_add_trend_indicators() {
        let df = create_test_ohlcv_df();
        let result = add_trend_indicators(&df).unwrap();

        // Check that indicators were added
        assert!(result.schema().contains("psar_0_02_0_20"));
    }
}
