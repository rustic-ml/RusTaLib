// Volume indicators module

use polars::prelude::*;

// Modules for volume indicators
mod cmf;
mod mfi;
mod obv;

// Re-export volume indicators
pub use cmf::calculate_cmf;
pub use mfi::calculate_mfi;
pub use obv::calculate_obv;

/// Add volume-based indicators to a DataFrame
///
/// This function calculates and adds multiple volume-based indicators to the input DataFrame,
/// which is useful for combining multiple indicators in a single call.
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLCV data
///
/// # Returns
///
/// * `PolarsResult<DataFrame>` - DataFrame with added volume indicators
///
/// # Example
///
/// ```
/// use polars::prelude::*;
/// use ta_lib_in_rust::indicators::volume::add_volume_indicators;
///
/// // Create or load a DataFrame with OHLCV data
/// let df = DataFrame::default(); // Replace with actual data
///
/// // Add volume indicators
/// let df_with_indicators = add_volume_indicators(&df).unwrap();
/// ```
pub fn add_volume_indicators(df: &DataFrame) -> PolarsResult<DataFrame> {
    let mut result_df = df.clone();

    // Calculate On Balance Volume (OBV)
    let obv = calculate_obv(df)?;
    result_df.with_column(obv)?;

    // Calculate Chaikin Money Flow (CMF) with default period of 20
    let cmf = calculate_cmf(df, 20)?;
    result_df.with_column(cmf)?;

    // Calculate Money Flow Index (MFI) with default period of 14
    let mfi = calculate_mfi(df, 14)?;
    result_df.with_column(mfi)?;

    Ok(result_df)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::test_util::create_test_ohlcv_df;

    #[test]
    fn test_add_volume_indicators() {
        let df = create_test_ohlcv_df();
        let result = add_volume_indicators(&df).unwrap();

        // Check that indicators were added
        assert!(result.schema().contains("obv"));
        assert!(result.schema().contains("cmf_20"));
        assert!(result.schema().contains("mfi_14"));
    }
}
