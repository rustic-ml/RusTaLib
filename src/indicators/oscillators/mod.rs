// Oscillators module

use polars::prelude::*;

// Module declarations
mod macd;
mod rsi;
mod stochastic;
mod williams_r;

// Re-export functions
pub use macd::calculate_macd;
pub use rsi::calculate_rsi;
pub use stochastic::calculate_stochastic;
pub use williams_r::calculate_williams_r;

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    // Helper function to create test DataFrame
    pub fn create_test_price_df() -> DataFrame {
        let price = Series::new(
            "close".into(),
            &[
                10.0, 11.0, 10.5, 10.0, 10.5, 11.5, 12.0, 12.5, 12.0, 11.0, 10.0, 9.5, 9.0, 9.5,
                10.0,
            ],
        );
        DataFrame::new(vec![price.into()]).unwrap()
    }
}

/// Add oscillator indicators to a DataFrame
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLCV data
///
/// # Returns
///
/// * `PolarsResult<DataFrame>` - DataFrame with added oscillator indicators
///
/// # Example
///
/// ```
/// use polars::prelude::*;
/// use ta_lib_in_rust::indicators::oscillators::add_oscillator_indicators;
///
/// // Create or load a DataFrame with OHLCV data
/// let df = DataFrame::default(); // Replace with actual data
///
/// // Add oscillator indicators
/// let df_with_indicators = add_oscillator_indicators(&df).unwrap();
/// ```
pub fn add_oscillator_indicators(df: &DataFrame) -> PolarsResult<DataFrame> {
    let mut result_df = df.clone();

    // RSI
    let rsi_14 = calculate_rsi(df, 14, "close")?;
    result_df.with_column(rsi_14)?;

    // MACD
    let (macd, macd_signal) = calculate_macd(df, 12, 26, 9, "close")?;
    result_df.with_column(macd)?;
    result_df.with_column(macd_signal)?;

    // Williams %R
    let williams_r_14 = calculate_williams_r(df, 14)?;
    result_df.with_column(williams_r_14)?;

    // Stochastic Oscillator
    let (stoch_k, stoch_d) = calculate_stochastic(df, 14, 3, 3)?;
    result_df.with_column(stoch_k)?;
    result_df.with_column(stoch_d)?;

    Ok(result_df)
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::indicators::test_util::create_test_ohlcv_df;

    #[test]
    fn test_add_oscillator_indicators() {
        let df = create_test_ohlcv_df();
        let result = add_oscillator_indicators(&df).unwrap();

        // Check that indicators were added
        assert!(result.schema().contains("rsi_14"));
        assert!(result.schema().contains("macd_12_26"));
        assert!(result.schema().contains("macd_signal_12_26_9"));
        assert!(result.schema().contains("williams_r_14"));
        assert!(result.schema().contains("stoch_k_14_3_3"));
        assert!(result.schema().contains("stoch_d_14_3_3"));
    }
}
