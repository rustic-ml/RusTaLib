use polars::prelude::*;
use crate::indicators::oscillators::add_oscillator_indicators;
use crate::indicators::test_util::create_test_ohlcv_df;

// Helper function to create test price DataFrame
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