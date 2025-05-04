use polars::prelude::*;

/// Placeholder for future implementation of Chaikin Money Flow
pub fn calculate_cmf(_df: &DataFrame, _window: usize) -> PolarsResult<Series> {
    unimplemented!("Chaikin Money Flow calculation not yet implemented")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::volume::tests::create_test_ohlcv_df;

    #[test]
    #[should_panic(expected = "not yet implemented")]
    fn test_calculate_cmf() {
        // Test that CMF function properly panics with "not yet implemented"
        let df = create_test_ohlcv_df();
        let _ = calculate_cmf(&df, 14).unwrap();
    }
}
