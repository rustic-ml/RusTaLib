use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Weighted Moving Average (WMA)
///
/// # Arguments
///
/// * `df` - DataFrame containing the input data
/// * `column` - Column name to calculate WMA on
/// * `window` - Window size for the WMA
///
/// # Returns
///
/// Returns a PolarsResult containing the WMA Series
pub fn calculate_wma(df: &DataFrame, column: &str, window: usize) -> PolarsResult<Series> {
    // Check we have enough data
    check_window_size(df, window, "WMA")?;

    let series = df.column(column)?.f64()?.clone().into_series();

    // Create linear weights [1, 2, 3, ..., window]
    let weights: Vec<f64> = (1..=window).map(|i| i as f64).collect();

    // Calculate WMA using rolling_mean with weights
    series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: Some(weights),
        fn_params: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::moving_averages::tests::create_test_df;

    #[test]
    fn test_calculate_wma_basic() {
        let df = create_test_df();
        let window = 3;

        let result = calculate_wma(&df, "price", window).unwrap();
        let result_ca = result.f64().unwrap();

        // First two values should be null or NaN
        for i in 0..(window - 1) {
            let val = result_ca.get(i);
            assert!(val.is_none() || val.map_or(false, |v| v.is_nan()));
        }

        // Manual calculation: (10*1 + 11*2 + 12*3) / (1+2+3) = 67/6 = 11.1666...
        let expected = (10.0 * 1.0 + 11.0 * 2.0 + 12.0 * 3.0) / 6.0;
        assert!((result_ca.get(2).unwrap() - expected).abs() < 1e-10);

        // Manual calculation for next position: (11*1 + 12*2 + 13*3) / (1+2+3) = 74/6 = 12.333...
        let expected = (11.0 * 1.0 + 12.0 * 2.0 + 13.0 * 3.0) / 6.0;
        assert!((result_ca.get(3).unwrap() - expected).abs() < 1e-10);
    }

    #[test]
    fn test_wma_window_edge_cases() {
        let df = create_test_df();

        // Test with window size 1 (should return the same series)
        let result = calculate_wma(&df, "price", 1).unwrap();
        let result_ca = result.f64().unwrap();

        for i in 0..df.height() {
            let price_val = df.column("price").unwrap().f64().unwrap().get(i).unwrap();
            assert!((result_ca.get(i).unwrap() - price_val).abs() < 1e-10);
        }

        // Test with window size 2
        let window = 2;
        let result = calculate_wma(&df, "price", window).unwrap();
        let result_ca = result.f64().unwrap();

        // Manual calculation: (10*1 + 11*2) / (1+2) = 32/3 = 10.667...
        let expected = (10.0 * 1.0 + 11.0 * 2.0) / 3.0;
        assert!((result_ca.get(1).unwrap() - expected).abs() < 1e-10);
    }

    #[test]
    fn test_wma_weights_comparison() {
        // Test that WMA gives more weight to recent prices
        let price_data = Series::new("price".into(), &[20.0, 15.0, 10.0, 5.0]);
        let df = DataFrame::new(vec![price_data.into()]).unwrap();

        let window = 3;
        let result = calculate_wma(&df, "price", window).unwrap();
        let result_ca = result.f64().unwrap();

        // WMA for index 2 should be: (20*1 + 15*2 + 10*3) / (1+2+3) = 80/6 = 13.333...
        let expected = (20.0 * 1.0 + 15.0 * 2.0 + 10.0 * 3.0) / 6.0;
        assert!((result_ca.get(2).unwrap() - expected).abs() < 1e-10);

        // Since WMA weights more recent prices higher, with decreasing prices
        // the WMA should be lower than a simple average
        let sma = (20.0 + 15.0 + 10.0) / 3.0; // 15.0
        assert!(result_ca.get(2).unwrap() < sma);
    }

    #[test]
    #[should_panic(expected = "Not enough data points")]
    fn test_insufficient_data() {
        let price_data = Series::new("price".into(), &[10.0, 11.0]);
        let df = DataFrame::new(vec![price_data.into()]).unwrap();
        let window = 3;

        // This should panic with "Not enough data points"
        let _ = calculate_wma(&df, "price", window).unwrap();
    }
}
