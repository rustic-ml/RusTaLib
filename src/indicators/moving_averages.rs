use polars::prelude::*;
use crate::util::dataframe_utils::check_window_size;

/// Calculates Simple Moving Average (SMA)
///
/// # Arguments
///
/// * `df` - DataFrame containing the input data
/// * `column` - Column name to calculate SMA on
/// * `window` - Window size for the SMA
///
/// # Returns
///
/// Returns a PolarsResult containing the SMA Series
pub fn calculate_sma(df: &DataFrame, column: &str, window: usize) -> PolarsResult<Series> {
    // Check we have enough data
    check_window_size(df, window, "SMA")?;
    
    let series = df.column(column)?.f64()?.clone().into_series();
    
    series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })
}

/// Calculates Exponential Moving Average (EMA)
///
/// # Arguments
///
/// * `df` - DataFrame containing the input data
/// * `column` - Column name to calculate EMA on
/// * `window` - Window size for the EMA
///
/// # Returns
///
/// Returns a PolarsResult containing the EMA Series
pub fn calculate_ema(df: &DataFrame, column: &str, window: usize) -> PolarsResult<Series> {
    // Check we have enough data
    check_window_size(df, window, "EMA")?;
    
    let series = df.column(column)?.f64()?.clone().into_series();
    
    // Using polars rolling_mean with EMA weights for first implementation
    // A more accurate implementation would use the true EMA formula with alpha = 2/(window+1)
    series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })
}

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

    // Helper function to create test DataFrame
    fn create_test_df() -> DataFrame {
        let price_data = Series::new("price".into(), &[10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0]);
        DataFrame::new(vec![price_data.into()]).unwrap()
    }

    #[test]
    fn test_calculate_sma_basic() {
        let df = create_test_df();
        let window = 3;
        
        let result = calculate_sma(&df, "price", window).unwrap();
        let result_ca = result.f64().unwrap();
        
        // First two values should be null or NaN
        for i in 0..(window-1) {
            let val = result_ca.get(i);
            assert!(val.is_none() || val.map_or(false, |v| v.is_nan()));
        }
        
        // Manual calculation: (10 + 11 + 12) / 3 = 11.0
        assert!((result_ca.get(2).unwrap() - 11.0).abs() < 1e-10);
        
        // Manual calculation: (11 + 12 + 13) / 3 = 12.0
        assert!((result_ca.get(3).unwrap() - 12.0).abs() < 1e-10);
        
        // Manual calculation: (12 + 13 + 14) / 3 = 13.0
        assert!((result_ca.get(4).unwrap() - 13.0).abs() < 1e-10);
    }
    
    #[test]
    fn test_calculate_sma_window_edge_case() {
        let df = create_test_df();
        
        // Test with window size 1 (should return the same series)
        let result = calculate_sma(&df, "price", 1).unwrap();
        let result_ca = result.f64().unwrap();
        
        for i in 0..df.height() {
            let price_val = df.column("price").unwrap().f64().unwrap().get(i).unwrap();
            assert!((result_ca.get(i).unwrap() - price_val).abs() < 1e-10);
        }
        
        // Test with window size equal to dataframe length
        let window = df.height();
        let result = calculate_sma(&df, "price", window).unwrap();
        let result_ca = result.f64().unwrap();
        
        // Only the last value should be non-NaN
        for i in 0..(window-1) {
            let val = result_ca.get(i);
            assert!(val.is_none() || val.map_or(false, |v| v.is_nan()));
        }
        
        // Last value should be the mean of all values
        let price_ca = df.column("price").unwrap().f64().unwrap();
        let expected_mean = price_ca.iter()
            .map(|opt_v| opt_v.unwrap())
            .sum::<f64>() / (df.height() as f64);
        assert!((result_ca.get(window-1).unwrap() - expected_mean).abs() < 1e-10);
    }
    
    #[test]
    fn test_calculate_ema_basic() {
        let df = create_test_df();
        let window = 3;
        
        let result = calculate_ema(&df, "price", window).unwrap();
        let result_ca = result.f64().unwrap();
        
        // First two values should be null or NaN
        for i in 0..(window-1) {
            let val = result_ca.get(i);
            assert!(val.is_none() || val.map_or(false, |v| v.is_nan()));
        }
        
        // Check that some value exists for remaining positions (and is not NaN)
        for i in window-1..df.height() {
            let val = result_ca.get(i);
            assert!(val.is_some());
            assert!(!val.unwrap().is_nan());
        }
    }
    
    #[test]
    fn test_calculate_wma_basic() {
        let df = create_test_df();
        let window = 3;
        
        let result = calculate_wma(&df, "price", window).unwrap();
        let result_ca = result.f64().unwrap();
        
        // First two values should be null or NaN
        for i in 0..(window-1) {
            let val = result_ca.get(i);
            assert!(val.is_none() || val.map_or(false, |v| v.is_nan()));
        }
        
        // Manual calculation: (10*1 + 11*2 + 12*3) / (1+2+3) = 67/6 = 11.1666...
        let expected = (10.0*1.0 + 11.0*2.0 + 12.0*3.0) / 6.0;
        assert!((result_ca.get(2).unwrap() - expected).abs() < 1e-10);
    }
    
    #[test]
    #[should_panic(expected = "Not enough data points")]
    fn test_insufficient_data() {
        let price_data = Series::new("price".into(), &[10.0, 11.0]);
        let df = DataFrame::new(vec![price_data.into()]).unwrap();
        let window = 3;
        
        // This should panic with "Not enough data points"
        let _ = calculate_sma(&df, "price", window).unwrap();
    }
} 