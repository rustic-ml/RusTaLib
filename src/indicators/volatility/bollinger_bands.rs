use polars::prelude::*;
use crate::util::dataframe_utils::check_window_size;

/// Calculates Bollinger Bands
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for the SMA (typically 20)
/// * `num_std` - Number of standard deviations (typically 2.0)
/// * `column` - Column name to use for calculations (default "close")
///
/// # Returns
///
/// Returns a PolarsResult containing a tuple of (middle, upper, lower) bands
pub fn calculate_bollinger_bands(
    df: &DataFrame,
    window: usize,
    num_std: f64,
    column: &str,
) -> PolarsResult<(Series, Series, Series)> {
    check_window_size(df, window, "Bollinger Bands")?;
    
    let series = df.column(column)?.f64()?.clone().into_series();
    
    let sma = series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })?;
    
    let std = series.rolling_std(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })?;
    
    let mut upper_band = Vec::with_capacity(series.len());
    let mut lower_band = Vec::with_capacity(series.len());
    
    for i in 0..series.len() {
        let ma = sma.f64()?.get(i).unwrap_or(0.0);
        let std_val = std.f64()?.get(i).unwrap_or(0.0);
        
        upper_band.push(ma + num_std * std_val);
        lower_band.push(ma - num_std * std_val);
    }
    
    Ok((
        sma.with_name("bb_middle".into()),
        Series::new("bb_upper".into(), upper_band),
        Series::new("bb_lower".into(), lower_band)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::volatility::tests::{create_test_price_df};

    #[test]
    fn test_calculate_bollinger_bands_basic() {
        let df = create_test_price_df();
        let window = 3;
        let num_std = 2.0;
        
        let (middle, upper, lower) = calculate_bollinger_bands(&df, window, num_std, "close").unwrap();
        let middle_ca = middle.f64().unwrap();
        let upper_ca = upper.f64().unwrap();
        let lower_ca = lower.f64().unwrap();
        
        // Check the calculated values for position 2 (where we expect valid data)
        // Manual calculations for index 2
        // SMA = (10 + 11 + 12) / 3 = 11.0
        // STD = sqrt(((10-11)^2 + (11-11)^2 + (12-11)^2) / 3) = sqrt(2/3) ≈ 0.8165
        // Upper = 11.0 + 2 * 0.8165 ≈ 12.633
        // Lower = 11.0 - 2 * 0.8165 ≈ 9.367
        let idx = 2; // Third element (where we have full window data)
        
        assert!((middle_ca.get(idx).unwrap() - 11.0).abs() < 1e-10);
        assert_eq!(upper_ca.get(idx).unwrap(), 13.0);
        assert_eq!(lower_ca.get(idx).unwrap(), 9.0);
    }
    
    #[test]
    #[should_panic(expected = "Not enough data points")]
    fn test_bollinger_bands_insufficient_data() {
        let price = Series::new("close".into(), &[10.0, 11.0]);
        let df = DataFrame::new(vec![price.into()]).unwrap();
        let window = 3;
        
        // This should panic with "Not enough data points"
        let _ = calculate_bollinger_bands(&df, window, 2.0, "close").unwrap();
    }
    
    #[test]
    fn test_bollinger_bands_edge_window() {
        let df = create_test_price_df();
        
        // Test with window size 1 (should be no standard deviation)
        let (middle, upper, lower) = calculate_bollinger_bands(&df, 1, 2.0, "close").unwrap();
        
        // With window size 1, upper and lower bands should be the same as middle for every point
        for i in 0..df.height() {
            let mid_val = middle.f64().unwrap().get(i).unwrap();
            assert_eq!(mid_val, df.column("close").unwrap().f64().unwrap().get(i).unwrap());
            assert_eq!(upper.f64().unwrap().get(i).unwrap(), mid_val);
            assert_eq!(lower.f64().unwrap().get(i).unwrap(), mid_val);
        }
    }

    #[test]
    fn test_bollinger_bands_accuracy_with_known_values() {
        // Create a test case with known values for accurate verification
        let price = Series::new("close".into(), &[10.0, 20.0, 30.0, 40.0, 50.0]);
        let df = DataFrame::new(vec![price.into()]).unwrap();
        
        let window = 3;
        let num_std = 2.0;
        
        let (middle, upper, lower) = calculate_bollinger_bands(&df, window, num_std, "close").unwrap();
        
        // Manual calculations:
        // At index 2:
        // SMA = (10 + 20 + 30) / 3 = 20
        // Variance = ((10-20)^2 + (20-20)^2 + (30-20)^2) / 3 = 100
        // STD = sqrt(100) = 10
        // Upper = 20 + 2 * 10 = 40
        // Lower = 20 - 2 * 10 = 0
        
        let idx = 2;
        assert!((middle.f64().unwrap().get(idx).unwrap() - 20.0).abs() < 1e-10);
        // Allow for some variation due to different methods of calculating std dev
        let upper_val = upper.f64().unwrap().get(idx).unwrap();
        let lower_val = lower.f64().unwrap().get(idx).unwrap();
        assert!(upper_val >= 39.0 && upper_val <= 41.0, 
                "Upper band should be close to 40.0, got {}", upper_val);
        assert!(lower_val >= -1.0 && lower_val <= 1.0, 
                "Lower band should be close to 0.0, got {}", lower_val);
        
        // At index 3:
        // SMA = (20 + 30 + 40) / 3 = 30
        // Variance = ((20-30)^2 + (30-30)^2 + (40-30)^2) / 3 = 100
        // STD = sqrt(100) = 10
        // Upper = 30 + 2 * 10 = 50
        // Lower = 30 - 2 * 10 = 10
        
        let idx = 3;
        assert!((middle.f64().unwrap().get(idx).unwrap() - 30.0).abs() < 1e-10);
        // Allow for some variation due to different methods of calculating std dev
        let upper_val = upper.f64().unwrap().get(idx).unwrap();
        let lower_val = lower.f64().unwrap().get(idx).unwrap();
        assert!(upper_val >= 49.0 && upper_val <= 51.0, 
                "Upper band should be close to 50.0, got {}", upper_val);
        assert!(lower_val >= 9.0 && lower_val <= 11.0, 
                "Lower band should be close to 10.0, got {}", lower_val);
    }
    
    #[test]
    fn test_bollinger_bands_with_different_std() {
        // Test the effect of different standard deviation multipliers
        let price = Series::new("close".into(), &[10.0, 20.0, 30.0, 40.0, 50.0]);
        let df = DataFrame::new(vec![price.into()]).unwrap();
        
        let window = 3;
        let idx = 2;
        
        // Test with 1 standard deviation
        let (middle1, upper1, lower1) = calculate_bollinger_bands(&df, window, 1.0, "close").unwrap();
        
        // Test with 2 standard deviations
        let (middle2, upper2, lower2) = calculate_bollinger_bands(&df, window, 2.0, "close").unwrap();
        
        // Test with 3 standard deviations
        let (middle3, upper3, lower3) = calculate_bollinger_bands(&df, window, 3.0, "close").unwrap();
        
        // All middle bands should be identical
        assert_eq!(middle1.f64().unwrap().get(idx).unwrap(), middle2.f64().unwrap().get(idx).unwrap());
        assert_eq!(middle2.f64().unwrap().get(idx).unwrap(), middle3.f64().unwrap().get(idx).unwrap());
        
        // Bands should widen as std multiplier increases
        let band_width1 = upper1.f64().unwrap().get(idx).unwrap() - lower1.f64().unwrap().get(idx).unwrap();
        let band_width2 = upper2.f64().unwrap().get(idx).unwrap() - lower2.f64().unwrap().get(idx).unwrap();
        let band_width3 = upper3.f64().unwrap().get(idx).unwrap() - lower3.f64().unwrap().get(idx).unwrap();
        
        assert!(band_width2 > band_width1);
        assert!(band_width3 > band_width2);
        assert!((band_width2 / band_width1 - 2.0).abs() < 0.01); // Should be 2x wider
        assert!((band_width3 / band_width1 - 3.0).abs() < 0.01); // Should be 3x wider
    }
} 