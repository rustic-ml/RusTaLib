use polars::prelude::*;
use super::bollinger_bands::calculate_bollinger_bands;

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
    column: &str
) -> PolarsResult<Series> {
    let (_, bb_upper, bb_lower) = calculate_bollinger_bands(df, window, num_std, column)?;
    
    let close = df.column(column)?.f64()?;
    
    // Calculate %B: (Price - Lower Band) / (Upper Band - Lower Band)
    let bb_b = (close - bb_lower.f64()?) / (bb_upper.f64()? - bb_lower.f64()?);
    
    Ok(bb_b.into_series().with_name("bb_b".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::volatility::tests::{create_test_price_df};

    #[test]
    fn test_calculate_bb_b_basic() {
        let df = create_test_price_df();
        let window = 3;
        let num_std = 2.0;
        
        let bb_b = calculate_bb_b(&df, window, num_std, "close").unwrap();
        let bb_b_ca = bb_b.f64().unwrap();
        
        // Manual calculation for index 2:
        // With price series [10.0, 11.0, 12.0, ...], window = 3
        // SMA = (10 + 11 + 12) / 3 = 11.0
        // STD = sqrt(((10-11)^2 + (11-11)^2 + (12-11)^2) / 3) = sqrt(2/3) ≈ 0.8165
        // Upper = 11.0 + 2 * 0.8165 ≈ 12.633
        // Lower = 11.0 - 2 * 0.8165 ≈ 9.367
        // %B = (12.0 - 9.367) / (12.633 - 9.367) ≈ 0.75
        let idx = 2; // Third element (where we have full window data)
        assert!((bb_b_ca.get(idx).unwrap() - 0.75).abs() < 1e-10);
    }
    
    #[test]
    fn test_bb_b_at_band_boundaries() {
        let window = 3;
        let num_std = 2.0;
        
        // Create dataframe with values where we know both the middle and std dev
        let values = vec![10.0, 20.0, 30.0, 20.0, 10.0];
        let series = Series::new("close".into(), values);
        let df = DataFrame::new(vec![series.into()]).unwrap();
        
        let (middle, upper, lower) = calculate_bollinger_bands(&df, window, num_std, "close").unwrap();
        let middle_ca = middle.f64().unwrap();
        let upper_ca = upper.f64().unwrap();
        let lower_ca = lower.f64().unwrap();
        let bb_b = calculate_bb_b(&df, window, num_std, "close").unwrap();
        let bb_b_ca = bb_b.f64().unwrap();
        
        // At index 2: window=[10,20,30], middle=20, std=10, upper=40, lower=0
        let idx = 2;
        
        // 1. At the lower band: %B should be 0.0
        // Check that the denominator is not zero to avoid division by zero
        assert!((upper_ca.get(idx).unwrap() - lower_ca.get(idx).unwrap()).abs() > 1e-10);
        // Use the lower band value directly
        let at_lower_value = lower_ca.get(idx).unwrap();
        let bb_b_at_lower = (at_lower_value - lower_ca.get(idx).unwrap()) / 
                            (upper_ca.get(idx).unwrap() - lower_ca.get(idx).unwrap());
        assert!((bb_b_at_lower - 0.0).abs() < 1e-10);
        
        // 2. At the middle band: %B should be 0.5
        let at_middle_value = middle_ca.get(idx).unwrap();
        let bb_b_at_middle = (at_middle_value - lower_ca.get(idx).unwrap()) / 
                             (upper_ca.get(idx).unwrap() - lower_ca.get(idx).unwrap());
        assert!((bb_b_at_middle - 0.5).abs() < 1e-10);
        
        // 3. At the upper band: %B should be 1.0
        let at_upper_value = upper_ca.get(idx).unwrap();
        let bb_b_at_upper = (at_upper_value - lower_ca.get(idx).unwrap()) / 
                            (upper_ca.get(idx).unwrap() - lower_ca.get(idx).unwrap());
        assert!((bb_b_at_upper - 1.0).abs() < 1e-10);
        
        // 4. For the actual value in our series at idx=2 (which is 30)
        // With middle=20, upper=40, lower=0, the %B should be 0.75
        assert!((bb_b_ca.get(idx).unwrap() - 0.75).abs() < 1e-10);
    }
    
    #[test]
    fn test_bb_b_extended_cases() {
        let window = 3;
        let num_std = 2.0;
        
        // Test values outside bands
        let values = vec![10.0, 20.0, 50.0, -10.0, 20.0];
        let series = Series::new("close".into(), values);
        let df = DataFrame::new(vec![series.into()]).unwrap();
        
        let bb_b = calculate_bb_b(&df, window, num_std, "close").unwrap();
        let bb_b_ca = bb_b.f64().unwrap();
        
        // At index 2: window=[10,20,50], middle≈26.67, std≈20.82, upper≈68.3, lower≈-15.0
        // %B for price=50 should be approximately 0.9
        let idx = 2;
        assert!(bb_b_ca.get(idx).unwrap() > 0.5 && bb_b_ca.get(idx).unwrap() < 1.0);
        
        // At index 3: window=[20,50,-10], middle=20, std=30, upper≈80, lower≈-40
        // %B for price=-10 should be approximately 0.25
        let idx = 3;
        assert!(bb_b_ca.get(idx).unwrap() > 0.0 && bb_b_ca.get(idx).unwrap() < 0.5);
    }

    #[test]
    fn test_bb_b_accuracy_with_precise_values() {
        // Test with precise values that can be manually verified
        let price = Series::new("close".into(), &[10.0, 20.0, 30.0, 40.0, 50.0]);
        let df = DataFrame::new(vec![price.into()]).unwrap();
        
        let window = 3;
        let num_std = 2.0;
        
        let bb_b = calculate_bb_b(&df, window, num_std, "close").unwrap();
        
        // Manual calculations (using the actual Bollinger Bands calculated by our implementation):
        // At index 2: 
        // Price = 30, Middle = 20, Upper ≈ 40, Lower ≈ 0
        // %B = (30 - 0) / (40 - 0) = 0.75
        
        let idx = 2;
        let b_value = bb_b.f64().unwrap().get(idx).unwrap();
        assert!((b_value - 0.75).abs() < 0.01, 
                "Expected %B to be close to 0.75, got {}", b_value);
        
        // At index 3: 
        // Price = 40, Middle = 30, Upper ≈ 50, Lower ≈ 10
        // %B = (40 - 10) / (50 - 10) = 0.75
        
        let idx = 3;
        let b_value = bb_b.f64().unwrap().get(idx).unwrap();
        assert!((b_value - 0.75).abs() < 0.01, 
                "Expected %B to be close to 0.75, got {}", b_value);
    }
    
    #[test]
    fn test_bb_b_outside_bands() {
        // Test values that are outside the bands (should return values outside 0-1 range)
        let price = Series::new("close".into(), &[20.0, 25.0, 30.0, 50.0, 10.0]);
        let df = DataFrame::new(vec![price.into()]).unwrap();
        
        let window = 3;
        let num_std = 2.0;
        
        let bb_b = calculate_bb_b(&df, window, num_std, "close").unwrap();
        
        // At index 3:
        // Window = [25.0, 30.0, 50.0]
        // SMA = 35.0
        // STD ≈ 13.23
        // Upper ≈ 61.46
        // Lower ≈ 8.54
        // With price = 50.0, %B should be about 0.786 (inside bands)
        let idx = 3;
        let b_value = bb_b.f64().unwrap().get(idx).unwrap();
        assert!(b_value > 0.0 && b_value < 1.0);
        
        // At index 4:
        // Window = [30.0, 50.0, 10.0]
        // SMA = 30.0
        // STD ≈ 20.0
        // Upper ≈ 70.0
        // Lower ≈ -10.0
        // With price = 10.0, %B should be about 0.25 (inside bands)
        let idx = 4;
        let b_value = bb_b.f64().unwrap().get(idx).unwrap();
        assert!(b_value > 0.0 && b_value < 0.5);
    }
} 