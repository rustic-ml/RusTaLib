use polars::prelude::*;

/// Calculates Garman-Klass volatility estimator (uses OHLC data)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for smoothing (typically 10)
///
/// # Returns
///
/// Returns a PolarsResult containing the GK volatility Series
pub fn calculate_gk_volatility(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let open = df.column("open")?.f64()?;
    let close = df.column("close")?.f64()?;
    
    let mut gk_values = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let h = high.get(i).unwrap_or(0.0);
        let l = low.get(i).unwrap_or(0.0);
        let o = open.get(i).unwrap_or(0.0);
        let c = close.get(i).unwrap_or(0.0);
        
        if h > 0.0 && l > 0.0 && o > 0.0 {
            let hl = (h/l).ln().powi(2) * 0.5;
            let co = (c/o).ln().powi(2);
            gk_values.push(hl - (2.0 * 0.386) * co);
        } else {
            gk_values.push(0.0);
        }
    }
    
    let gk_series = Series::new("gk_raw".into(), gk_values);
    
    // Apply rolling mean to get smoother estimate
    let gk_volatility = gk_series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: 1, // Allow calculation with fewer values
        center: false,
        weights: None,
        fn_params: None,
    })?;
    
    Ok(gk_volatility.with_name("gk_volatility".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::volatility::tests::create_test_ohlc_df;

    #[test]
    fn test_calculate_gk_volatility_basic() {
        let df = create_test_ohlc_df();
        let window = 3;
        
        let gk = calculate_gk_volatility(&df, window).unwrap();
        
        // All values should have a value (not NaN) since min_periods is 1
        for i in 0..df.height() {
            assert!(!gk.f64().unwrap().get(i).unwrap().is_nan());
        }
        
        // Test that values are positive (volatility is always positive)
        for i in 0..df.height() {
            assert!(gk.f64().unwrap().get(i).unwrap() >= 0.0);
        }
    }
    
    #[test]
    fn test_gk_volatility_flat_prices() {
        // Test with flat prices (no volatility)
        let open = Series::new("open".into(), &[10.0, 10.0, 10.0, 10.0, 10.0]);
        let high = Series::new("high".into(), &[10.0, 10.0, 10.0, 10.0, 10.0]);
        let low = Series::new("low".into(), &[10.0, 10.0, 10.0, 10.0, 10.0]);
        let close = Series::new("close".into(), &[10.0, 10.0, 10.0, 10.0, 10.0]);
        let df = DataFrame::new(vec![open.into(), high.into(), low.into(), close.into()]).unwrap();
        
        let window = 3;
        let gk = calculate_gk_volatility(&df, window).unwrap();
        
        // GK volatility should be close to zero with no price movement
        for i in 0..df.height() {
            assert!(gk.f64().unwrap().get(i).unwrap() < 0.001);
        }
    }
    
    #[test]
    fn test_gk_volatility_formula_verification() {
        // Test with specific values for manual verification
        let open = Series::new("open".into(), &[100.0, 101.0, 102.0]);
        let high = Series::new("high".into(), &[105.0, 106.0, 107.0]);
        let low = Series::new("low".into(), &[95.0, 96.0, 97.0]);
        let close = Series::new("close".into(), &[101.0, 102.0, 103.0]);
        let df = DataFrame::new(vec![open.into(), high.into(), low.into(), close.into()]).unwrap();
        
        let window = 1; // Use window=1 to get raw GK values without smoothing
        let gk = calculate_gk_volatility(&df, window).unwrap();
        
        // Manual calculation for the first row:
        // h = 105.0, l = 95.0, o = 100.0, c = 101.0
        // hl = 0.5 * ln(105/95)^2 = 0.5 * ln(1.105)^2 = 0.5 * 0.0997^2 = 0.00497
        // co = ln(101/100)^2 = ln(1.01)^2 = 0.00995^2 = 0.000099
        // gk = 0.00497 - (2*0.386)*0.000099 = 0.00497 - 0.000076 = 0.00489
        
        let idx = 0;
        let hl_term = 0.5 * ((105.0/95.0) as f64).ln().powi(2);
        let co_term = ((101.0/100.0) as f64).ln().powi(2);
        let expected = hl_term - (2.0 * 0.386) * co_term;
        assert!((gk.f64().unwrap().get(idx).unwrap() - expected).abs() < 0.0001);
    }
    
    #[test]
    fn test_gk_volatility_with_extreme_values() {
        // Test with extreme price movements
        let open = Series::new("open".into(), &[100.0, 100.0, 100.0, 100.0, 100.0]);
        let high = Series::new("high".into(), &[105.0, 110.0, 120.0, 130.0, 140.0]);
        let low = Series::new("low".into(), &[95.0, 90.0, 80.0, 70.0, 60.0]);
        let close = Series::new("close".into(), &[101.0, 102.0, 105.0, 110.0, 120.0]);
        let df = DataFrame::new(vec![open.into(), high.into(), low.into(), close.into()]).unwrap();
        
        let window = 3;
        let gk = calculate_gk_volatility(&df, window).unwrap();
        
        // Volatility should increase with wider price ranges
        let v0 = gk.f64().unwrap().get(0).unwrap();
        let v4 = gk.f64().unwrap().get(4).unwrap();
        
        // The last value should show higher volatility than the first
        assert!(v4 > v0);
    }
    
    #[test]
    fn test_gk_volatility_smoothing() {
        // Test that the smoothing is working correctly
        let mut open = vec![100.0];
        let mut high = vec![110.0];
        let mut low = vec![90.0];
        let mut close = vec![105.0];
        
        // Add some stable prices
        for i in 1..5 {
            open.push(100.0 + i as f64);
            high.push(102.0 + i as f64);
            low.push(98.0 + i as f64);
            close.push(101.0 + i as f64);
        }
        
        // Add a sudden volatility spike
        open.push(105.0);
        high.push(130.0);
        low.push(80.0);
        close.push(110.0);
        
        // Back to normal
        for i in 0..3 {
            open.push(110.0 + i as f64);
            high.push(112.0 + i as f64);
            low.push(108.0 + i as f64);
            close.push(111.0 + i as f64);
        }
        
        let df = DataFrame::new(vec![
            Series::new("open".into(), open).into(),
            Series::new("high".into(), high).into(),
            Series::new("low".into(), low).into(),
            Series::new("close".into(), close).into(),
        ]).unwrap();
        
        // Calculate with different window sizes
        let gk1 = calculate_gk_volatility(&df, 1).unwrap();
        let gk3 = calculate_gk_volatility(&df, 3).unwrap();
        
        // The spike should be more pronounced with window=1
        let spike_idx = 5;
        assert!(gk1.f64().unwrap().get(spike_idx).unwrap() > gk3.f64().unwrap().get(spike_idx).unwrap());
        
        // The effect should linger longer with window=3
        let post_spike_idx = 7;
        assert!(gk3.f64().unwrap().get(post_spike_idx).unwrap() > gk1.f64().unwrap().get(post_spike_idx).unwrap());
    }
} 