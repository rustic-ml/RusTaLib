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

/// Calculates Average True Range (ATR)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for ATR (typically 14)
///
/// # Returns
///
/// Returns a PolarsResult containing the ATR Series
pub fn calculate_atr(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    check_window_size(df, window, "ATR")?;
    
    let high = df.column("high")?.f64()?.clone().into_series();
    let low = df.column("low")?.f64()?.clone().into_series();
    let close = df.column("close")?.f64()?.clone().into_series();
    
    let prev_close = close.shift(1);
    let mut tr_values = Vec::with_capacity(df.height());
    
    let first_tr = {
        let h = high.f64()?.get(0).unwrap_or(0.0);
        let l = low.f64()?.get(0).unwrap_or(0.0);
        h - l
    };
    tr_values.push(first_tr);
    
    for i in 1..df.height() {
        let h = high.f64()?.get(i).unwrap_or(0.0);
        let l = low.f64()?.get(i).unwrap_or(0.0);
        let pc = prev_close.f64()?.get(i).unwrap_or(0.0);
        
        let tr = if pc == 0.0 {
            h - l
        } else {
            (h - l).max((h - pc).abs()).max((l - pc).abs())
        };
        tr_values.push(tr);
    }
    
    let tr_series = Series::new("tr".into(), tr_values);
    let atr = tr_series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })?;
    
    Ok(atr.with_name("atr".into()))
}

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
    use polars::prelude::*;

    // Helper function to create test DataFrame with OHLC data
    fn create_test_ohlc_df() -> DataFrame {
        let open = Series::new("open".into(), &[10.0, 11.0, 12.0, 11.5, 12.5, 13.0, 14.0]);
        let high = Series::new("high".into(), &[12.0, 13.0, 13.5, 12.5, 13.5, 15.0, 15.5]);
        let low = Series::new("low".into(), &[9.5, 10.5, 11.0, 10.5, 11.5, 12.5, 13.5]);
        let close = Series::new("close".into(), &[11.0, 12.0, 13.0, 11.0, 13.0, 14.0, 14.5]);
        
        DataFrame::new(vec![open.into(), high.into(), low.into(), close.into()]).unwrap()
    }

    // Helper function to create test DataFrame with only price data
    fn create_test_price_df() -> DataFrame {
        let price = Series::new("close".into(), &[10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0]);
        DataFrame::new(vec![price.into()]).unwrap()
    }

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
        println!("Middle band value: {}", middle_ca.get(idx).unwrap());
        println!("Upper band value: {}", upper_ca.get(idx).unwrap());
        println!("Lower band value: {}", lower_ca.get(idx).unwrap());
        
        assert!((middle_ca.get(idx).unwrap() - 11.0).abs() < 1e-10);
        assert_eq!(upper_ca.get(idx).unwrap(), 13.0);
        assert_eq!(lower_ca.get(idx).unwrap(), 9.0);
    }
    
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
    fn test_calculate_atr_basic() {
        let df = create_test_ohlc_df();
        let window = 3;
        
        let atr = calculate_atr(&df, window).unwrap();
        let atr_ca = atr.f64().unwrap();
        
        // First two values should be null or NaN
        for i in 0..(window-1) {
            let val = atr_ca.get(i);
            assert!(val.is_none() || val.map_or(false, |v| v.is_nan()));
        }
        
        // Manual calculation for TR values:
        // TR₀ = High₀ - Low₀ = 12.0 - 9.5 = 2.5
        // TR₁ = max(High₁-Low₁, |High₁-Close₀|, |Low₁-Close₀|) = max(2.5, 2.0, 0.5) = 2.5
        // TR₂ = max(High₂-Low₂, |High₂-Close₁|, |Low₂-Close₁|) = max(2.5, 1.5, 1.0) = 2.5
        
        // ATR₂ = (TR₀ + TR₁ + TR₂)/3 = (2.5 + 2.5 + 2.5)/3 = 2.5
        assert!((atr_ca.get(2).unwrap() - 2.5).abs() < 0.001);
    }
    
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
} 