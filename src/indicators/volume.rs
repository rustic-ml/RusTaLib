use polars::prelude::*;

/// Calculates On-Balance Volume (OBV)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price and volume data
///
/// # Returns
///
/// Returns a PolarsResult containing the OBV Series
pub fn calculate_obv(df: &DataFrame) -> PolarsResult<Series> {
    // Check for required columns
    if !df.schema().contains("close") || !df.schema().contains("volume") {
        return Err(PolarsError::ComputeError(
            "OBV calculation requires both close and volume columns".into()
        ));
    }
    
    let close = df.column("close")?.f64()?;
    let prev_close = close.shift(1);
    let volume = df.column("volume")?.f64()?;
    
    let mut obv = Vec::with_capacity(df.height());
    let mut cumulative = 0.0;
    
    // First value
    cumulative = volume.get(0).unwrap_or(0.0);
    obv.push(cumulative);
    
    for i in 1..df.height() {
        let curr_close = close.get(i).unwrap_or(0.0);
        let prev_close_val = prev_close.get(i).unwrap_or(0.0);
        let curr_volume = volume.get(i).unwrap_or(0.0);
        
        if curr_close > prev_close_val {
            cumulative += curr_volume;
        } else if curr_close < prev_close_val {
            cumulative -= curr_volume;
        }
        // If equal, no change
        
        obv.push(cumulative);
    }
    
    Ok(Series::new("obv".into(), obv))
}

/// Placeholder for future implementation of Chaikin Money Flow
pub fn calculate_cmf(_df: &DataFrame, _window: usize) -> PolarsResult<Series> {
    unimplemented!("Chaikin Money Flow calculation not yet implemented")
} 

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;
    
    // Helper function to create test DataFrame with OHLCV data
    fn create_test_ohlcv_df() -> DataFrame {
        let open = Series::new("open".into(), &[10.0, 11.0, 12.0, 11.5, 12.5, 13.0, 14.0]);
        let high = Series::new("high".into(), &[12.0, 13.0, 13.5, 12.5, 13.5, 15.0, 15.5]);
        let low = Series::new("low".into(), &[9.5, 10.5, 11.0, 10.5, 11.5, 12.5, 13.5]);
        let close = Series::new("close".into(), &[11.0, 12.0, 13.0, 11.0, 13.0, 14.0, 14.5]);
        let volume = Series::new("volume".into(), &[1000.0, 1500.0, 2000.0, 1800.0, 2200.0, 2500.0, 3000.0]);
        
        DataFrame::new(vec![open.into(), high.into(), low.into(), close.into(), volume.into()]).unwrap()
    }
    
    #[test]
    fn test_calculate_obv_basic() {
        let df = create_test_ohlcv_df();
        let obv = calculate_obv(&df).unwrap();
        
        // OBV should have the same length as the dataframe
        assert_eq!(obv.len(), df.height());
        
        // First value should be equal to the first volume
        assert_eq!(obv.f64().unwrap().get(0).unwrap(), 1000.0);
        
        // Manual calculation:
        // i=0: OBV = 1000
        // i=1: close[1] > close[0], OBV = 1000 + 1500 = 2500
        // i=2: close[2] > close[1], OBV = 2500 + 2000 = 4500
        // i=3: close[3] < close[2], OBV = 4500 - 1800 = 2700
        // i=4: close[4] > close[3], OBV = 2700 + 2200 = 4900
        // i=5: close[5] > close[4], OBV = 4900 + 2500 = 7400
        // i=6: close[6] > close[5], OBV = 7400 + 3000 = 10400
        
        assert_eq!(obv.f64().unwrap().get(1).unwrap(), 2500.0);
        assert_eq!(obv.f64().unwrap().get(2).unwrap(), 4500.0);
        assert_eq!(obv.f64().unwrap().get(3).unwrap(), 2700.0);
        assert_eq!(obv.f64().unwrap().get(4).unwrap(), 4900.0);
        assert_eq!(obv.f64().unwrap().get(5).unwrap(), 7400.0);
        assert_eq!(obv.f64().unwrap().get(6).unwrap(), 10400.0);
    }
    
    #[test]
    fn test_calculate_obv_equal_prices() {
        // Test case where prices are equal (no change in OBV)
        let close = Series::new("close".into(), &[10.0, 10.0, 10.0, 10.0]);
        let volume = Series::new("volume".into(), &[1000.0, 1500.0, 2000.0, 2500.0]);
        let df = DataFrame::new(vec![close.into(), volume.into()]).unwrap();
        
        let obv = calculate_obv(&df).unwrap();
        
        // First value should be equal to the first volume
        assert_eq!(obv.f64().unwrap().get(0).unwrap(), 1000.0);
        
        // Since all prices are equal, subsequent OBV values should remain the same
        for i in 1..df.height() {
            assert_eq!(obv.f64().unwrap().get(i).unwrap(), 1000.0);
        }
    }
    
    #[test]
    fn test_calculate_obv_edge_cases() {
        // Test with empty volume values
        let close = Series::new("close".into(), &[10.0, 12.0, 11.0, 13.0]);
        let volume = Series::new("volume".into(), &[0.0, 0.0, 0.0, 0.0]);
        let df = DataFrame::new(vec![close.into(), volume.into()]).unwrap();
        
        let obv = calculate_obv(&df).unwrap();
        
        // All OBV values should be zero
        for i in 0..df.height() {
            assert_eq!(obv.f64().unwrap().get(i).unwrap(), 0.0);
        }
    }
    
    #[test]
    #[should_panic(expected = "requires both close and volume columns")]
    fn test_calculate_obv_missing_columns() {
        // Test missing required columns
        let dummy = Series::new("dummy".into(), &[10.0, 12.0, 11.0, 13.0]);
        let df = DataFrame::new(vec![dummy.into()]).unwrap();
        
        // This should panic as we're missing close and volume columns
        let _ = calculate_obv(&df).unwrap();
    }
    
    #[test]
    #[should_panic(expected = "not yet implemented")]
    fn test_calculate_cmf() {
        // Test that CMF function properly panics with "not yet implemented"
        let df = create_test_ohlcv_df();
        let _ = calculate_cmf(&df,
         14).unwrap();
    }
} 