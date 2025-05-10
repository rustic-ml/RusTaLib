use polars::prelude::*;
use crate::indicators::moving_averages::vwap::calculate_vwap;

fn create_test_df() -> DataFrame {
    // Create sample OHLCV data
    let dates = Series::new("date".into(), &[
        "2023-01-01 09:30", "2023-01-01 10:30", "2023-01-01 11:30", "2023-01-01 12:30",
        "2023-01-02 09:30", "2023-01-02 10:30", "2023-01-02 11:30", "2023-01-02 12:30",
    ]);
    let open = Series::new("open".into(), &[100.0, 102.0, 103.0, 101.0, 105.0, 107.0, 106.0, 108.0]);
    let high = Series::new("high".into(), &[104.0, 105.0, 104.0, 102.0, 108.0, 109.0, 108.0, 110.0]);
    let low = Series::new("low".into(), &[99.0, 101.0, 100.0, 99.0, 104.0, 105.0, 105.0, 106.0]);
    let close = Series::new("close".into(), &[102.0, 103.0, 101.0, 100.0, 107.0, 106.0, 108.0, 109.0]);
    let volume = Series::new("volume".into(), &[5000.0, 4000.0, 3000.0, 2000.0, 6000.0, 5500.0, 4800.0, 5200.0]);
    
    DataFrame::new(vec![dates.into(), open.into(), high.into(), low.into(), close.into(), volume.into()]).unwrap()
}

#[test]
fn test_calculate_vwap() {
    let df = create_test_df();
    
    // Test with default full-period VWAP (lookback = 0)
    let vwap = calculate_vwap(&df, 0).unwrap();
    
    // VWAP should have the same length as the dataframe
    assert_eq!(vwap.len(), df.height());
    
    // Test with a 4-period rolling VWAP
    let vwap_rolling = calculate_vwap(&df, 4).unwrap();
    
    // Rolling VWAP should have the same length as the dataframe
    assert_eq!(vwap_rolling.len(), df.height());
    
    // Values should be different
    assert_ne!(
        vwap.f64().unwrap().get(7).unwrap(),
        vwap_rolling.f64().unwrap().get(7).unwrap()
    );
}

#[test]
fn test_vwap_with_different_lookbacks() {
    let df = create_test_df();
    
    // Calculate VWAP with different lookback periods
    let vwap1 = calculate_vwap(&df, 2).unwrap();
    let vwap2 = calculate_vwap(&df, 4).unwrap();
    
    // They should have the same length
    assert_eq!(vwap1.len(), vwap2.len());
    
    // But different values at the end
    assert_ne!(
        vwap1.f64().unwrap().get(7).unwrap(),
        vwap2.f64().unwrap().get(7).unwrap()
    );
}

#[test]
fn test_vwap_with_insufficient_data() {
    // Create a smaller dataframe with just 2 rows
    let dates = Series::new("date".into(), &["2023-01-01 09:30", "2023-01-01 10:30"]);
    let open = Series::new("open".into(), &[100.0, 102.0]);
    let high = Series::new("high".into(), &[104.0, 105.0]);
    let low = Series::new("low".into(), &[99.0, 101.0]);
    let close = Series::new("close".into(), &[102.0, 103.0]);
    let volume = Series::new("volume".into(), &[5000.0, 4000.0]);
    
    let small_df = DataFrame::new(vec![
        dates.into(), open.into(), high.into(), low.into(), close.into(), volume.into()
    ]).unwrap();
    
    // It should still work with a lookback longer than the dataframe
    let result = calculate_vwap(&small_df, 10);
    assert!(result.is_ok());
} 