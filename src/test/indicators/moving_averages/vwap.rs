use polars::prelude::*;
use crate::indicators::moving_averages::vwap::calculate_vwap;

fn create_vwap_test_df() -> DataFrame {
    let price = Series::new("price", &[10.0, 11.0, 12.0, 13.0, 14.0]);
    let volume = Series::new("volume", &[100.0, 200.0, 150.0, 120.0, 130.0]);
    DataFrame::new(vec![price, volume]).unwrap()
}

#[test]
fn test_vwap_basic() {
    let df = create_vwap_test_df();
    let result = calculate_vwap(&df, "price", "volume").unwrap();
    let result_ca = result.f64().unwrap();
    // Manual calculation for first value
    assert!((result_ca.get(0).unwrap() - 10.0).abs() < 1e-10);
    // Check monotonicity
    for i in 1..df.height() {
        assert!(result_ca.get(i).unwrap() >= result_ca.get(i-1).unwrap());
    }
}

#[test]
fn test_vwap_empty_input() {
    let df = DataFrame::new(vec![Series::new("price", Vec::<f64>::new()), Series::new("volume", Vec::<f64>::new())]).unwrap();
    let result = calculate_vwap(&df, "price", "volume");
    assert!(result.is_err());
}

#[test]
fn test_vwap_with_nans() {
    let price = Series::new("price", &[10.0, f64::NAN, 12.0, 13.0, 14.0]);
    let volume = Series::new("volume", &[100.0, 200.0, 150.0, 120.0, 130.0]);
    let df = DataFrame::new(vec![price, volume]).unwrap();
    let result = calculate_vwap(&df, "price", "volume").unwrap();
    let result_ca = result.f64().unwrap();
    // Should handle NaNs gracefully
    assert!(result_ca.get(1).unwrap().is_nan());
}

#[test]
fn test_vwap_single_row() {
    let price = Series::new("price", &[42.0]);
    let volume = Series::new("volume", &[100.0]);
    let df = DataFrame::new(vec![price, volume]).unwrap();
    let result = calculate_vwap(&df, "price", "volume").unwrap();
    let result_ca = result.f64().unwrap();
    assert!((result_ca.get(0).unwrap() - 42.0).abs() < 1e-10);
} 