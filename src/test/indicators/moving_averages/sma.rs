use polars::prelude::*;
use crate::indicators::moving_averages::sma::calculate_sma;
use crate::test::indicators::moving_averages::test_utils::create_test_df;

#[test]
fn test_sma_basic() {
    let df = create_test_df();
    let window = 3;
    let result = calculate_sma(&df, "price", window).unwrap();
    let result_ca = result.f64().unwrap();
    // Check first two values are NaN
    for i in 0..(window-1) {
        assert!(result_ca.get(i).unwrap().is_nan());
    }
    // Check SMA values
    assert!((result_ca.get(2).unwrap() - 11.0).abs() < 1e-10);
    assert!((result_ca.get(3).unwrap() - 12.0).abs() < 1e-10);
}

#[test]
fn test_sma_window_one() {
    let df = create_test_df();
    let result = calculate_sma(&df, "price", 1).unwrap();
    let result_ca = result.f64().unwrap();
    for i in 0..df.height() {
        let price_val = df.column("price").unwrap().f64().unwrap().get(i).unwrap();
        assert!((result_ca.get(i).unwrap() - price_val).abs() < 1e-10);
    }
}

#[test]
fn test_sma_window_too_large() {
    let df = create_test_df();
    let window = df.height() + 1;
    let result = calculate_sma(&df, "price", window);
    assert!(result.is_err());
}

#[test]
fn test_sma_empty_input() {
    let df = DataFrame::new(vec![Series::new("price", Vec::<f64>::new())]).unwrap();
    let result = calculate_sma(&df, "price", 3);
    assert!(result.is_err());
}

#[test]
fn test_sma_with_nans() {
    let price_data = Series::new("price", &[10.0, f64::NAN, 12.0, 13.0, 14.0]);
    let df = DataFrame::new(vec![price_data.into()]).unwrap();
    let result = calculate_sma(&df, "price", 3).unwrap();
    let result_ca = result.f64().unwrap();
    // Should handle NaNs gracefully
    assert!(result_ca.get(1).unwrap().is_nan());
} 