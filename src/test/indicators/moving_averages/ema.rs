// Tests for Exponential Moving Average (EMA)
use polars::prelude::*;
use crate::indicators::moving_averages::{calculate_ema};
use crate::test::indicators::moving_averages::test_utils::create_test_df;

#[test]
fn test_calculate_ema_basic() {
    let df = create_test_df();
    let window = 3;

    let result = calculate_ema(&df, "price", window).unwrap();
    let result_ca = result.f64().unwrap();

    // First two values should be null or NaN
    for i in 0..(window - 1) {
        let val = result_ca.get(i);
        assert!(val.is_none() || val.map_or(false, |v| v.is_nan()));
    }

    // Check that some value exists for remaining positions (and is not NaN)
    for i in window - 1..df.height() {
        let val = result_ca.get(i);
        assert!(val.is_some());
        assert!(!val.unwrap().is_nan());
    }

    // Manual calculation for EMA
    // Initial SMA = (10 + 11 + 12) / 3 = 11.0
    // alpha = 2/(3+1) = 0.5
    // EMA[3] = 0.5 * 13 + (1-0.5) * 11 = 6.5 + 5.5 = 12.0
    // EMA[4] = 0.5 * 14 + (1-0.5) * 12 = 7 + 6 = 13.0
    assert!((result_ca.get(3).unwrap() - 12.0).abs() < 1e-10);
    assert!((result_ca.get(4).unwrap() - 13.0).abs() < 1e-10);
}

#[test]
fn test_ema_window_edge_cases() {
    let df = create_test_df();

    // Test with window size 1 (should return the same series)
    let result = calculate_ema(&df, "price", 1).unwrap();
    let result_ca = result.f64().unwrap();

    for i in 0..df.height() {
        let price_val = df.column("price").unwrap().f64().unwrap().get(i).unwrap();
        assert!((result_ca.get(i).unwrap() - price_val).abs() < 1e-10);
    }

    // Test with window size equal to dataframe length
    let window = df.height();
    let result = calculate_ema(&df, "price", window).unwrap();
    let result_ca = result.f64().unwrap();

    // Only the last value should be non-NaN
    for i in 0..(window - 1) {
        let val = result_ca.get(i);
        assert!(val.is_none() || val.map_or(false, |v| v.is_nan()));
    }

    // Last value should not be NaN
    assert!(!result_ca.get(window - 1).unwrap().is_nan());
}

#[test]
fn test_ema_responsiveness() {
    // Test EMA responsiveness to price changes compared to SMA
    let price_data = Series::new("price".into(), &[10.0, 10.0, 10.0, 10.0, 10.0, 20.0, 20.0]);
    let df = DataFrame::new(vec![price_data.into()]).unwrap();

    let window = 3;
    let result = calculate_ema(&df, "price", window).unwrap();
    let result_ca = result.f64().unwrap();

    // After a sudden price jump from 10 to 20, EMA should respond faster than SMA
    // For index 5 (first value after the jump):
    // SMA would be (10.0 + 10.0 + 20.0)/3 = 13.33
    // EMA with alpha=0.5 would be 0.5*20.0 + 0.5*10.0 = 15.0
    assert!(result_ca.get(5).unwrap() > 13.33);
    assert!((result_ca.get(5).unwrap() - 15.0).abs() < 1e-10);
}

#[test]
#[should_panic(expected = "Not enough data points")]
fn test_insufficient_data() {
    let price_data = Series::new("price".into(), &[10.0, 11.0]);
    let df = DataFrame::new(vec![price_data.into()]).unwrap();
    let window = 3;

    // This should panic with "Not enough data points"
    let _ = calculate_ema(&df, "price", window).unwrap();
} 