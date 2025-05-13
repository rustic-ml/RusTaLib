use polars::prelude::*;
use ta_lib_in_rust::trade::stock::calculate_vwap_bands;

#[test]
fn test_vwap_bands() {
    let df = df![
        "close" => &[10.0, 11.0, 12.0, 13.0, 14.0],
        "volume" => &[100.0, 110.0, 120.0, 130.0, 140.0],
    ].unwrap();
    let (vwap, upper, lower) = calculate_vwap_bands(&df, "close", "volume", 3, 2.0).unwrap();
    assert_eq!(vwap.len(), 5);
    assert_eq!(upper.len(), 5);
    assert_eq!(lower.len(), 5);
    // Check that upper > vwap > lower for non-NaN values
    for i in 2..5 {
        let v = vwap.f64().unwrap().get(i).unwrap();
        let u = upper.f64().unwrap().get(i).unwrap();
        let l = lower.f64().unwrap().get(i).unwrap();
        assert!(u > v && v > l);
    }
} 