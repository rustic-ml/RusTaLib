use polars::prelude::*;
use ta_lib_in_rust::trade::stock::calculate_pairs_zscore;

#[test]
fn test_pairs_zscore() {
    let s1 = df!["close" => &[10.0, 12.0, 14.0, 16.0, 18.0]].unwrap();
    let s2 = df!["close" => &[9.0, 11.0, 13.0, 15.0, 17.0]].unwrap();
    let zscore = calculate_pairs_zscore(&s1, "close", &s2, "close", 3).unwrap();
    assert_eq!(zscore.len(), 5);
    // The first two should be NaN, the rest should be finite
    assert!(zscore.f64().unwrap().get(0).unwrap().is_nan());
    assert!(zscore.f64().unwrap().get(1).unwrap().is_nan());
    for i in 2..5 {
        assert!(zscore.f64().unwrap().get(i).unwrap().is_finite());
    }
} 