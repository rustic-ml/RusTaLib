use polars::prelude::*;
use ta_lib_in_rust::trade::stock::calculate_relative_strength;

#[test]
fn test_relative_strength() {
    let stock_df = df!["close" => &[10.0, 12.0, 14.0, 16.0]].unwrap();
    let bench_df = df!["close" => &[5.0, 6.0, 7.0, 8.0]].unwrap();
    let rs = calculate_relative_strength(&stock_df, "close", &bench_df, "close").unwrap();
    let expected = vec![2.0, 2.0, 2.0, 2.0];
    for (i, val) in expected.iter().enumerate() {
        assert!((rs.f64().unwrap().get(i).unwrap() - val).abs() < 1e-6);
    }
} 