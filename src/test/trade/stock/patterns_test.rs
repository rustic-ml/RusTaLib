use polars::prelude::*;
use ta_lib_in_rust::trade::stock::recognize_candlestick_patterns;

#[test]
fn test_candlestick_patterns() {
    let df = df![
        "open" => &[10.0, 11.0, 12.0, 13.0, 14.0],
        "high" => &[11.0, 12.0, 13.0, 14.0, 15.0],
        "low" => &[9.0, 10.0, 11.0, 12.0, 13.0],
        "close" => &[11.0, 10.5, 12.0, 13.5, 13.0],
    ].unwrap();
    let patterns = recognize_candlestick_patterns(&df, "open", "high", "low", "close").unwrap();
    assert_eq!(patterns.len(), 5);
    // Should contain only known pattern labels
    let valid = ["none", "bullish_engulfing", "bearish_engulfing", "doji", "hammer", "shooting_star"];
    for i in 0..patterns.len() {
        let label = patterns.str_value(i).unwrap();
        assert!(valid.contains(&label));
    }
} 