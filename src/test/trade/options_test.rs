use polars::prelude::*;
use ta_lib_in_rust::trade::options::{calculate_put_call_oi_ratio, calculate_skew_sentiment, calculate_gamma_exposure};

#[test]
fn test_put_call_oi_ratio() {
    let df = df![
        "open_interest" => &[100.0, 200.0, 150.0, 120.0],
        "is_call" => &[true, false, true, false],
    ].unwrap();
    let ratio = calculate_put_call_oi_ratio(&df, "open_interest", "is_call", 2).unwrap();
    assert_eq!(ratio.len(), 4);
}

#[test]
fn test_skew_sentiment() {
    let df = df![
        "iv_call" => &[0.2, 0.25, 0.22],
        "iv_put" => &[0.25, 0.3, 0.28],
    ].unwrap();
    let skew = calculate_skew_sentiment(&df, "iv_call", "iv_put").unwrap();
    assert_eq!(skew.len(), 3);
}

#[test]
fn test_gamma_exposure() {
    let df = df![
        "gamma" => &[0.01, 0.02, 0.03],
        "open_interest" => &[100.0, 200.0, 150.0],
        "contract_multiplier" => &[100.0, 100.0, 100.0],
    ].unwrap();
    let gex = calculate_gamma_exposure(&df, "gamma", "open_interest", "contract_multiplier").unwrap();
    assert_eq!(gex.len(), 3);
} 