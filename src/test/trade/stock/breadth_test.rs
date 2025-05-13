use polars::prelude::*;
use ta_lib_in_rust::trade::stock::{calculate_advance_decline_line, calculate_trin, calculate_mcclellan_oscillator};

#[test]
fn test_advance_decline_line() {
    let df = df![
        "advance" => &[1, 0, 1, 1, 0],
        "decline" => &[0, 1, 0, 0, 1],
    ].unwrap();
    let ad_line = calculate_advance_decline_line(&df, "advance", "decline").unwrap();
    let expected = vec![1, 0, 1, 2, 1];
    assert_eq!(ad_line.i32().unwrap().to_vec(), expected);
}

#[test]
fn test_trin() {
    let df = df![
        "advancing_volume" => &[100.0, 200.0, 150.0],
        "declining_volume" => &[80.0, 120.0, 100.0],
        "advance" => &[10.0, 12.0, 11.0],
        "decline" => &[8.0, 9.0, 10.0],
    ].unwrap();
    let trin = calculate_trin(&df, "advancing_volume", "declining_volume", "advance", "decline").unwrap();
    assert!(trin.f64().unwrap().get(2).is_some());
}

#[test]
fn test_mcclellan_oscillator() {
    let df = df![
        "advance" => &[10.0, 12.0, 11.0, 13.0, 15.0],
        "decline" => &[8.0, 9.0, 10.0, 9.0, 8.0],
    ].unwrap();
    let mcclellan = calculate_mcclellan_oscillator(&df, "advance", "decline", 2, 3).unwrap();
    assert_eq!(mcclellan.len(), 5);
} 