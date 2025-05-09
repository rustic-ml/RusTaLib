use polars::prelude::*;
use crate::util::dataframe_utils::check_window_size;

#[test]
fn test_check_window_size_valid() {
    let s = Series::new("a", &[1, 2, 3, 4, 5]);
    let df = DataFrame::new(vec![s]).unwrap();
    assert!(check_window_size(&df, 3, "test").is_ok());
}

#[test]
fn test_check_window_size_equal() {
    let s = Series::new("a", &[1, 2, 3]);
    let df = DataFrame::new(vec![s]).unwrap();
    assert!(check_window_size(&df, 3, "test").is_ok());
}

#[test]
fn test_check_window_size_too_large() {
    let s = Series::new("a", &[1, 2]);
    let df = DataFrame::new(vec![s]).unwrap();
    let result = check_window_size(&df, 3, "test");
    assert!(result.is_err());
}

#[test]
fn test_check_window_size_empty() {
    let s = Series::new("a", Vec::<i32>::new());
    let df = DataFrame::new(vec![s]).unwrap();
    let result = check_window_size(&df, 1, "test");
    assert!(result.is_err());
} 