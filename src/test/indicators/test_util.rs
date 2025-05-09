use polars::prelude::*;

#[test]
fn test_create_test_ohlcv_df() {
    let df = crate::indicators::test_util::create_test_ohlcv_df();

    // Validate the DataFrame shape
    assert_eq!(df.height(), 100);
    assert_eq!(df.width(), 5);

    // Validate column types
    assert!(df.column("open").unwrap().dtype().is_float());
    assert!(df.column("high").unwrap().dtype().is_float());
    assert!(df.column("low").unwrap().dtype().is_float());
    assert!(df.column("close").unwrap().dtype().is_float());
    assert!(df.column("volume").unwrap().dtype().is_float());

    // Check high > low for all rows
    let high = df.column("high").unwrap().f64().unwrap();
    let low = df.column("low").unwrap().f64().unwrap();

    for i in 0..df.height() {
        assert!(high.get(i).unwrap() > low.get(i).unwrap());
    }
} 