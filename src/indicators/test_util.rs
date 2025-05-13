use polars::prelude::*;

/// Creates a test OHLCV DataFrame for testing indicator functions
///
/// This function generates a DataFrame with OHLCV data suitable for testing technical indicators.
/// The data follows a simple pattern with some randomness to simulate realistic price movements.
///
/// # Returns
///
/// * `DataFrame` - A DataFrame with columns "open", "high", "low", "close", "volume"
pub fn create_test_ohlcv_df() -> DataFrame {
    // Generate some sample price data
    let base_price = 100.0;
    let mut prices = Vec::new();
    let mut open = Vec::new();
    let mut high = Vec::new();
    let mut low = Vec::new();
    let mut close = Vec::new();
    let mut volume = Vec::new();

    for i in 0..100 {
        // Create a simple sine wave pattern with some noise
        let time_factor = i as f64 * 0.1;
        let wave = (time_factor.sin() * 10.0) + base_price;
        let noise = (i % 7) as f64 * 0.5;

        let price = wave + noise;
        prices.push(price);

        // Generate OHLC based on the price
        let o = price - 1.0 + (i % 5) as f64 * 0.2;
        let c = price + 0.5 - (i % 3) as f64 * 0.3;
        let h = price.max(o).max(c) + 1.0 + (i % 4) as f64 * 0.4;
        let l = price.min(o).min(c) - 1.0 - (i % 6) as f64 * 0.2;

        open.push(o);
        high.push(h);
        low.push(l);
        close.push(c);

        // Generate volume
        let v = 1000.0 + (i % 10) as f64 * 200.0;
        volume.push(v);
    }

    // Create the DataFrame

    DataFrame::new(vec![
        Series::new("open".into(), open).into(),
        Series::new("high".into(), high).into(),
        Series::new("low".into(), low).into(),
        Series::new("close".into(), close).into(),
        Series::new("volume".into(), volume).into(),
    ])
    .unwrap()
}
