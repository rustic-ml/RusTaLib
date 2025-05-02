use polars::prelude::*;

// Helper function to create test DataFrame with OHLC data
pub fn create_test_ohlc_df() -> DataFrame {
    let open = Series::new("open".into(), &[10.0, 11.0, 12.0, 11.5, 12.5, 13.0, 14.0]);
    let high = Series::new("high".into(), &[12.0, 13.0, 13.5, 12.5, 13.5, 15.0, 15.5]);
    let low = Series::new("low".into(), &[9.5, 10.5, 11.0, 10.5, 11.5, 12.5, 13.5]);
    let close = Series::new("close".into(), &[11.0, 12.0, 13.0, 11.0, 13.0, 14.0, 14.5]);
    
    DataFrame::new(vec![open.into(), high.into(), low.into(), close.into()]).unwrap()
}

// Helper function to create test DataFrame with only price data
pub fn create_test_price_df() -> DataFrame {
    let price = Series::new("close".into(), &[10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0]);
    DataFrame::new(vec![price.into()]).unwrap()
} 