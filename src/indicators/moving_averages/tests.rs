use polars::prelude::*;

// Helper function to create test DataFrame
pub fn create_test_df() -> DataFrame {
    let price = Series::new("price".into(), &[10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0]);
    DataFrame::new(vec![price.into()]).unwrap()
} 