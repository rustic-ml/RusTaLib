// Moving Averages module

pub mod sma;
pub mod ema;
pub mod wma;

// Re-export indicators
pub use sma::*;
pub use ema::*;
pub use wma::*;

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    // Helper function to create test DataFrame
    pub fn create_test_df() -> DataFrame {
        let price_data = Series::new("price".into(), &[10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0]);
        DataFrame::new(vec![price_data.into()]).unwrap()
    }
} 