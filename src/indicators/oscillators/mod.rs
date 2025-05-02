// Oscillators module

pub mod rsi;
pub mod macd;

// Re-export indicators
pub use rsi::*;
pub use macd::*;

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    // Helper function to create test DataFrame
    pub fn create_test_price_df() -> DataFrame {
        let price = Series::new("close".into(), &[10.0, 11.0, 10.5, 10.0, 10.5, 11.5, 12.0, 12.5, 12.0, 11.0, 10.0, 9.5, 9.0, 9.5, 10.0]);
        DataFrame::new(vec![price.into()]).unwrap()
    }
} 