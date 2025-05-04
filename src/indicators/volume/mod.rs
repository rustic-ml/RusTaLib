// Volume indicators module

pub mod cmf;
pub mod obv;

// Re-export indicators
pub use cmf::*;
pub use obv::*;

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    // Helper function to create test DataFrame with OHLCV data
    pub fn create_test_ohlcv_df() -> DataFrame {
        let open = Series::new("open".into(), &[10.0, 11.0, 12.0, 11.5, 12.5, 13.0, 14.0]);
        let high = Series::new("high".into(), &[12.0, 13.0, 13.5, 12.5, 13.5, 15.0, 15.5]);
        let low = Series::new("low".into(), &[9.5, 10.5, 11.0, 10.5, 11.5, 12.5, 13.5]);
        let close = Series::new("close".into(), &[11.0, 12.0, 13.0, 11.0, 13.0, 14.0, 14.5]);
        let volume = Series::new(
            "volume".into(),
            &[1000.0, 1500.0, 2000.0, 1800.0, 2200.0, 2500.0, 3000.0],
        );

        DataFrame::new(vec![
            open.into(),
            high.into(),
            low.into(),
            close.into(),
            volume.into(),
        ])
        .unwrap()
    }
}
