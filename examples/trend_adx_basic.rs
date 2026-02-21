use polars::prelude::*;
use rustalib::indicators::trend::calculate_adx;

fn main() -> PolarsResult<()> {
    // Simple mock OHLC data (30 bars)
    let high = Series::new(
        "high".into(),
        &[
            30.0, 31.0, 32.0, 31.5, 32.5, 33.0, 34.0, 35.0, 34.5, 35.5, 36.0, 37.0, 38.0, 37.5,
            38.5, 39.0, 40.0, 41.0, 41.5, 42.0, 43.0, 42.5, 43.5, 44.0, 45.0, 45.5, 46.0, 46.5,
            47.0, 48.0,
        ],
    );
    let low = Series::new(
        "low".into(),
        &[
            29.0, 30.0, 30.5, 31.0, 31.5, 32.0, 33.0, 33.5, 34.0, 34.5, 35.0, 36.0, 37.0, 36.5,
            37.5, 38.0, 39.0, 40.0, 40.5, 41.0, 42.0, 41.5, 42.5, 43.0, 44.0, 44.5, 45.0, 45.5,
            46.0, 47.0,
        ],
    );
    let close = Series::new(
        "close".into(),
        &[
            29.5, 30.5, 31.5, 31.2, 32.0, 32.8, 33.5, 34.5, 34.2, 35.0, 35.5, 36.5, 37.5, 37.0,
            38.0, 38.8, 39.5, 40.5, 41.0, 41.5, 42.5, 42.0, 43.0, 43.5, 44.5, 45.0, 45.5, 46.0,
            46.5, 47.5,
        ],
    );

    let df = DataFrame::new(high.len(), vec![high.into(), low.into(), close.into()])?;

    // Calculate 14-period ADX
    let adx = calculate_adx(&df, 14)?;

    println!("14-period ADX values:\n{}", adx);
    println!("\nBasic interpretation:\n- ADX < 20 ⇒ weak trend\n- ADX > 25 ⇒ strong trend");

    Ok(())
}
