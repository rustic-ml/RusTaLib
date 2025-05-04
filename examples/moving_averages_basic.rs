use polars::prelude::*;
use ta_lib_in_rust::indicators::moving_averages::{calculate_ema, calculate_sma, calculate_wma};

fn main() -> Result<(), PolarsError> {
    // Create a simple price series with trend followed by sideways movement
    let close_prices = Series::new(
        "close".into(),
        &[
            100.0, 101.0, 102.0, 103.0, 105.0, 107.0, 110.0, 113.0, 115.0, 116.0, 115.0, 116.0,
            115.0, 116.0, 115.0, 114.0, 115.0, 116.0, 117.0, 119.0, 121.0, 123.0, 125.0, 127.0,
            126.0,
        ],
    );

    // Create DataFrame with price data
    let df = DataFrame::new(vec![close_prices.clone().into()])?;

    // Calculate different moving averages
    let sma_10 = calculate_sma(&df, "close", 10)?;
    let ema_10 = calculate_ema(&df, "close", 10)?;
    let wma_10 = calculate_wma(&df, "close", 10)?;

    // Print the moving average values
    println!("Simple Moving Average (SMA, 10-period):");
    println!("{}", sma_10);

    println!("\nExponential Moving Average (EMA, 10-period):");
    println!("{}", ema_10);

    println!("\nWeighted Moving Average (WMA, 10-period):");
    println!("{}", wma_10);

    // Show how to interpret moving averages
    println!("\nBasic Moving Averages interpretation:");
    println!("----------------------------------");
    println!("1. Price crossing above MA: Bullish signal");
    println!("2. Price crossing below MA: Bearish signal");
    println!("3. Fast MA crossing above slow MA: Bullish crossover (golden cross)");
    println!("4. Fast MA crossing below slow MA: Bearish crossover (death cross)");
    println!("5. MA slope: Indicates trend direction (up = bullish, down = bearish)");
    println!("6. MA differences:");
    println!("   - SMA: Simple, equal weighting to all prices in the period");
    println!("   - EMA: More weight to recent prices, reacts faster to price changes");
    println!("   - WMA: Linearly weighted, moderate responsiveness");

    Ok(())
}
