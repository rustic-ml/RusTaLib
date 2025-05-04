use polars::prelude::*;
use ta_lib_in_rust::indicators::oscillators::calculate_rsi;

fn main() -> Result<(), PolarsError> {
    // Create a simple price series with some movement
    let close_prices = Series::new(
        "close".into(),
        &[
            100.0, 102.0, 104.0, 103.0, 105.0, 107.0, 108.0, 107.0, 105.0, 103.0, 101.0, 99.0,
            97.0, 95.0, 94.0,
        ],
    );

    // Create DataFrame with date and price data
    let df = DataFrame::new(vec![close_prices.clone().into()])?;

    // Calculate RSI with 5-period setting for this short example
    let rsi = calculate_rsi(&df, 5, "close")?;

    // Print the RSI values
    println!("RSI values:");
    println!("{}", rsi);

    // Show how to interpret RSI values
    println!("\nBasic RSI interpretation:");
    println!("-------------------------");
    println!("Values above 70: Overbought condition");
    println!("Values below 30: Oversold condition");
    println!("Values around 50: Neutral market");

    Ok(())
}
