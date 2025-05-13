use polars::prelude::*;
use rustalib::indicators::oscillators::calculate_macd;

fn main() -> Result<(), PolarsError> {
    // Create a simple price series with some movement
    let close_prices = Series::new(
        "close".into(),
        &[
            100.0, 102.0, 104.0, 103.0, 105.0, 107.0, 108.0, 109.0, 110.0, 112.0, 111.0, 113.0,
            114.0, 116.0, 115.0, 114.0, 113.0, 112.0, 111.0, 110.0, 109.0, 111.0, 113.0, 115.0,
            117.0, 118.0, 120.0, 122.0, 121.0, 123.0,
        ],
    );

    // Create DataFrame with price data
    let df = DataFrame::new(vec![close_prices.clone().into()])?;

    // Calculate MACD with standard parameters
    // fast_period = 12, slow_period = 26, signal_period = 9
    let (macd, signal) = calculate_macd(&df, 12, 26, 9, "close")?;

    // Print the MACD and signal values
    println!("MACD values:");
    println!("{}", macd);

    println!("\nMACD Signal line:");
    println!("{}", signal);

    // Show how to interpret MACD values
    println!("\nBasic MACD interpretation:");
    println!("--------------------------");
    println!("1. MACD crossing above signal line: Bullish signal");
    println!("2. MACD crossing below signal line: Bearish signal");
    println!("3. MACD above zero line: Bullish trend");
    println!("4. MACD below zero line: Bearish trend");
    println!("5. Divergence between MACD and price: Potential reversal");

    Ok(())
}
