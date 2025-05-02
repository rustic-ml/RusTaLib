use polars::prelude::*;
use technical_indicators::indicators::volatility::calculate_bollinger_bands;

fn main() -> Result<(), PolarsError> {
    // Create a simple price series with some movement
    let close_prices = Series::new(
        "close".into(),
        &[
            100.0, 101.0, 102.0, 103.0, 105.0, 104.0, 106.0, 107.0, 109.0, 108.0,
            107.0, 109.0, 111.0, 114.0, 113.0, 116.0, 119.0, 120.0, 119.0, 117.0,
            118.0, 120.0, 123.0, 122.0, 120.0, 118.0, 119.0, 121.0, 124.0, 125.0,
        ],
    );
    
    // Create DataFrame with price data
    let df = DataFrame::new(vec![close_prices.clone().into()])?;
    
    // Calculate Bollinger Bands with standard parameters (20-period, 2 standard deviations)
    let (middle, upper, lower) = calculate_bollinger_bands(&df, 20, 2.0, "close")?;
    
    // Print the Bollinger Bands values
    println!("Middle Band (SMA):");
    println!("{}", middle);
    
    println!("\nUpper Band:");
    println!("{}", upper);
    
    println!("\nLower Band:");
    println!("{}", lower);
    
    // Show how to interpret Bollinger Bands
    println!("\nBasic Bollinger Bands interpretation:");
    println!("-----------------------------------");
    println!("1. Price touching upper band: Potential overbought condition");
    println!("2. Price touching lower band: Potential oversold condition");
    println!("3. Price breaking outside bands: Strong trend or possible reversal");
    println!("4. Bands narrowing (squeezing): Low volatility, often precedes significant price movement");
    println!("5. Bands widening: Increasing volatility, often during strong trends");
    println!("6. Price reverting to middle band: Typical price behavior in ranging markets");
    
    Ok(())
} 