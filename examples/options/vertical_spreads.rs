// Example: Vertical Spreads Strategy
// This example demonstrates how to analyze different vertical spread strategies

use polars::prelude::*;
// We need to use spreads from src/trade/options directory
// First, let's simulate the data we would need for spread analysis

fn main() -> Result<(), PolarsError> {
    println!("Vertical Spreads Analysis Example");
    println!("=================================\n");

    // Create example data for both call and put vertical spreads
    let short_strike = Series::new(
        "short_strike".into(),
        &[210.0, 190.0, 210.0, 190.0] // Different configurations
    );
    
    let long_strike = Series::new(
        "long_strike".into(),
        &[200.0, 200.0, 220.0, 180.0] // Different configurations
    );
    
    let short_price = Series::new(
        "short_price".into(),
        &[5.0, 15.0, 10.0, 3.0] // Example option prices
    );
    
    let long_price = Series::new(
        "long_price".into(),
        &[2.0, 10.0, 16.0, 1.0] // Example option prices
    );
    
    let is_call = Series::new(
        "is_call".into(),
        &[true, true, false, false] // Both call and put spreads
    );
    
    // Create spread type and description columns for better readability
    // Using StringChunked to create string Series
    let spread_type_vec = vec![
        "Bear Call".to_string(), 
        "Bull Call".to_string(), 
        "Bear Put".to_string(), 
        "Bull Put".to_string()
    ];
    let spread_type = Series::new("spread_type".into(), spread_type_vec);
    
    let description_vec = vec![
        "Short Call Vertical (Sell high strike, buy low strike)".to_string(),
        "Long Call Vertical (Buy high strike, sell low strike)".to_string(),
        "Long Put Vertical (Buy high strike, sell low strike)".to_string(),
        "Short Put Vertical (Sell high strike, buy low strike)".to_string()
    ];
    let description = Series::new("description".into(), description_vec);

    // Create a DataFrame with our spread data
    let mut df = DataFrame::new(vec![
        short_strike.into(),
        long_strike.into(),
        short_price.into(),
        long_price.into(),
        is_call.into(),
        spread_type.clone().into(),  // Clone here to avoid the move
        description.into(),
    ])?;

    // Calculate metrics manually to demonstrate how vertical spreads work
    // (since the trade module is not exported in lib.rs yet)
    let mut max_profit = Vec::with_capacity(4);
    let mut max_loss = Vec::with_capacity(4);
    let mut breakeven = Vec::with_capacity(4);
    let mut risk_reward = Vec::with_capacity(4);
    let mut strike_width = Vec::with_capacity(4);
    
    for i in 0..4 {
        let ss = df.column("short_strike")?.f64()?.get(i).unwrap();
        let ls = df.column("long_strike")?.f64()?.get(i).unwrap();
        let sp = df.column("short_price")?.f64()?.get(i).unwrap();
        let lp = df.column("long_price")?.f64()?.get(i).unwrap();
        let call = df.column("is_call")?.bool()?.get(i).unwrap();
        
        // Calculate width between strikes
        strike_width.push((ss - ls).abs());
        
        // Calculate net premium
        let net_premium = sp - lp;
        
        // Calculate metrics based on call or put vertical and configuration
        match (call, spread_type.str()?.get(i).unwrap()) {
            (true, "Bear Call") => {
                // Bear Call Spread (Short Call Vertical)
                max_profit.push(net_premium);
                max_loss.push(strike_width[i] - net_premium);
                breakeven.push(ss - net_premium);
            },
            (true, "Bull Call") => {
                // Bull Call Spread (Long Call Vertical)
                max_profit.push(strike_width[i] - net_premium);
                max_loss.push(net_premium);
                breakeven.push(ls + net_premium);
            },
            (false, "Bear Put") => {
                // Bear Put Spread (Long Put Vertical)
                max_profit.push(strike_width[i] - net_premium);
                max_loss.push(net_premium);
                breakeven.push(ss - net_premium);
            },
            (false, "Bull Put") => {
                // Bull Put Spread (Short Put Vertical)
                max_profit.push(net_premium);
                max_loss.push(strike_width[i] - net_premium);
                breakeven.push(ls + net_premium);
            },
            _ => unreachable!()
        }
        
        // Calculate risk/reward ratio
        risk_reward.push(if max_loss[i] > 0.0 {
            max_profit[i] / max_loss[i]
        } else {
            f64::NAN
        });
    }
    
    // Add calculated metrics to the dataframe
    df.with_column(Series::new("max_profit".into(), max_profit))?;
    df.with_column(Series::new("max_loss".into(), max_loss))?;
    df.with_column(Series::new("breakeven".into(), breakeven))?;
    df.with_column(Series::new("risk_reward".into(), risk_reward))?;
    df.with_column(Series::new("strike_width".into(), strike_width))?;

    // Display the results
    println!("Vertical Spread Metrics:");
    println!("{}", df);

    // Show educational explanations
    print_vertical_spread_education();

    // Suggestion for library improvement:
    println!("\nNote: To use the built-in calculate_vertical_spread_metrics function,");
    println!("the trade module should be added to lib.rs with:");
    println!("pub mod trade;");

    Ok(())
}

fn print_vertical_spread_education() {
    println!("\nVertical Spread Education");
    println!("========================\n");
    
    println!("Bear Call Spread (Credit Call Spread):");
    println!("- Sell a lower strike call, buy a higher strike call");
    println!("- Max profit: Net credit received");
    println!("- Max loss: Strike width minus net credit");
    println!("- Market outlook: Bearish or neutral\n");
    
    println!("Bull Call Spread (Debit Call Spread):");
    println!("- Buy a lower strike call, sell a higher strike call");
    println!("- Max profit: Strike width minus net debit");
    println!("- Max loss: Net debit paid");
    println!("- Market outlook: Bullish\n");
    
    println!("Bear Put Spread (Debit Put Spread):");
    println!("- Buy a higher strike put, sell a lower strike put");
    println!("- Max profit: Strike width minus net debit");
    println!("- Max loss: Net debit paid");
    println!("- Market outlook: Bearish\n");
    
    println!("Bull Put Spread (Credit Put Spread):");
    println!("- Sell a higher strike put, buy a lower strike put");
    println!("- Max profit: Net credit received");
    println!("- Max loss: Strike width minus net credit");
    println!("- Market outlook: Bullish or neutral\n");
    
    println!("Risk Management Tips:");
    println!("- Size positions according to max loss, not max profit");
    println!("- Consider early management at 50-75% of max profit");
    println!("- Watch for changes in implied volatility");
    println!("- Be aware of upcoming events like earnings or dividends");
} 