use polars::prelude::*;
use ta_lib_in_rust::strategy::options::iron_condor::{run_strategy, StrategyParams, calculate_performance};

fn main() -> Result<(), PolarsError> {
    println!("Options Iron Condor Strategy Example\n");
    
    // Load or generate sample price data
    let price_df = generate_sample_price_data()?;
    
    // Generate sample options data
    let options_df = generate_sample_options_data(&price_df)?;
    
    // Configure iron condor strategy parameters
    let params = StrategyParams {
        days_to_expiration: 45,
        short_call_delta: 0.16,
        short_put_delta: 0.16,
        call_spread_width: 5.0,
        put_spread_width: 5.0,
        max_risk_pct: 5.0,
        profit_target_pct: 50.0,
        stop_loss_pct: 200.0,
        days_to_close_before_expiry: 21,
    };
    
    // Run the strategy
    println!("Running iron condor strategy with parameters:");
    println!("  Days to Expiration: {}", params.days_to_expiration);
    println!("  Short Call Delta: {}", params.short_call_delta);
    println!("  Short Put Delta: {}", params.short_put_delta);
    println!("  Call Spread Width: ${:.2}", params.call_spread_width);
    println!("  Put Spread Width: ${:.2}", params.put_spread_width);
    println!("  Profit Target: {}% of max credit", params.profit_target_pct);
    println!("  Stop Loss: {}% of max credit", params.stop_loss_pct);
    println!("");
    
    let signals = run_strategy(&price_df, &options_df, &params)?;
    
    // Calculate performance metrics
    let initial_capital = 10000.0;
    
    let (final_capital, total_return, num_trades, win_rate, max_drawdown, profit_factor) = 
        calculate_performance(&signals.trade_details, initial_capital);
    
    // Display strategy performance
    println!("\nStrategy Performance:");
    println!("  Initial Capital: ${:.2}", initial_capital);
    println!("  Final Capital: ${:.2}", final_capital);
    println!("  Total Return: {:.2}%", total_return);
    println!("  Number of Trades: {}", num_trades);
    println!("  Win Rate: {:.2}%", win_rate);
    println!("  Maximum Drawdown: {:.2}%", max_drawdown);
    println!("  Profit Factor: {:.2}", profit_factor);
    
    // Display sample trade
    println!("\nSample Iron Condor Trade:");
    if signals.trade_details.is_empty() {
        println!("  No trades were generated");
    } else {
        let sample_trade = &signals.trade_details[0];
        println!("  Entry Date: {}", sample_trade.entry_date);
        println!("  Exit Date: {}", sample_trade.exit_date);
        println!("  Short Call Strike: ${:.2}", sample_trade.short_call_strike);
        println!("  Long Call Strike: ${:.2}", sample_trade.long_call_strike);
        println!("  Short Put Strike: ${:.2}", sample_trade.short_put_strike);
        println!("  Long Put Strike: ${:.2}", sample_trade.long_put_strike);
        println!("  Net Credit: ${:.2}", sample_trade.net_credit);
        println!("  Max Profit: ${:.2}", sample_trade.max_profit);
        println!("  Max Loss: ${:.2}", sample_trade.max_loss);
        println!("  P&L: ${:.2}", sample_trade.pnl);
        println!("  Exit Reason: {}", sample_trade.exit_reason);
    }
    
    println!("\nIron condor strategies work best in:");
    println!("  - Range-bound markets with low expected volatility");
    println!("  - High implied volatility environments that tend to revert");
    println!("  - Markets with well-defined support and resistance levels");
    
    Ok(())
}

// Helper function to generate sample price data
fn generate_sample_price_data() -> Result<DataFrame, PolarsError> {
    // Create synthetic price data
    let dates: Vec<String> = (0..90)
        .map(|i| format!("2023-{:02}-{:02}", (i % 12) + 1, (i % 28) + 1))
        .collect();
    
    // Generate price data with range-bound behavior (ideal for iron condors)
    let center_price = 100.0;
    let range_width = 15.0;
    
    let mut price = center_price;
    let mut prices = Vec::with_capacity(90);
    let mut open_prices = Vec::with_capacity(90);
    let mut high_prices = Vec::with_capacity(90);
    let mut low_prices = Vec::with_capacity(90);
    let mut volumes = Vec::with_capacity(90);
    
    // Generate mean-reverting price series with occasional volatility spikes
    for i in 0..90 {
        // Mean reversion factor - stronger when price deviates from center
        let price_deviation = (price - center_price) / center_price;
        let mean_reversion = -price_deviation * 0.08 * price;
        
        // Random component
        let random_factor = (rand::random::<f64>() - 0.5) * price * 0.02;
        
        // Add some cycles to create a choppy but range-bound market
        let cycle1 = (i as f64 * 0.1).sin() * price * 0.01;
        let cycle2 = (i as f64 * 0.05).sin() * price * 0.02;
        
        // Occasional volatility spikes (bad for iron condors)
        let volatility_spike = if i % 21 == 0 {
            (rand::random::<f64>() - 0.5) * price * 0.05
        } else {
            0.0
        };
        
        let daily_change = mean_reversion + random_factor + cycle1 + cycle2 + volatility_spike;
        
        let open = price;
        price += daily_change;
        
        // Keep price within range
        price = price.max(center_price - range_width).min(center_price + range_width);
        
        // Intraday volatility
        let volatility_factor = 0.015; // 1.5% intraday volatility
        let high = f64::max(open, price) * (1.0 + rand::random::<f64>() * volatility_factor);
        let low = f64::min(open, price) * (1.0 - rand::random::<f64>() * volatility_factor);
        
        // Higher volume on bigger price moves
        let volume = 1_000_000.0 * (1.0 + daily_change.abs() / price * 10.0);
        
        open_prices.push(open);
        high_prices.push(high);
        low_prices.push(low);
        prices.push(price);
        volumes.push(volume);
    }
    
    DataFrame::new(vec![
        Series::new("date".into(), dates).into(),
        Series::new("open".into(), open_prices).into(),
        Series::new("high".into(), high_prices).into(),
        Series::new("low".into(), low_prices).into(),
        Series::new("close".into(), prices).into(),
        Series::new("volume".into(), volumes).into(),
    ])
}

// Helper function to generate sample options data
fn generate_sample_options_data(price_df: &DataFrame) -> Result<DataFrame, PolarsError> {
    // This would normally be a much larger DataFrame with multiple options contracts
    // For simplicity, we'll create just enough for the example
    
    let close_prices = price_df.column("close")?.f64()?;
    let dates = price_df.column("date")?.str()?;
    
    let mut option_dates = Vec::new();
    let mut underlying_prices = Vec::new();
    let mut strike_prices = Vec::new();
    let mut option_types = Vec::new();
    let mut expirations = Vec::new();
    let mut bid_prices = Vec::new();
    let mut ask_prices = Vec::new();
    let mut implied_vols = Vec::new();
    let mut deltas = Vec::new();
    let mut gammas = Vec::new();
    let mut thetas = Vec::new();
    let mut vegas = Vec::new();
    
    // Generate options data for each day
    for day in 0..close_prices.len() {
        let date = dates.get(day).unwrap_or("").to_string();
        let current_price = close_prices.get(day).unwrap_or(0.0);
        
        // Generate strikes around current price
        for strike_offset in [-15.0, -10.0, -5.0, -2.5, 0.0, 2.5, 5.0, 10.0, 15.0] {
            let strike = (current_price + strike_offset).round();
            
            for option_type in ["call", "put"] {
                option_dates.push(date.clone());
                underlying_prices.push(current_price);
                strike_prices.push(strike);
                option_types.push(option_type.to_string());
                
                // Create 45-day expiration
                let expiry = format!("2023-{:02}-{:02}", 
                                  ((day / 28) % 12) + 2, 
                                  ((day % 28) + 45) % 28 + 1);
                expirations.push(expiry);
                
                // Calculate approximate option prices and greeks
                let distance_pct = (current_price - strike) / current_price;
                
                // Higher IV for further expirations and strikes
                let iv_base = 0.25 + rand::random::<f64>() * 0.05; // 25% IV base
                let implied_vol = iv_base * (1.0 + 0.2 * distance_pct.abs());
                
                // Simple approximation for demo
                let time_to_expiry = 45.0 / 365.0;
                let option_value = if option_type == "call" {
                    if strike <= current_price {
                        // In the money call
                        (current_price - strike) + 
                        current_price * implied_vol * f64::sqrt(time_to_expiry) * 0.4
                    } else {
                        // Out of the money call
                        current_price * implied_vol * f64::sqrt(time_to_expiry) * 0.4 * 
                        f64::exp(-1.0 * distance_pct.powi(2) / 0.2)
                    }
                } else {
                    if strike >= current_price {
                        // In the money put
                        (strike - current_price) + 
                        current_price * implied_vol * f64::sqrt(time_to_expiry) * 0.4
                    } else {
                        // Out of the money put
                        current_price * implied_vol * f64::sqrt(time_to_expiry) * 0.4 * 
                        f64::exp(-1.0 * distance_pct.powi(2) / 0.2)
                    }
                };
                
                let option_value = f64::max(0.05, option_value);
                
                // Add bid/ask with a spread
                let spread = f64::max(0.05, option_value * 0.1);
                bid_prices.push(option_value - spread/2.0);
                ask_prices.push(option_value + spread/2.0);
                
                // Add Greeks
                implied_vols.push(implied_vol);
                
                // Delta calculation (approximate)
                let delta = if option_type == "call" {
                    if strike < current_price {
                        // ITM call: 0.5 to 1.0
                        0.5 + 0.5 * (1.0 - (-distance_pct).min(1.0))
                    } else {
                        // OTM call: 0 to 0.5
                        0.5 * f64::exp(-1.0 * distance_pct.powi(2) / 0.1)
                    }
                } else {
                    if strike > current_price {
                        // ITM put: -0.5 to -1.0
                        -0.5 - 0.5 * (1.0 - distance_pct.min(1.0))
                    } else {
                        // OTM put: 0 to -0.5
                        -0.5 * f64::exp(-1.0 * distance_pct.powi(2) / 0.1)
                    }
                };
                
                deltas.push(delta);
                
                // Approximate other Greeks
                gammas.push(f64::max(0.01, 0.04 * (1.0 - 2.0 * distance_pct.abs())));
                thetas.push(-option_value * 0.01);
                vegas.push(option_value * 0.15);
            }
        }
    }
    
    DataFrame::new(vec![
        Series::new("date".into(), option_dates).into(),
        Series::new("underlying_price".into(), underlying_prices).into(),
        Series::new("strike".into(), strike_prices).into(),
        Series::new("option_type".into(), option_types).into(),
        Series::new("expiration".into(), expirations).into(),
        Series::new("bid".into(), bid_prices).into(),
        Series::new("ask".into(), ask_prices).into(),
        Series::new("implied_volatility".into(), implied_vols).into(),
        Series::new("delta".into(), deltas).into(),
        Series::new("gamma".into(), gammas).into(),
        Series::new("theta".into(), thetas).into(),
        Series::new("vega".into(), vegas).into(),
    ])
} 