use polars::prelude::*;
use ta_lib_in_rust::strategy::options::vertical_spreads::{run_strategy, StrategyParams, calculate_performance};

fn main() -> Result<(), PolarsError> {
    println!("Options Vertical Spreads Strategy Example\n");
    
    // Load or generate sample price data
    let price_df = generate_sample_price_data()?;
    
    // Generate sample options data
    let options_df = generate_sample_options_data(&price_df)?;
    
    // Configure vertical spreads strategy parameters
    let params = StrategyParams {
        spread_type: "bull_put".to_string(),
        min_days_to_expiry: 30,
        max_days_to_expiry: 45, 
        short_option_delta_target: 0.30,
        strike_width: 5.0,
        max_risk_pct: 2.0,
        profit_target_pct: 50.0,
        stop_loss_pct: 100.0,
        use_rsi_filter: true,
        rsi_period: 14,
        rsi_oversold: 30.0,
        rsi_overbought: 70.0,
        use_iv_filter: true,
        iv_percentile_threshold: 60.0,
        use_trend_filter: true,
        ema_short_period: 10,
        ema_long_period: 30,
        days_to_close_before_expiry: 10,
        max_concurrent_spreads: 4
    };
    
    // Run the strategy
    println!("Running vertical spreads strategy with parameters:");
    println!("  Spread Type: {}", params.spread_type);
    println!("  Days to Expiration: {}-{}", params.min_days_to_expiry, params.max_days_to_expiry);
    println!("  Delta Target: {}", params.short_option_delta_target);
    println!("  Strike Width: ${:.2}", params.strike_width);
    println!("  Profit Target: {}%", params.profit_target_pct);
    println!("  Stop Loss: {}%", params.stop_loss_pct);
    println!("");
    
    let signals = run_strategy(&price_df, &options_df, &params)?;
    
    // Calculate performance metrics
    let initial_capital = 10000.0;
    let first_n_trades = signals.trade_details.iter()
        .take(std::cmp::min(signals.trade_details.len(), 10))
        .cloned()
        .collect::<Vec<_>>();
    
    let (final_capital, total_return, num_trades, win_rate, max_drawdown, profit_factor, avg_trade_duration) = 
        calculate_performance(&first_n_trades, initial_capital);
    
    // Display strategy performance
    println!("\nStrategy Performance:");
    println!("  Initial Capital: ${:.2}", initial_capital);
    println!("  Final Capital: ${:.2}", final_capital);
    println!("  Total Return: {:.2}%", total_return);
    println!("  Number of Trades: {}", num_trades);
    println!("  Win Rate: {:.2}%", win_rate);
    println!("  Maximum Drawdown: {:.2}%", max_drawdown);
    println!("  Profit Factor: {:.2}", profit_factor);
    println!("  Average Trade Duration: {:.1} days", avg_trade_duration);
    
    // Display sample trades
    println!("\nSample Trade Details:");
    if signals.trade_details.is_empty() {
        println!("  No trades were generated");
    } else {
        let sample_trade = &signals.trade_details[0];
        println!("  Entry Date: {}", sample_trade.entry_date);
        println!("  Exit Date: {}", sample_trade.exit_date);
        println!("  Short Strike: {}", sample_trade.short_strike);
        println!("  Long Strike: {}", sample_trade.long_strike);
        println!("  Credit Received: ${:.2}", sample_trade.credit_received);
        println!("  P&L: ${:.2}", sample_trade.pnl);
        println!("  Exit Reason: {}", sample_trade.exit_reason);
    }
    
    println!("\nThis is a simplified example. In real options trading:");
    println!("  - Consider implied volatility environment");
    println!("  - Account for bid-ask spreads in pricing");
    println!("  - Monitor Greek exposures (delta, theta, vega)");
    println!("  - Properly size positions relative to account size");
    
    Ok(())
}

// Helper function to generate sample price data
fn generate_sample_price_data() -> Result<DataFrame, PolarsError> {
    // Create synthetic price data
    let dates: Vec<String> = (0..90)
        .map(|i| format!("2023-{:02}-{:02}", (i % 12) + 1, (i % 28) + 1))
        .collect();
    
    // Generate price data with an upward trend and some volatility
    let mut price = 150.0;
    let mut prices = Vec::with_capacity(90);
    let mut open_prices = Vec::with_capacity(90);
    let mut high_prices = Vec::with_capacity(90);
    let mut low_prices = Vec::with_capacity(90);
    let mut volumes = Vec::with_capacity(90);
    
    for _ in 0..90 {
        let daily_change = (rand::random::<f64>() - 0.45) * 3.0; // Slight bullish bias
        let open = price;
        price += daily_change;
        let high = f64::max(open, price) + rand::random::<f64>() * 1.5;
        let low = f64::min(open, price) - rand::random::<f64>() * 1.5;
        let volume = 1_000_000.0 + rand::random::<f64>() * 1_000_000.0;
        
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
        for strike_offset in [-15.0, -10.0, -5.0, 0.0, 5.0, 10.0, 15.0] {
            let strike = (current_price + strike_offset).round();
            
            // Add put option
            option_dates.push(date.clone());
            underlying_prices.push(current_price);
            strike_prices.push(strike);
            option_types.push("put".to_string());
            
            // Add 30-day expiration
            let expiry = format!("2023-{:02}-{:02}", 
                                 ((day / 28) % 12) + 2, 
                                 ((day % 28) + 30) % 28 + 1);
            expirations.push(expiry);
            
            // Calculate approximate option prices and greeks
            let distance_pct = (current_price - strike) / current_price;
            let iv_base = 0.30 + rand::random::<f64>() * 0.10; // 30% IV base
            let implied_vol = iv_base * (1.0 - 0.5 * distance_pct);
            
            // Simple approximation of Black-Scholes for demo
            let time_to_expiry = 30.0 / 365.0;
            let put_value = if strike <= current_price {
                (strike - current_price) + 
                strike * implied_vol * f64::sqrt(time_to_expiry) * 0.4
            } else {
                (strike - current_price) +
                strike * implied_vol * f64::sqrt(time_to_expiry) * 0.4
            };
            let put_value = f64::max(0.05, put_value);
            
            // Add bid/ask with a spread
            let mid_price = f64::max(0.05, put_value);
            let spread = f64::max(0.05, mid_price * 0.1);
            bid_prices.push(mid_price - spread/2.0);
            ask_prices.push(mid_price + spread/2.0);
            
            // Calculate approximate greeks
            implied_vols.push(implied_vol);
            
            // Delta: -1 to 0 for puts, closer to -0.5 at the money
            let delta = if strike < current_price {
                -0.1 - 0.4 * (1.0 - distance_pct.abs())
            } else {
                -0.5 - 0.4 * (1.0 - distance_pct.abs())
            };
            deltas.push(delta);
            
            // Other greeks - simplified approximations
            gammas.push(f64::max(0.01, 0.05 * (1.0 - 2.0 * distance_pct.abs())));
            thetas.push(-0.02 * mid_price);
            vegas.push(0.1 * mid_price);
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