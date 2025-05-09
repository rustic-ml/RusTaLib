use polars::prelude::*;
use ta_lib_in_rust::strategy::stock::mean_reversion::{run_strategy, StrategyParams, calculate_performance};
use std::path::Path;

fn main() -> Result<(), PolarsError> {
    println!("Stock Market Mean Reversion Strategy Example\n");
    
    // Load sample stock data (or use synthetic data if file doesn't exist)
    let file_path = "examples/csv/MSFT_daily_ohlcv.csv";
    
    let df = if Path::new(file_path).exists() {
        // Read from CSV if file exists
        CsvReadOptions::default()
            .with_has_header(true)
            .try_into_reader_with_file_path(Some(Path::new(file_path).to_path_buf()))?
            .finish()?
    } else {
        // Generate synthetic data for demonstration purposes
        println!("CSV file not found, using synthetic data for demonstration...");
        
        // Create a price series with mean-reverting characteristics
        let mut price = 250.0;
        let mean_price = 250.0;
        let dates: Vec<String> = (0..100)
            .map(|i| format!("2023-{:02}-{:02}", (i % 12) + 1, (i % 28) + 1))
            .collect();
        
        let mut prices = Vec::with_capacity(100);
        let mut open_prices = Vec::with_capacity(100);
        let mut high_prices = Vec::with_capacity(100);
        let mut low_prices = Vec::with_capacity(100);
        let mut volumes = Vec::with_capacity(100);
        
        for _ in 0..100 {
            // Mean-reverting factor pulls price back toward mean
            let mean_reversion_factor = (mean_price - price) * 0.05;
            let random_factor = (rand::random::<f64>() - 0.5) * 8.0;
            let daily_change = mean_reversion_factor + random_factor;
            
            let open = price;
            price += daily_change;
            let high = f64::max(open, price) + rand::random::<f64>() * 2.0;
            let low = f64::min(open, price) - rand::random::<f64>() * 2.0;
            // Higher volume on larger price moves
            let volume = 1_000_000.0 * (1.0 + (daily_change.abs() / 4.0));
            
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
        ])?
    };
    
    // Configure mean reversion strategy parameters
    let params = StrategyParams {
        lookback_period: 20,
        zscore_threshold: 2.0,
        profit_target_pct: 5.0,
        stop_loss_pct: 3.0,
    };
    
    // Run the strategy
    println!("Running mean reversion strategy with parameters:");
    println!("  Lookback Period: {}", params.lookback_period);
    println!("  Z-Score Threshold: {}", params.zscore_threshold);
    println!("  Profit Target: {}%", params.profit_target_pct);
    println!("  Stop Loss: {}%", params.stop_loss_pct);
    println!("");
    
    let signals = run_strategy(&df, &params)?;
    
    // Calculate performance metrics
    let close_series = df.column("close")?;
    let initial_capital = 10000.0;
    
    // Extract price data and create a proper Series
    let close_values = close_series.f64()?;
    let close_vec: Vec<f64> = close_values.into_iter()
        .map(|opt_val| opt_val.unwrap_or(0.0))
        .collect();
    let price_series = Series::new("close".into(), close_vec);
    
    let (final_capital, total_return, num_trades, win_rate, max_drawdown, profit_factor) = 
        calculate_performance(&price_series, &signals.buy_signals, &signals.sell_signals, initial_capital);
    
    // Display strategy performance
    println!("\nStrategy Performance:");
    println!("  Initial Capital: ${:.2}", initial_capital);
    println!("  Final Capital: ${:.2}", final_capital);
    println!("  Total Return: {:.2}%", total_return);
    println!("  Number of Trades: {}", num_trades);
    println!("  Win Rate: {:.2}%", win_rate);
    println!("  Maximum Drawdown: {:.2}%", max_drawdown);
    println!("  Profit Factor: {:.2}", profit_factor);
    
    // Show some Z-score values for reference
    println!("\nSample Z-Score Values:");
    let zscore_values = signals.zscore_values.iter().enumerate()
        .filter(|(_, v)| !v.is_nan())
        .take(5)
        .map(|(i, v)| format!("Day {}: {:.2}", i, v))
        .collect::<Vec<_>>()
        .join(", ");
    println!("  {}", zscore_values);
    
    println!("\nMean reversion strategies work best in range-bound markets.");
    println!("They tend to perform poorly in strongly trending markets.");
    
    Ok(())
} 