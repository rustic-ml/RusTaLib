use polars::prelude::*;
use ta_lib_in_rust::strategy::stock::trend_following::{run_strategy, StrategyParams, calculate_performance};
use std::path::Path;

fn main() -> Result<(), PolarsError> {
    println!("Stock Market Trend Following Strategy Example\n");
    
    // Load sample stock data (in a real application, you would load actual market data)
    let file_path = "examples/csv/AAPL_daily_ohlcv.csv";
    
    let df = if Path::new(file_path).exists() {
        // Read from CSV if file exists
        CsvReadOptions::default()
            .with_has_header(true)
            .try_into_reader_with_file_path(Some(Path::new(file_path).to_path_buf()))?
            .finish()?
    } else {
        // Generate synthetic data for demonstration purposes
        println!("CSV file not found, using synthetic data for demonstration...");
        
        // Generate dates (this is just an approximation for demo purposes)
        let dates: Vec<String> = (0..100)
            .map(|i| format!("2023-{:02}-{:02}", (i % 12) + 1, (i % 28) + 1))
            .collect();
        
        // Generate price data with an upward trend and some volatility
        let mut price = 150.0;
        let mut prices = Vec::with_capacity(100);
        let mut open_prices = Vec::with_capacity(100);
        let mut high_prices = Vec::with_capacity(100);
        let mut low_prices = Vec::with_capacity(100);
        let mut volumes = Vec::with_capacity(100);
        
        for _ in 0..100 {
            let daily_change = (rand::random::<f64>() - 0.45) * 5.0; // Slightly bullish bias
            let open = price;
            price += daily_change;
            let high = f64::max(open, price) + rand::random::<f64>() * 2.0;
            let low = f64::min(open, price) - rand::random::<f64>() * 2.0;
            let volume = 1_000_000.0 + rand::random::<f64>() * 1_000_000.0;
            
            open_prices.push(open);
            high_prices.push(high);
            low_prices.push(low);
            prices.push(price);
            volumes.push(volume);
        }
        
        let date_series = Series::new("date".into(), dates);
        let open_series = Series::new("open".into(), open_prices);
        let high_series = Series::new("high".into(), high_prices);
        let low_series = Series::new("low".into(), low_prices);
        let close_series = Series::new("close".into(), prices);
        let volume_series = Series::new("volume".into(), volumes);
        
        DataFrame::new(vec![
            date_series.into(), 
            open_series.into(), 
            high_series.into(), 
            low_series.into(), 
            close_series.into(), 
            volume_series.into()
        ])?
    };
    
    // Configure trend following strategy parameters
    let params = StrategyParams {
        sma_short_period: 20,
        sma_long_period: 50,
        bb_period: 14,
        bb_std_dev: 2.0,
        rsi_period: 14,
        rsi_overbought: 70.0,
        rsi_oversold: 30.0,
        macd_fast: 12,
        macd_slow: 26,
        macd_signal: 9,
        min_signals_for_buy: 2,
        min_signals_for_sell: 2,
    };
    
    // Run the strategy
    println!("Running trend following strategy with parameters:");
    println!("  SMA Short Period: {}", params.sma_short_period);
    println!("  SMA Long Period: {}", params.sma_long_period);
    println!("  RSI Period: {}", params.rsi_period);
    println!("  RSI Overbought: {}", params.rsi_overbought);
    println!("  RSI Oversold: {}", params.rsi_oversold);
    println!("");
    
    let signals = run_strategy(&df, &params)?;
    
    // Calculate performance metrics
    let close_series = df.column("close")?.clone();
    let initial_capital = 10000.0;
    
    let (final_capital, total_return, num_trades, win_rate, max_drawdown, profit_factor) = 
        calculate_performance(&close_series, &signals.buy_signals, &signals.sell_signals, initial_capital);
    
    // Display strategy performance
    println!("\nStrategy Performance:");
    println!("  Initial Capital: ${:.2}", initial_capital);
    println!("  Final Capital: ${:.2}", final_capital);
    println!("  Total Return: {:.2}%", total_return);
    println!("  Number of Trades: {}", num_trades);
    println!("  Win Rate: {:.2}%", win_rate);
    println!("  Maximum Drawdown: {:.2}%", max_drawdown);
    println!("  Profit Factor: {:.2}", profit_factor);
    
    println!("\nThis is a simplified example. In a real trading strategy, you would:");
    println!("  - Use actual market data");
    println!("  - Apply proper position sizing");
    println!("  - Consider transaction costs and slippage");
    println!("  - Conduct more thorough backtesting");
    
    Ok(())
} 