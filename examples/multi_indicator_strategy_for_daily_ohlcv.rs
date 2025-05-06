use polars::prelude::*;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use ta_lib_in_rust::strategy::daily::multi_indicator_daily_1::{
    calculate_performance, run_strategy, StrategyParams,
};

fn main() -> Result<(), PolarsError> {
    // Define the tickers to analyze
    let tickers = vec!["AAPL", "GOOGL", "MSFT"];
    
    // Store results for each ticker
    let mut ticker_results = Vec::new();

    // Process each ticker
    for ticker in &tickers {
        println!("\n--------------------------------------------------------------");
        println!("ANALYZING {}", ticker);
        println!("--------------------------------------------------------------");
        
        // Load ticker's daily OHLCV data
        let file_path = format!("examples/csv/{}_daily_ohlcv.csv", ticker);

        // The CSV now has headers: Symbol, Timestamp, Open, High, Low, Close, Volume, VWAP
        let df = CsvReadOptions::default()
            .with_has_header(true)
            .try_into_reader_with_file_path(Some(Path::new(&file_path).to_path_buf()))?
            .finish()?;
        
        // Create a DataFrame with lowercase column names expected by the indicators
        let lowercase_df = df.lazy()
            .select([
                col("Open").alias("open"),
                col("High").alias("high"),
                col("Low").alias("low"),
                col("Close").alias("close"),
                col("Volume").alias("volume"),
                col("Timestamp").alias("date")
            ])
            .collect()?;

        // Define parameter grid for testing
        let sma_short_periods = vec![10, 20, 50];
        let sma_long_periods = vec![30, 50, 100];
        let rsi_periods = vec![9, 14, 21];
        let rsi_overboughts = vec![65.0, 70.0, 75.0];
        let rsi_oversolds = vec![25.0, 30.0, 35.0];
        let bb_periods = vec![14, 20, 30];
        let bb_std_devs = vec![1.5, 2.0, 2.5];
        let macd_fasts = vec![8, 12, 16];
        let macd_slows = vec![21, 26, 30];
        let macd_signals = vec![7, 9, 12];
        let min_signals = vec![2, 3, 4];

        // Store the best parameter combinations
        #[derive(Clone)]
        struct BacktestResult {
            ticker: String,
            params: StrategyParams,
            final_value: f64,
            total_return: f64,
            num_trades: usize,
            win_rate: f64,
            max_drawdown: f64,
            profit_factor: f64,
        }

        let mut best_params: Option<BacktestResult> = None;
        let mut all_results = Vec::new();
        let mut param_count = 0;

        // Limit the number of combinations to avoid too many computations
        let max_combinations = 50;
        let start_capital = 10000.0;

        println!("Running backtests with different parameter combinations for {}...", ticker);

        for sma_short in &sma_short_periods {
            for sma_long in &sma_long_periods {
                if sma_short >= sma_long {
                    continue;
                } // Skip invalid combinations

                for rsi_period in &rsi_periods {
                    for &rsi_overbought in &rsi_overboughts {
                        for &rsi_oversold in &rsi_oversolds {
                            if rsi_oversold >= rsi_overbought {
                                continue;
                            } // Skip invalid combinations

                            for &bb_period in &bb_periods {
                                for &bb_std_dev in &bb_std_devs {
                                    for &macd_fast in &macd_fasts {
                                        for &macd_slow in &macd_slows {
                                            if macd_fast >= macd_slow {
                                                continue;
                                            } // Skip invalid combinations

                                            for &macd_signal in &macd_signals {
                                                for &min_signal_buy in &min_signals {
                                                    for &min_signal_sell in &min_signals {
                                                        param_count += 1;

                                                        // Limit number of combinations
                                                        if param_count > max_combinations {
                                                            break;
                                                        }

                                                        let params = StrategyParams {
                                                            sma_short_period: *sma_short,
                                                            sma_long_period: *sma_long,
                                                            rsi_period: *rsi_period,
                                                            rsi_overbought,
                                                            rsi_oversold,
                                                            bb_period,
                                                            bb_std_dev,
                                                            macd_fast,
                                                            macd_slow,
                                                            macd_signal,
                                                            min_signals_for_buy: min_signal_buy,
                                                            min_signals_for_sell: min_signal_sell,
                                                        };

                                                        println!("Testing combination {}/{} for {}: SMA {}/{}, RSI {}/{}/{}, MACD {}/{}/{}",
                                                            param_count, max_combinations, ticker,
                                                            sma_short, sma_long,
                                                            rsi_period, rsi_oversold, rsi_overbought,
                                                            macd_fast, macd_slow, macd_signal);

                                                        // Run the strategy on the DataFrame with lowercase column names
                                                        match run_strategy(&lowercase_df, &params) {
                                                            Ok(signals) => {
                                                                // Calculate performance metrics
                                                                let close_series =
                                                                    lowercase_df.column("close")?.clone();

                                                                let (
                                                                    final_value,
                                                                    total_return,
                                                                    num_trades,
                                                                    win_rate,
                                                                    max_drawdown,
                                                                    profit_factor,
                                                                ) = calculate_performance(
                                                                    &close_series,
                                                                    &signals.buy_signals,
                                                                    &signals.sell_signals,
                                                                    start_capital,
                                                                );

                                                                let result = BacktestResult {
                                                                    ticker: ticker.to_string(),
                                                                    params: params.clone(),
                                                                    final_value,
                                                                    total_return,
                                                                    num_trades,
                                                                    win_rate,
                                                                    max_drawdown,
                                                                    profit_factor,
                                                                };

                                                                all_results.push(result.clone());

                                                                if best_params.is_none()
                                                                    || result.total_return
                                                                        > best_params
                                                                            .as_ref()
                                                                            .unwrap()
                                                                            .total_return
                                                                {
                                                                    best_params = Some(result);
                                                                }
                                                            }
                                                            Err(e) => {
                                                                println!(
                                                                    "Error running strategy: {:?}",
                                                                    e
                                                                );
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Display top 10 parameter combinations by return for this ticker
        all_results.sort_by(|a, b| b.total_return.partial_cmp(&a.total_return).unwrap());

        println!("\nTop 10 Parameter Combinations for {} by Return:", ticker);
        println!("----------------------------------------");
        println!(
            "{:<5} {:<15} {:<10} {:<10} {:<10} {:<10} {:<10}",
            "Rank", "Return (%)", "Final Value", "Trades", "Win Rate", "Max DD%", "Profit Factor"
        );

        for (i, result) in all_results.iter().take(10).enumerate() {
            println!(
                "{:<5} {:<15.2} {:<10.2} {:<10} {:<10.2}% {:<10.2}% {:<10.2}",
                i + 1,
                result.total_return,
                result.final_value,
                result.num_trades,
                result.win_rate,
                result.max_drawdown * 100.0,
                result.profit_factor
            );
        }

        // Save the best result for this ticker
        if let Some(best) = &best_params {
            ticker_results.push(best.clone());
            
            // Show the best parameters in detail
            println!("\nBest Parameter Combination for {}:", ticker);
            println!("--------------------------");
            println!("SMA Short Period: {}", best.params.sma_short_period);
            println!("SMA Long Period: {}", best.params.sma_long_period);
            println!("RSI Period: {}", best.params.rsi_period);
            println!("RSI Overbought: {}", best.params.rsi_overbought);
            println!("RSI Oversold: {}", best.params.rsi_oversold);
            println!("Bollinger Band Period: {}", best.params.bb_period);
            println!("Bollinger Band Std Dev: {}", best.params.bb_std_dev);
            println!("MACD Fast: {}", best.params.macd_fast);
            println!("MACD Slow: {}", best.params.macd_slow);
            println!("MACD Signal: {}", best.params.macd_signal);
            println!("Min Signals for Buy: {}", best.params.min_signals_for_buy);
            println!("Min Signals for Sell: {}", best.params.min_signals_for_sell);
            println!();
            println!("Performance Metrics:");
            println!("-------------------");
            println!("Final Value: ${:.2}", best.final_value);
            println!("Total Return: {:.2}%", best.total_return);
            println!("Number of Trades: {}", best.num_trades);
            println!("Win Rate: {:.2}%", best.win_rate);
            println!("Maximum Drawdown: {:.2}%", best.max_drawdown * 100.0);
            println!("Profit Factor: {:.2}", best.profit_factor);
        }
    }
    
    // Compare results across tickers
    println!("\n--------------------------------------------------------------");
    println!("CROSS-TICKER COMPARISON");
    println!("--------------------------------------------------------------");
    println!("{:<6} {:<15} {:<10} {:<10} {:<10} {:<10}",
        "Ticker", "Return (%)", "Final Value", "Trades", "Win Rate", "Max DD%");
    
    for result in &ticker_results {
        println!(
            "{:<6} {:<15.2} {:<10.2} {:<10} {:<10.2}% {:<10.2}%",
            result.ticker,
            result.total_return,
            result.final_value,
            result.num_trades,
            result.win_rate,
            result.max_drawdown * 100.0
        );
    }
    
    // Find best performing ticker
    if !ticker_results.is_empty() {
        let best_ticker = ticker_results.iter()
            .max_by(|a, b| a.total_return.partial_cmp(&b.total_return).unwrap())
            .unwrap();
            
        println!("\n{} had the best performance with a {:.2}% return.", 
            best_ticker.ticker, best_ticker.total_return);
    }

    Ok(())
}
