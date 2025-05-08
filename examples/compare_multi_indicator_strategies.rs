use polars::prelude::*;
use ta_lib_in_rust::strategy::daily::multi_indicator_daily_1;
use ta_lib_in_rust::strategy::daily::multi_indicator_daily_2;
use ta_lib_in_rust::strategy::daily::multi_indicator_daily_3;
use ta_lib_in_rust::strategy::daily::multi_indicator_daily_4;
use ta_lib_in_rust::{FeatureSelection, select_features};
use std::env;

fn main() -> Result<(), PolarsError> {
    // Parse CLI arguments for feature selection
    let args: Vec<String> = env::args().collect();
    let feature_mode = args.get(1).map(|s| s.as_str()).unwrap_or("all");
    let strategy_name = args.get(2).map(|s| s.as_str()).unwrap_or("daily_1");

    // Define the tickers to analyze
    let tickers = vec!["AAPL", "GOOGL", "MSFT"];

    // Store results for each strategy and ticker
    struct StrategyResult {
        ticker: String,
        strategy_name: String,
        final_value: f64,
        total_return: f64,
        num_trades: usize,
        win_rate: f64,
        max_drawdown: f64,
        profit_factor: f64,
    }

    let mut all_results: Vec<StrategyResult> = Vec::new();

    // Process each ticker
    for ticker in &tickers {
        println!("\n==============================================================");
        println!("ANALYZING {}", ticker);
        println!("==============================================================");

        // Load ticker's daily OHLCV data
        let file_path = format!("examples/csv/{}_daily_ohlcv.csv", ticker);

        // Read CSV file with Polars using CsvReadOptions
        let df = CsvReadOptions::default()
            .with_has_header(true)
            .try_into_reader_with_file_path(Some(file_path.into()))?
            .finish()?;

        // Print the available columns
        println!("Available columns: {:?}", df.get_column_names());
        println!("Data types:");
        for col_name in df.get_column_names() {
            println!("  {} -> {}", col_name, df.column(col_name)?.dtype());
        }

        // Create a new DataFrame with renamed columns and proper types
        let mut df = df
            .lazy()
            .select([
                col("Open").alias("open"),
                col("High").alias("high"),
                col("Low").alias("low"),
                col("Close").alias("close"),
                col("Volume").cast(DataType::Float64).alias("volume"),
            ])
            .collect()?;

        // Feature selection logic
        let selection = match feature_mode {
            "indicators" => FeatureSelection::Indicators,
            "strategy" => FeatureSelection::Strategy { strategy_name, params: None },
            _ => FeatureSelection::All { strategy_name, params: None },
        };
        let result_df = select_features(&mut df, selection)?;
        println!("\nResulting DataFrame for {ticker} ({feature_mode}):\n{result_df}");

        println!("--------------------------------------------------------------");
        println!(
            "COMPARATIVE ANALYSIS OF MULTI-INDICATOR TRADING STRATEGIES FOR {}",
            ticker
        );
        println!("--------------------------------------------------------------\n");

        // Save reference to close prices for performance calculations
        let close_prices = df.column("close")?;

        // Run Strategy 1
        println!(
            "Running Strategy 1: Standard Multi-Indicator for {}...",
            ticker
        );
        let strategy1_params = multi_indicator_daily_1::StrategyParams::default();

        println!("Strategy 1 Parameters:");
        println!("- SMA Short Period: {}", strategy1_params.sma_short_period);
        println!("- SMA Long Period: {}", strategy1_params.sma_long_period);
        println!("- RSI Period: {}", strategy1_params.rsi_period);
        println!("- RSI Overbought: {}", strategy1_params.rsi_overbought);
        println!("- RSI Oversold: {}", strategy1_params.rsi_oversold);
        println!("- Bollinger Bands Period: {}", strategy1_params.bb_period);
        println!("- Bollinger Bands StdDev: {}", strategy1_params.bb_std_dev);
        println!("- MACD Fast: {}", strategy1_params.macd_fast);
        println!("- MACD Slow: {}", strategy1_params.macd_slow);
        println!("- MACD Signal: {}", strategy1_params.macd_signal);
        println!(
            "- Min Signals for Buy: {}",
            strategy1_params.min_signals_for_buy
        );
        println!(
            "- Min Signals for Sell: {}",
            strategy1_params.min_signals_for_sell
        );
        println!();

        // Run strategy 1
        let signals1 = multi_indicator_daily_1::run_strategy(&df, &strategy1_params)?;
        let (final_value1, total_return1, num_trades1, win_rate1, max_drawdown1, profit_factor1) =
            multi_indicator_daily_1::calculate_performance(
                close_prices,
                &signals1.buy_signals,
                &signals1.sell_signals,
                10000.0,
            );

        println!("Strategy 1 Results for {}:", ticker);
        println!("- Final Value: ${:.2}", final_value1);
        println!("- Total Return: {:.2}%", total_return1);
        println!("- Number of Trades: {}", num_trades1);
        println!("- Win Rate: {:.2}%", win_rate1);
        println!("- Maximum Drawdown: {:.2}%", max_drawdown1 * 100.0);
        println!("- Profit Factor: {:.2}", profit_factor1);
        println!();

        // Save strategy 1 results
        all_results.push(StrategyResult {
            ticker: ticker.to_string(),
            strategy_name: "Standard Multi-Indicator".to_string(),
            final_value: final_value1,
            total_return: total_return1,
            num_trades: num_trades1,
            win_rate: win_rate1,
            max_drawdown: max_drawdown1,
            profit_factor: profit_factor1,
        });

        // Run Strategy 2
        println!(
            "Running Strategy 2: Volatility-Focused Multi-Indicator for {}...",
            ticker
        );
        let strategy2_params = multi_indicator_daily_2::StrategyParams::default();

        println!("Strategy 2 Parameters:");
        println!("- SMA Short Period: {}", strategy2_params.sma_short_period);
        println!("- SMA Long Period: {}", strategy2_params.sma_long_period);
        println!("- RSI Period: {}", strategy2_params.rsi_period);
        println!("- RSI Overbought: {}", strategy2_params.rsi_overbought);
        println!("- RSI Oversold: {}", strategy2_params.rsi_oversold);
        println!("- Bollinger Bands Period: {}", strategy2_params.bb_period);
        println!("- Bollinger Bands StdDev: {}", strategy2_params.bb_std_dev);
        println!("- MACD Fast: {}", strategy2_params.macd_fast);
        println!("- MACD Slow: {}", strategy2_params.macd_slow);
        println!("- MACD Signal: {}", strategy2_params.macd_signal);
        println!("- ATR Period: {}", strategy2_params.atr_period);
        println!("- ATR Multiplier: {}", strategy2_params.atr_multiplier);
        println!("- Volume Threshold: {}", strategy2_params.volume_threshold);
        println!(
            "- Min Signals for Buy: {}",
            strategy2_params.min_signals_for_buy
        );
        println!(
            "- Min Signals for Sell: {}",
            strategy2_params.min_signals_for_sell
        );
        println!();

        // Run strategy 2
        let signals2 = multi_indicator_daily_2::run_strategy(&df, &strategy2_params)?;
        let (final_value2, total_return2, num_trades2, win_rate2, max_drawdown2, profit_factor2) =
            multi_indicator_daily_2::calculate_performance(
                close_prices,
                &signals2.buy_signals,
                &signals2.sell_signals,
                10000.0,
            );

        println!("Strategy 2 Results for {}:", ticker);
        println!("- Final Value: ${:.2}", final_value2);
        println!("- Total Return: {:.2}%", total_return2);
        println!("- Number of Trades: {}", num_trades2);
        println!("- Win Rate: {:.2}%", win_rate2);
        println!("- Maximum Drawdown: {:.2}%", max_drawdown2 * 100.0);
        println!("- Profit Factor: {:.2}", profit_factor2);
        println!();

        // Save strategy 2 results
        all_results.push(StrategyResult {
            ticker: ticker.to_string(),
            strategy_name: "Volatility-Focused Multi-Indicator".to_string(),
            final_value: final_value2,
            total_return: total_return2,
            num_trades: num_trades2,
            win_rate: win_rate2,
            max_drawdown: max_drawdown2,
            profit_factor: profit_factor2,
        });

        // Run Strategy 3
        println!(
            "Running Strategy 3: Adaptive Trend-Filtered Strategy with Dynamic Position Sizing for {}...", ticker
        );
        let strategy3_params = multi_indicator_daily_3::StrategyParams::default();

        println!("Strategy 3 Parameters:");
        println!("- EMA Short Period: {}", strategy3_params.ema_short_period);
        println!("- EMA Mid Period: {}", strategy3_params.ema_mid_period);
        println!("- EMA Long Period: {}", strategy3_params.ema_long_period);
        println!("- RSI Period: {}", strategy3_params.rsi_period);
        println!("- RSI Overbought: {}", strategy3_params.rsi_overbought);
        println!("- RSI Oversold: {}", strategy3_params.rsi_oversold);
        println!("- Bollinger Bands Period: {}", strategy3_params.bb_period);
        println!("- Bollinger Bands StdDev: {}", strategy3_params.bb_std_dev);
        println!("- MACD Fast: {}", strategy3_params.macd_fast);
        println!("- MACD Slow: {}", strategy3_params.macd_slow);
        println!("- MACD Signal: {}", strategy3_params.macd_signal);
        println!("- ATR Period: {}", strategy3_params.atr_period);
        println!(
            "- ATR Position Size Factor: {}",
            strategy3_params.atr_position_size_factor
        );
        println!("- OBV EMA Period: {}", strategy3_params.obv_ema_period);
        println!("- Volume Threshold: {}", strategy3_params.volume_threshold);
        println!(
            "- Min Signals for Buy: {}",
            strategy3_params.min_signals_for_buy
        );
        println!(
            "- Min Signals for Sell: {}",
            strategy3_params.min_signals_for_sell
        );
        println!(
            "- Stop Loss ATR Multiple: {}",
            strategy3_params.stop_loss_atr_multiple
        );
        println!(
            "- Take Profit ATR Multiple: {}",
            strategy3_params.take_profit_atr_multiple
        );
        println!();

        // Run strategy 3
        let signals3 = multi_indicator_daily_3::run_strategy(&df, &strategy3_params)?;
        let (final_value3, total_return3, num_trades3, win_rate3, max_drawdown3, profit_factor3) =
            multi_indicator_daily_3::calculate_performance(
                close_prices,
                &signals3.buy_signals,
                &signals3.sell_signals,
                &signals3.position_sizes,
                10000.0,
            );

        println!("Strategy 3 Results for {}:", ticker);
        println!("- Final Value: ${:.2}", final_value3);
        println!("- Total Return: {:.2}%", total_return3);
        println!("- Number of Trades: {}", num_trades3);
        println!("- Win Rate: {:.2}%", win_rate3);
        println!("- Maximum Drawdown: {:.2}%", max_drawdown3 * 100.0);
        println!("- Profit Factor: {:.2}", profit_factor3);
        println!();

        // Save strategy 3 results
        all_results.push(StrategyResult {
            ticker: ticker.to_string(),
            strategy_name: "Adaptive Trend-Filtered".to_string(),
            final_value: final_value3,
            total_return: total_return3,
            num_trades: num_trades3,
            win_rate: win_rate3,
            max_drawdown: max_drawdown3,
            profit_factor: profit_factor3,
        });

        // Run Strategy 4
        println!(
            "Running Strategy 4: Hybrid Adaptive Strategy for {}...",
            ticker
        );
        let strategy4_params = multi_indicator_daily_4::StrategyParams::default();

        println!("Strategy 4 Parameters:");
        println!("- EMA Short Period: {}", strategy4_params.ema_short_period);
        println!("- EMA Mid Period: {}", strategy4_params.ema_mid_period);
        println!("- EMA Long Period: {}", strategy4_params.ema_long_period);
        println!("- SMA Short Period: {}", strategy4_params.sma_short_period);
        println!("- SMA Long Period: {}", strategy4_params.sma_long_period);
        println!("- RSI Period: {}", strategy4_params.rsi_period);
        println!("- RSI Overbought: {}", strategy4_params.rsi_overbought);
        println!("- RSI Oversold: {}", strategy4_params.rsi_oversold);
        println!("- Bollinger Bands Period: {}", strategy4_params.bb_period);
        println!("- Bollinger Bands StdDev: {}", strategy4_params.bb_std_dev);
        println!("- MACD Fast: {}", strategy4_params.macd_fast);
        println!("- MACD Slow: {}", strategy4_params.macd_slow);
        println!("- MACD Signal: {}", strategy4_params.macd_signal);
        println!("- ROC Period: {}", strategy4_params.roc_period);
        println!("- ATR Period: {}", strategy4_params.atr_period);
        println!(
            "- ATR Position Size Factor: {}",
            strategy4_params.atr_position_size_factor
        );
        println!("- OBV EMA Period: {}", strategy4_params.obv_ema_period);
        println!("- Volume Threshold: {}", strategy4_params.volume_threshold);
        println!(
            "- Min Signals for Buy: {}",
            strategy4_params.min_signals_for_buy
        );
        println!(
            "- Min Signals for Sell: {}",
            strategy4_params.min_signals_for_sell
        );
        println!(
            "- Stop Loss ATR Multiple: {}",
            strategy4_params.stop_loss_atr_multiple
        );
        println!(
            "- Take Profit ATR Multiple: {}",
            strategy4_params.take_profit_atr_multiple
        );
        println!(
            "- Trailing Stop Enabled: {}",
            strategy4_params.trailing_stop_enabled
        );
        println!(
            "- Trailing Stop ATR Multiple: {}",
            strategy4_params.trailing_stop_atr_multiple
        );
        println!(
            "- Max Position Size %: {:.1}%",
            strategy4_params.max_position_size_pct * 100.0
        );
        println!();

        // Run strategy 4
        let signals4 = multi_indicator_daily_4::run_strategy(&df, &strategy4_params)?;
        let (final_value4, total_return4, num_trades4, win_rate4, max_drawdown4, profit_factor4) =
            multi_indicator_daily_4::calculate_performance(
                close_prices,
                &signals4.buy_signals,
                &signals4.sell_signals,
                &signals4.position_sizes,
                10000.0,
            );

        println!("Strategy 4 Results for {}:", ticker);
        println!("- Final Value: ${:.2}", final_value4);
        println!("- Total Return: {:.2}%", total_return4);
        println!("- Number of Trades: {}", num_trades4);
        println!("- Win Rate: {:.2}%", win_rate4);
        println!("- Maximum Drawdown: {:.2}%", max_drawdown4 * 100.0);
        println!("- Profit Factor: {:.2}", profit_factor4);
        println!();

        // Save strategy 4 results
        all_results.push(StrategyResult {
            ticker: ticker.to_string(),
            strategy_name: "Hybrid Adaptive Strategy".to_string(),
            final_value: final_value4,
            total_return: total_return4,
            num_trades: num_trades4,
            win_rate: win_rate4,
            max_drawdown: max_drawdown4,
            profit_factor: profit_factor4,
        });

        // Determine the best strategy for this ticker
        println!("--------------------------------------------------------------");
        println!("STRATEGY COMPARISON SUMMARY FOR {}", ticker);
        println!("--------------------------------------------------------------");

        // Find the best strategy for this ticker
        let returns = [total_return1, total_return2, total_return3, total_return4];
        let max_return = returns.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

        if total_return1 == max_return {
            println!(
                "Strategy 1 (Standard Multi-Indicator) performed BEST with a return of {:.2}%",
                total_return1
            );
        } else if total_return2 == max_return {
            println!("Strategy 2 (Volatility-Focused Multi-Indicator) performed BEST with a return of {:.2}%", 
                    total_return2);
        } else if total_return3 == max_return {
            println!(
                "Strategy 3 (Adaptive Trend-Filtered) performed BEST with a return of {:.2}%",
                total_return3
            );
        } else {
            println!(
                "Strategy 4 (Hybrid Adaptive Strategy) performed BEST with a return of {:.2}%",
                total_return4
            );
        }

        // Compare key metrics
        println!("\nKey Metrics Comparison for {} (Strategy 1 vs Strategy 2 vs Strategy 3 vs Strategy 4):", ticker);
        println!(
            "- Number of Trades: {} vs {} vs {} vs {}",
            num_trades1, num_trades2, num_trades3, num_trades4
        );
        println!(
            "- Win Rate: {:.2}% vs {:.2}% vs {:.2}% vs {:.2}%",
            win_rate1, win_rate2, win_rate3, win_rate4
        );
        println!(
            "- Maximum Drawdown: {:.2}% vs {:.2}% vs {:.2}% vs {:.2}%",
            max_drawdown1 * 100.0,
            max_drawdown2 * 100.0,
            max_drawdown3 * 100.0,
            max_drawdown4 * 100.0
        );
        println!(
            "- Profit Factor: {:.2} vs {:.2} vs {:.2} vs {:.2}",
            profit_factor1, profit_factor2, profit_factor3, profit_factor4
        );

        // Highlight best metrics
        let win_rates = [win_rate1, win_rate2, win_rate3, win_rate4];
        let max_win_rate = win_rates.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

        let drawdowns = [max_drawdown1, max_drawdown2, max_drawdown3, max_drawdown4];
        let min_drawdown = drawdowns.iter().fold(f64::INFINITY, |a, &b| a.min(b));

        let profit_factors = [
            profit_factor1,
            profit_factor2,
            profit_factor3,
            profit_factor4,
        ];
        let max_profit_factor = profit_factors
            .iter()
            .fold(f64::NEG_INFINITY, |a, &b| a.max(b));

        println!("\nBest metrics by category for {}:", ticker);
        if win_rate1 == max_win_rate {
            println!("- Strategy 1 had the highest win rate: {:.2}%", win_rate1);
        } else if win_rate2 == max_win_rate {
            println!("- Strategy 2 had the highest win rate: {:.2}%", win_rate2);
        } else if win_rate3 == max_win_rate {
            println!("- Strategy 3 had the highest win rate: {:.2}%", win_rate3);
        } else {
            println!("- Strategy 4 had the highest win rate: {:.2}%", win_rate4);
        }

        if max_drawdown1 == min_drawdown {
            println!(
                "- Strategy 1 had the lowest maximum drawdown: {:.2}%",
                max_drawdown1 * 100.0
            );
        } else if max_drawdown2 == min_drawdown {
            println!(
                "- Strategy 2 had the lowest maximum drawdown: {:.2}%",
                max_drawdown2 * 100.0
            );
        } else if max_drawdown3 == min_drawdown {
            println!(
                "- Strategy 3 had the lowest maximum drawdown: {:.2}%",
                max_drawdown3 * 100.0
            );
        } else {
            println!(
                "- Strategy 4 had the lowest maximum drawdown: {:.2}%",
                max_drawdown4 * 100.0
            );
        }

        if profit_factor1 == max_profit_factor {
            println!(
                "- Strategy 1 had the highest profit factor: {:.2}",
                profit_factor1
            );
        } else if profit_factor2 == max_profit_factor {
            println!(
                "- Strategy 2 had the highest profit factor: {:.2}",
                profit_factor2
            );
        } else if profit_factor3 == max_profit_factor {
            println!(
                "- Strategy 3 had the highest profit factor: {:.2}",
                profit_factor3
            );
        } else {
            println!(
                "- Strategy 4 had the highest profit factor: {:.2}",
                profit_factor4
            );
        }
    }

    // Cross-ticker comparison for each strategy
    println!("\n==============================================================");
    println!("CROSS-TICKER COMPARISON BY STRATEGY");
    println!("==============================================================");

    // Group results by strategy
    let strategy_names = vec![
        "Standard Multi-Indicator",
        "Volatility-Focused Multi-Indicator",
        "Adaptive Trend-Filtered",
        "Hybrid Adaptive Strategy",
    ];

    for strategy_name in &strategy_names {
        println!("\nStrategy: {}", strategy_name);
        println!("--------------------------");
        println!(
            "{:<6} {:<15} {:<10} {:<10} {:<10} {:<10}",
            "Ticker", "Return (%)", "Final Value", "Trades", "Win Rate", "Max DD%"
        );

        // Filter results for this strategy
        let strategy_results: Vec<&StrategyResult> = all_results
            .iter()
            .filter(|r| r.strategy_name == *strategy_name)
            .collect();

        // Print results for each ticker
        for result in &strategy_results {
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

        // Find best performing ticker for this strategy
        if !strategy_results.is_empty() {
            let best_ticker = strategy_results
                .iter()
                .max_by(|a, b| a.total_return.partial_cmp(&b.total_return).unwrap())
                .unwrap();

            println!(
                "\n{} performed best on {} with a {:.2}% return.",
                strategy_name, best_ticker.ticker, best_ticker.total_return
            );
        }
    }

    // Overall best combination
    println!("\n==============================================================");
    println!("OVERALL BEST STRATEGY-TICKER COMBINATION");
    println!("==============================================================");

    if !all_results.is_empty() {
        let best_overall = all_results
            .iter()
            .max_by(|a, b| a.total_return.partial_cmp(&b.total_return).unwrap())
            .unwrap();

        println!(
            "The best overall performance was {} on {} with a {:.2}% return.",
            best_overall.strategy_name, best_overall.ticker, best_overall.total_return
        );
        println!("Performance details:");
        println!("- Final Value: ${:.2}", best_overall.final_value);
        println!("- Number of Trades: {}", best_overall.num_trades);
        println!("- Win Rate: {:.2}%", best_overall.win_rate);
        println!(
            "- Maximum Drawdown: {:.2}%",
            best_overall.max_drawdown * 100.0
        );
        println!("- Profit Factor: {:.2}", best_overall.profit_factor);
    }

    Ok(())
}
