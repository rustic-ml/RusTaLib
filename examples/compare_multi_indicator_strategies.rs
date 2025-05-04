use polars::prelude::*;
use ta_lib_in_rust::strategy::daily::multi_indicator_daily_1;
use ta_lib_in_rust::strategy::daily::multi_indicator_daily_2;
use ta_lib_in_rust::strategy::daily::multi_indicator_daily_3;
use ta_lib_in_rust::strategy::daily::multi_indicator_daily_4;

fn main() -> Result<(), PolarsError> {
    // Load AAPL daily OHLCV data
    let file_path = "examples/AAPL_daily_ohlcv.csv";

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
    let df = df
        .lazy()
        .select([
            col("AAPL.Open").alias("open"),
            col("AAPL.High").alias("high"),
            col("AAPL.Low").alias("low"),
            col("AAPL.Close").alias("close"),
            col("AAPL.Volume").cast(DataType::Float64).alias("volume"),
        ])
        .collect()?;

    println!("--------------------------------------------------------------");
    println!("COMPARATIVE ANALYSIS OF MULTI-INDICATOR TRADING STRATEGIES");
    println!("--------------------------------------------------------------\n");

    println!("Running Strategy 1: Standard Multi-Indicator...");
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
    let close_prices = df.column("close")?;
    let (final_value1, total_return1, num_trades1, win_rate1, max_drawdown1, profit_factor1) =
        multi_indicator_daily_1::calculate_performance(
            close_prices,
            &signals1.buy_signals,
            &signals1.sell_signals,
            10000.0,
        );

    println!("Strategy 1 Results:");
    println!("- Final Value: ${:.2}", final_value1);
    println!("- Total Return: {:.2}%", total_return1);
    println!("- Number of Trades: {}", num_trades1);
    println!("- Win Rate: {:.2}%", win_rate1);
    println!("- Maximum Drawdown: {:.2}%", max_drawdown1 * 100.0);
    println!("- Profit Factor: {:.2}", profit_factor1);
    println!();

    println!("Running Strategy 2: Volatility-Focused Multi-Indicator...");
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

    println!("Strategy 2 Results:");
    println!("- Final Value: ${:.2}", final_value2);
    println!("- Total Return: {:.2}%", total_return2);
    println!("- Number of Trades: {}", num_trades2);
    println!("- Win Rate: {:.2}%", win_rate2);
    println!("- Maximum Drawdown: {:.2}%", max_drawdown2 * 100.0);
    println!("- Profit Factor: {:.2}", profit_factor2);
    println!();

    println!(
        "Running Strategy 3: Adaptive Trend-Filtered Strategy with Dynamic Position Sizing..."
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

    println!("Strategy 3 Results:");
    println!("- Final Value: ${:.2}", final_value3);
    println!("- Total Return: {:.2}%", total_return3);
    println!("- Number of Trades: {}", num_trades3);
    println!("- Win Rate: {:.2}%", win_rate3);
    println!("- Maximum Drawdown: {:.2}%", max_drawdown3 * 100.0);
    println!("- Profit Factor: {:.2}", profit_factor3);
    println!();

    println!("Running Strategy 4: Hybrid Adaptive Strategy...");
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

    println!("Strategy 4 Results:");
    println!("- Final Value: ${:.2}", final_value4);
    println!("- Total Return: {:.2}%", total_return4);
    println!("- Number of Trades: {}", num_trades4);
    println!("- Win Rate: {:.2}%", win_rate4);
    println!("- Maximum Drawdown: {:.2}%", max_drawdown4 * 100.0);
    println!("- Profit Factor: {:.2}", profit_factor4);
    println!();

    // Determine the best strategy
    println!("--------------------------------------------------------------");
    println!("STRATEGY COMPARISON SUMMARY");
    println!("--------------------------------------------------------------");

    // Find the best strategy
    let returns = [total_return1, total_return2, total_return3, total_return4];
    let max_return = returns.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

    if total_return1 == max_return {
        println!(
            "Strategy 1 (Standard Multi-Indicator) performed BEST with a return of {:.2}%",
            total_return1
        );
        println!(
            "Outperformed Strategy 2 by {:.2}%",
            total_return1 - total_return2
        );
        println!(
            "Outperformed Strategy 3 by {:.2}%",
            total_return1 - total_return3
        );
        println!(
            "Outperformed Strategy 4 by {:.2}%",
            total_return1 - total_return4
        );
    } else if total_return2 == max_return {
        println!("Strategy 2 (Volatility-Focused Multi-Indicator) performed BEST with a return of {:.2}%", 
                total_return2);
        println!(
            "Outperformed Strategy 1 by {:.2}%",
            total_return2 - total_return1
        );
        println!(
            "Outperformed Strategy 3 by {:.2}%",
            total_return2 - total_return3
        );
        println!(
            "Outperformed Strategy 4 by {:.2}%",
            total_return2 - total_return4
        );
    } else if total_return3 == max_return {
        println!(
            "Strategy 3 (Adaptive Trend-Filtered) performed BEST with a return of {:.2}%",
            total_return3
        );
        println!(
            "Outperformed Strategy 1 by {:.2}%",
            total_return3 - total_return1
        );
        println!(
            "Outperformed Strategy 2 by {:.2}%",
            total_return3 - total_return2
        );
        println!(
            "Outperformed Strategy 4 by {:.2}%",
            total_return3 - total_return4
        );
    } else {
        println!(
            "Strategy 4 (Hybrid Adaptive Strategy) performed BEST with a return of {:.2}%",
            total_return4
        );
        println!(
            "Outperformed Strategy 1 by {:.2}%",
            total_return4 - total_return1
        );
        println!(
            "Outperformed Strategy 2 by {:.2}%",
            total_return4 - total_return2
        );
        println!(
            "Outperformed Strategy 3 by {:.2}%",
            total_return4 - total_return3
        );
    }

    // Compare key metrics
    println!("\nKey Metrics Comparison (Strategy 1 vs Strategy 2 vs Strategy 3 vs Strategy 4):");
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

    println!("\nBest metrics by category:");
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

    Ok(())
}
