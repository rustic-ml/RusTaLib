use polars::prelude::*;
use ta_lib_in_rust::strategy::minute::{
    multi_indicator_minute_1, multi_indicator_minute_2, multi_indicator_minute_3,
};

fn main() -> Result<(), PolarsError> {
    println!("Loading minute OHLCV data...");
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some("examples/AAPL_minute_ohlcv.csv".into()))?
        .finish()?;

    println!("Data shape: {} rows, {} columns", df.height(), df.width());
    println!("First few rows:");
    println!("{}", df.head(Some(5)));

    // Create a DataFrame with lowercase column names expected by the indicators
    let df = df
        .lazy()
        .select([
            col("Symbol").alias("symbol"),
            col("Timestamp").alias("timestamp"),
            col("Open").alias("open"),
            col("High").alias("high"),
            col("Low").alias("low"),
            col("Close").alias("close"),
            col("Volume").cast(DataType::Float64).alias("volume"),
            col("VWAP").alias("vwap"),
        ])
        .collect()?;

    // Initialize strategies with default parameters
    let params1 = multi_indicator_minute_1::StrategyParams::default();
    let params2 = multi_indicator_minute_2::StrategyParams::default();
    let params3 = multi_indicator_minute_3::StrategyParams::default();

    // Run the first strategy
    println!("\nRunning Strategy 1 - Standard Multi-Indicator...");
    let signals1 = multi_indicator_minute_1::run_strategy(&df, &params1)?;
    let num_buy_signals1 = signals1.buy_signals.iter().filter(|&&s| s == 1).count();
    let num_sell_signals1 = signals1.sell_signals.iter().filter(|&&s| s == 1).count();
    println!(
        "Strategy 1 generated {} buy signals and {} sell signals",
        num_buy_signals1, num_sell_signals1
    );

    // Run the second strategy
    println!("\nRunning Strategy 2 - Volatility-Focused...");
    let signals2 = multi_indicator_minute_2::run_strategy(&df, &params2)?;
    let num_buy_signals2 = signals2.buy_signals.iter().filter(|&&s| s == 1).count();
    let num_sell_signals2 = signals2.sell_signals.iter().filter(|&&s| s == 1).count();
    println!(
        "Strategy 2 generated {} buy signals and {} sell signals",
        num_buy_signals2, num_sell_signals2
    );

    // Run the third strategy
    println!("\nRunning Strategy 3 - Momentum-Focused...");
    let signals3 = multi_indicator_minute_3::run_strategy(&df, &params3)?;
    let num_buy_signals3 = signals3.buy_signals.iter().filter(|&&s| s == 1).count();
    let num_sell_signals3 = signals3.sell_signals.iter().filter(|&&s| s == 1).count();
    println!(
        "Strategy 3 generated {} buy signals and {} sell signals",
        num_buy_signals3, num_sell_signals3
    );

    // Calculate performance metrics for all strategies
    let close_prices = df.column("close")?;

    // Performance for Strategy 1
    println!("\nCalculating performance for Strategy 1...");
    let (
        final_value1,
        total_return1,
        num_trades1,
        win_rate1,
        max_drawdown1,
        profit_factor1,
        avg_duration1,
    ) = multi_indicator_minute_1::calculate_performance(
        close_prices,
        &signals1.buy_signals,
        &signals1.sell_signals,
        10000.0, // Initial capital
        true,    // Close positions at end of day
    );

    // Performance for Strategy 2
    println!("Calculating performance for Strategy 2...");
    let (
        final_value2,
        total_return2,
        num_trades2,
        win_rate2,
        max_drawdown2,
        profit_factor2,
        sharpe_ratio2,
    ) = multi_indicator_minute_2::calculate_performance(
        close_prices,
        &signals2.buy_signals,
        &signals2.sell_signals,
        &signals2.position_sizes,
        10000.0, // Initial capital
        true,    // Close positions at end of day
        None,    // Default risk-free rate
    );

    // Performance for Strategy 3
    println!("Calculating performance for Strategy 3...");
    let (
        final_value3,
        total_return3,
        num_trades3,
        win_rate3,
        max_drawdown3,
        avg_return3,
        avg_holding3,
    ) = multi_indicator_minute_3::calculate_performance(
        close_prices,
        &signals3.buy_signals,
        &signals3.sell_signals,
        &signals3.stop_levels,
        &signals3.target_levels,
        10000.0, // Initial capital
        true,    // Include last day
    );

    // Print comparison results
    println!("\n--------------- STRATEGY COMPARISON RESULTS ---------------");
    println!("Metric                  | Strategy 1         | Strategy 2         | Strategy 3");
    println!(
        "------------------------|--------------------|--------------------|--------------------"
    );
    println!(
        "Final Portfolio Value   | ${:.2}           | ${:.2}           | ${:.2}",
        final_value1, final_value2, final_value3
    );
    println!(
        "Total Return            | {:.2}%             | {:.2}%             | {:.2}%",
        total_return1, total_return2, total_return3
    );
    println!(
        "Number of Trades        | {}                  | {}                  | {}",
        num_trades1, num_trades2, num_trades3
    );
    println!(
        "Win Rate                | {:.2}%             | {:.2}%             | {:.2}%",
        win_rate1, win_rate2, win_rate3
    );
    println!(
        "Maximum Drawdown        | {:.2}%             | {:.2}%             | {:.2}%",
        max_drawdown1 * 100.0,
        max_drawdown2 * 100.0,
        max_drawdown3 * 100.0
    );
    println!(
        "Performance Metric*     | {:.2}              | {:.2}              | {:.2}%/trade",
        profit_factor1, profit_factor2, avg_return3
    );
    println!(
        "Time Metric**           | {:.2} min          | {:.2} SR           | {:.2} min/trade",
        avg_duration1, sharpe_ratio2, avg_holding3
    );
    println!(
        "* Strategy 1: Profit Factor, Strategy 2: Profit Factor, Strategy 3: Avg Return per Trade"
    );
    println!("** Strategy 1: Avg Duration, Strategy 2: Sharpe Ratio, Strategy 3: Avg Holding Time");

    // Determine the better strategy
    let strategies = [
        (
            "Strategy 1 (Standard)",
            total_return1,
            win_rate1,
            max_drawdown1,
        ),
        (
            "Strategy 2 (Volatility)",
            total_return2,
            win_rate2,
            max_drawdown2,
        ),
        (
            "Strategy 3 (Momentum)",
            total_return3,
            win_rate3,
            max_drawdown3,
        ),
    ];

    let best_return = strategies
        .iter()
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap();

    let best_risk_adjusted = strategies
        .iter()
        .max_by(|a, b| {
            (a.1 / a.3)
                .partial_cmp(&(b.1 / b.3))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap();

    println!(
        "\nBest overall return: {} with {:.2}%",
        best_return.0, best_return.1
    );
    println!(
        "Best risk-adjusted return: {} with return/drawdown ratio of {:.2}",
        best_risk_adjusted.0,
        best_risk_adjusted.1 / (best_risk_adjusted.3 * 100.0)
    );

    // Print additional insights
    println!("\nStrategy Insights:");
    println!("- Strategy 1 (Standard Multi-Indicator): Optimized for trend-following with fast parameters");
    println!("  for intraday trading. Simple but effective approach using standard indicators.");

    println!("- Strategy 2 (Volatility-Focused): Adapts position sizing based on volatility,");
    println!("  taking smaller positions during high volatility periods to manage risk.");

    println!(
        "- Strategy 3 (Momentum-Focused): Uses time-based filters and targets momentum during"
    );
    println!("  specific trading sessions, with dynamic stop loss and take profit levels.");

    // Comparative analysis
    println!("\nComparative Analysis:");

    if num_trades3 < num_trades1
        && num_trades3 < num_trades2
        && win_rate3 > win_rate1
        && win_rate3 > win_rate2
    {
        println!("- Strategy 3 trades less frequently but has the highest win rate, suggesting");
        println!("  that its time-based filters and momentum criteria are effective at identifying quality trades.");
    }

    if max_drawdown2 < max_drawdown1 && max_drawdown2 < max_drawdown3 {
        println!(
            "- Strategy 2 has the lowest drawdown, showing that its volatility-based position"
        );
        println!("  sizing effectively reduces risk during turbulent market conditions.");
    }

    if avg_return3 > total_return1 / (num_trades1 as f64)
        && avg_return3 > total_return2 / (num_trades2 as f64)
    {
        println!(
            "- Strategy 3 has the highest return per trade, likely due to its risk management"
        );
        println!("  approach with pre-defined take profit levels based on ATR.");
    }

    Ok(())
}
