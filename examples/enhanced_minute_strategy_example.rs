//! Example: Enhanced Minute Multi-Indicator Strategy
//!
//! This example demonstrates how to use the enhanced minute-level multi-indicator strategy for intraday trading.
//!
//! - Loads minute-level OHLCV data from CSV
//! - Runs a multi-indicator strategy with risk management
//! - Calculates and prints performance metrics
//! - Saves all signals and indicators to `enhanced_minute_strategy_results.csv` for further analysis
//!
//! The output CSV contains all calculated indicators and trading signals for each row of the input data. You can use this file for further research, visualization, or strategy refinement.

use polars::prelude::*;
use ta_lib_in_rust::strategy::minute::enhanced_minute_strategy::{
    calculate_performance, run_strategy, StrategyParams,
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
            col("Timestamp").alias("timestamp").alias("date"), // Also alias as date for later use
            col("Open").alias("open"),
            col("High").alias("high"),
            col("Low").alias("low"),
            col("Close").alias("close"),
            col("Volume").cast(DataType::Float64).alias("volume"),
            col("VWAP").alias("vwap"),
        ])
        .collect()?;

    // Create strategy parameters with customized settings for intraday trading
    let params = StrategyParams {
        ema_fast_period: 5,    // Faster for minute data
        ema_slow_period: 13,   // Faster for minute data
        rsi_period: 7,         // Shorter for intraday
        rsi_overbought: 75.0,  // More extreme for minute data
        rsi_oversold: 25.0,    // More extreme for minute data
        williams_r_period: 10, // Shorter for minute data
        stoch_k_period: 9,     // Shorter for minute data
        stoch_d_period: 3,
        stoch_slowing: 3,
        psar_af_step: 0.02,
        psar_af_max: 0.2,
        atr_period: 10,             // Shorter for minute data
        atr_stop_multiplier: 1.5,   // Tighter stops for intraday
        atr_profit_multiplier: 2.0, // Smaller profit targets for intraday
        bb_period: 14,              // Shorter for minute data
        bb_std_dev: 2.0,
        mfi_period: 10,              // Shorter for minute data
        cmf_period: 14,              // Shorter for minute data
        min_buy_signals: 4,          // Require more signals for confidence
        min_sell_signals: 3,         // Easier to exit than enter
        use_volume_filter: true,     // Filter by volume
        volume_threshold: 1.2,       // Require above average volume
        use_time_filter: true,       // Apply time filters
        filter_morning_minutes: 15,  // Skip first 15 min after open
        filter_lunch_hour: true,     // Skip lunch hour (12-1 PM)
        filter_late_day_minutes: 15, // Skip last 15 min before close
    };

    println!("Running enhanced minute multi-indicator strategy...");
    let signals = run_strategy(&df, &params)?;

    // Calculate performance metrics
    let start_capital = 10000.0;
    let close_positions_eod = true;

    println!("Calculating performance metrics...");
    let (
        final_value,
        total_return,
        num_trades,
        win_rate,
        max_drawdown,
        profit_factor,
        avg_profit_per_trade,
    ) = calculate_performance(
        df.column("close")?,
        &signals.buy_signals,
        &signals.sell_signals,
        &signals.stop_levels,
        &signals.target_levels,
        start_capital,
        close_positions_eod,
    );

    // Print performance metrics
    println!("\n=== Performance Metrics ===");
    println!("Starting Capital: ${:.2}", start_capital);
    println!("Final Capital: ${:.2}", final_value);
    println!("Total Return: {:.2}%", total_return);
    println!("Number of Trades: {}", num_trades);
    println!("Win Rate: {:.2}%", win_rate);
    println!("Max Drawdown: {:.2}%", max_drawdown * 100.0);
    println!("Profit Factor: {:.2}", profit_factor);
    println!("Average Profit per Trade: {:.2}%", avg_profit_per_trade);

    // Save results for further analysis
    println!("\nSaving results to 'enhanced_minute_strategy_results.csv'...");
    let mut signals_df = signals.indicator_values;
    CsvWriter::new(std::io::BufWriter::new(std::fs::File::create(
        "enhanced_minute_strategy_results.csv",
    )?))
    .finish(&mut signals_df)?;

    // Print trade breakdown
    println!("\nTrade Summary:");
    let mut buy_count = 0;
    let mut sell_count = 0;

    for i in 0..signals.buy_signals.len() {
        if signals.buy_signals[i] == 1 {
            buy_count += 1;
        }
        if signals.sell_signals[i] == 1 {
            sell_count += 1;
        }
    }

    println!("Total Buy Signals: {}", buy_count);
    println!("Total Sell Signals: {}", sell_count);

    // Find the days with most active trading
    if df.schema().contains("date") {
        let date_counts = signals_df
            .lazy()
            .filter(col("buy_signal").eq(lit(1)))
            .group_by([col("date")])
            .agg([col("*").count().alias("trade_count")])
            .sort(["trade_count"], Default::default())
            .collect()?;

        println!("\nMost Active Trading Days (Buy Signals):");
        println!("{}", date_counts.tail(Some(5)));
    }

    Ok(())
}
