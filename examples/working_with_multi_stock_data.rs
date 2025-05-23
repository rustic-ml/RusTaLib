use polars::prelude::*;
use rustalib::indicators::{
    moving_averages::{calculate_ema, calculate_sma},
    oscillators::{calculate_macd, calculate_rsi},
    volatility::{calculate_atr, calculate_bollinger_bands},
    volume::calculate_obv,
};
use std::convert::TryInto;

/// This example demonstrates how to load and process data from multiple stock CSV files
/// that may have different column formats, and perform cross-stock comparisons.
fn main() -> Result<(), PolarsError> {
    // Define the tickers to analyze
    let tickers = vec!["AAPL", "GOOGL", "MSFT"];

    // Store results for comparison
    struct StockMetrics {
        ticker: String,
        avg_volatility: f64, // Average ATR / Close
        avg_volume: f64,     // Average volume
        rsi_latest: f64,     // Latest RSI value
        bb_width_avg: f64,   // Average Bollinger Band width
        obv_trend: f64,      // OBV change over period (%)
    }

    let mut all_metrics = Vec::new();

    println!("Analyzing technical indicators across multiple stocks...");

    // Process each ticker
    for ticker in &tickers {
        println!("\n=== Processing {} ===", ticker);

        // Load CSV data
        let file_path = format!("examples/csv/{}_daily_ohlcv.csv", ticker);
        println!("Loading data from {}", file_path);

        // Read first line to show raw data format
        let df_peek = CsvReadOptions::default()
            .with_has_header(false)
            .with_n_rows(Some(1))
            .try_into_reader_with_file_path(Some(file_path.clone().into()))?
            .finish()?;

        println!("Original columns: {:?}", df_peek.get_column_names());

        // Now read the actual data with proper column names
        let df = CsvReadOptions::default()
            .with_has_header(false)
            .try_into_reader_with_file_path(Some(file_path.into()))?
            .finish()?;

        // Rename columns manually
        let df = df
            .clone()
            .lazy()
            .select([
                col("column_1").alias("symbol"),
                col("column_2").alias("date"),
                col("column_3").alias("open"),
                col("column_4").alias("high"),
                col("column_5").alias("low"),
                col("column_6").alias("close"),
                col("column_7").cast(DataType::Float64).alias("volume"),
                col("column_8").alias("adjusted"),
            ])
            .collect()?;

        println!("Data shape: {} rows x {} columns", df.height(), df.width());

        // Handle column format differences by standardizing to lowercase
        let df = df
            .lazy()
            .select([
                col("symbol"),
                col("date"),
                col("open"),
                col("high"),
                col("low"),
                col("close"),
                col("volume"),
            ])
            .collect()?;

        // Calculate various technical indicators
        let sma_20 = calculate_sma(&df, "close", 20)?;
        let ema_20 = calculate_ema(&df, "close", 20)?;
        let rsi_14 = calculate_rsi(&df, 14, "close")?;
        let atr_14 = calculate_atr(&df, 14)?;
        let (bb_mid, bb_upper, bb_lower) = calculate_bollinger_bands(&df, 20, 2.0, "close")?;
        let obv = calculate_obv(&df)?;
        let (macd_line, macd_signal) = calculate_macd(&df, 12, 26, 9, "close")?;

        // Calculate Bollinger Band width as (Upper - Lower) / Middle
        let mut bb_width = Vec::with_capacity(df.height());
        let bb_mid_ca = bb_mid.f64()?;
        let bb_upper_ca = bb_upper.f64()?;
        let bb_lower_ca = bb_lower.f64()?;

        for i in 0..df.height() {
            let mid = bb_mid_ca.get(i).unwrap_or(f64::NAN);
            let upper = bb_upper_ca.get(i).unwrap_or(f64::NAN);
            let lower = bb_lower_ca.get(i).unwrap_or(f64::NAN);

            if mid.is_nan() || upper.is_nan() || lower.is_nan() || mid.abs() < 1e-10 {
                bb_width.push(f64::NAN);
            } else {
                bb_width.push((upper - lower) / mid);
            }
        }

        let bb_width_series = Series::new("bb_width".into(), bb_width);

        // Calculate volatility as ATR / Close
        let mut volatility = Vec::with_capacity(df.height());
        let close_ca = df.column("close")?.f64()?;
        let atr_ca = atr_14.f64()?;

        for i in 0..df.height() {
            let close = close_ca.get(i).unwrap_or(f64::NAN);
            let atr = atr_ca.get(i).unwrap_or(f64::NAN);

            if close.is_nan() || atr.is_nan() || close.abs() < 1e-10 {
                volatility.push(f64::NAN);
            } else {
                volatility.push(atr / close * 100.0); // As percentage
            }
        }

        let volatility_series = Series::new("volatility".into(), volatility);

        // Calculate metrics for this stock
        let start_idx: usize = 26; // Skip NaN values at the beginning
        let start_idx_i64: i64 = start_idx.try_into().unwrap(); // Convert to i64 for slice method
        let len = df.height().saturating_sub(start_idx);
        let last_idx = df.height().saturating_sub(1);

        // Get metrics before adding to DataFrame (to avoid ownership issues)
        let rsi_latest = if last_idx < rsi_14.len() {
            rsi_14.f64()?.get(last_idx).unwrap_or(0.0)
        } else {
            0.0
        };

        let bb_width_avg = bb_width_series
            .f64()?
            .slice(start_idx_i64, len)
            .mean()
            .unwrap_or(0.0);

        let avg_volatility = volatility_series
            .f64()?
            .slice(start_idx_i64, len)
            .mean()
            .unwrap_or(0.0);

        let avg_volume = df
            .column("volume")?
            .f64()?
            .slice(start_idx_i64, len)
            .mean()
            .unwrap_or(0.0);

        // Calculate OBV trend as percentage change
        let obv_ca = obv.f64()?;
        let obv_start = if start_idx < obv_ca.len() {
            obv_ca.get(start_idx).unwrap_or(0.0)
        } else {
            0.0
        };

        let obv_end = if last_idx < obv_ca.len() {
            obv_ca.get(last_idx).unwrap_or(0.0)
        } else {
            0.0
        };

        let obv_trend = if obv_start.abs() < 1e-10 {
            0.0
        } else {
            (obv_end - obv_start) / obv_start.abs() * 100.0
        };

        // Save metrics
        all_metrics.push(StockMetrics {
            ticker: ticker.to_string(),
            avg_volatility,
            avg_volume,
            rsi_latest,
            bb_width_avg,
            obv_trend,
        });

        // Print some key metrics for this stock
        println!(
            "Latest Close: ${:.2}",
            if last_idx < close_ca.len() {
                close_ca.get(last_idx).unwrap_or(0.0)
            } else {
                0.0
            }
        );
        println!("Latest RSI: {:.2}", rsi_latest);
        println!("Avg Volatility: {:.2}%", avg_volatility);
        println!("Avg BB Width: {:.4}", bb_width_avg);
        println!("OBV Trend: {:.2}%", obv_trend);

        // Create a new DataFrame with all indicators
        let mut df_with_indicators = df.clone();

        // Helper function to add columns safely, without shape mismatch errors
        let add_column_safely = |df: &mut DataFrame, series: Series| -> Result<(), PolarsError> {
            // Only add the column if it has the same length as the DataFrame
            if series.len() == df.height() {
                df.with_column(series)?;
            }
            Ok(())
        };

        // Add columns safely
        add_column_safely(&mut df_with_indicators, sma_20.clone())?;
        add_column_safely(&mut df_with_indicators, ema_20.clone())?;
        add_column_safely(&mut df_with_indicators, rsi_14.clone())?;
        add_column_safely(&mut df_with_indicators, atr_14.clone())?;
        add_column_safely(&mut df_with_indicators, bb_mid.clone())?;
        add_column_safely(&mut df_with_indicators, bb_upper.clone())?;
        add_column_safely(&mut df_with_indicators, bb_lower.clone())?;
        add_column_safely(&mut df_with_indicators, bb_width_series.clone())?;
        add_column_safely(&mut df_with_indicators, volatility_series.clone())?;
        add_column_safely(&mut df_with_indicators, obv.clone())?;
        add_column_safely(&mut df_with_indicators, macd_line.clone())?;
        add_column_safely(&mut df_with_indicators, macd_signal.clone())?;

        // Save indicators to CSV for further analysis
        let output_path = format!("examples/csv/{}_indicators.csv", ticker);
        println!("Saving indicators to {}", output_path);

        CsvWriter::new(std::io::BufWriter::new(std::fs::File::create(output_path)?))
            .finish(&mut df_with_indicators)?;
    }

    // Cross-stock comparison
    println!("\n=== Cross-Stock Comparison ===");
    println!(
        "{:<6} {:<14} {:<14} {:<10} {:<12} {:<10}",
        "Ticker", "Avg Volatility", "Avg Volume", "RSI", "BB Width", "OBV Trend"
    );
    println!("------------------------------------------------------------------");

    for metrics in &all_metrics {
        println!(
            "{:<6} {:<14.2}% {:<14.0} {:<10.2} {:<12.4} {:<10.2}%",
            metrics.ticker,
            metrics.avg_volatility,
            metrics.avg_volume,
            metrics.rsi_latest,
            metrics.bb_width_avg,
            metrics.obv_trend
        );
    }

    // Find highest/lowest metric values
    let highest_volatility = all_metrics
        .iter()
        .max_by(|a, b| {
            a.avg_volatility
                .partial_cmp(&b.avg_volatility)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap();

    let highest_volume = all_metrics
        .iter()
        .max_by(|a, b| {
            a.avg_volume
                .partial_cmp(&b.avg_volume)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap();

    let highest_obv_trend = all_metrics
        .iter()
        .max_by(|a, b| {
            a.obv_trend
                .partial_cmp(&b.obv_trend)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap();

    println!("\n=== Analysis Highlights ===");
    println!(
        "- Highest volatility: {} at {:.2}%",
        highest_volatility.ticker, highest_volatility.avg_volatility
    );
    println!(
        "- Highest volume: {} with an average of {:.0} shares",
        highest_volume.ticker, highest_volume.avg_volume
    );
    println!(
        "- Strongest OBV trend: {} at {:.2}%",
        highest_obv_trend.ticker, highest_obv_trend.obv_trend
    );

    Ok(())
}
