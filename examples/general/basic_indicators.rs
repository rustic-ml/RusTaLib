use polars::prelude::*;
use ta_lib_in_rust::indicators::{
    moving_averages::{calculate_sma, calculate_ema, calculate_wma},
    oscillators::{calculate_rsi, calculate_macd},
    volatility::calculate_bollinger_bands,
};

fn main() -> Result<(), PolarsError> {
    // Create a simple price series with some movement
    let close_prices = Series::new(
        "close".into(),
        &[
            100.0, 102.0, 104.0, 103.0, 105.0, 107.0, 108.0, 107.0, 105.0, 103.0, 
            101.0, 99.0, 97.0, 95.0, 94.0, 96.0, 98.0, 100.0, 102.0, 104.0,
            105.0, 107.0, 109.0, 108.0, 110.0, 112.0, 111.0, 109.0, 107.0, 105.0,
        ],
    );

    // Create DataFrame with price data
    let df = DataFrame::new(vec![close_prices.clone().into()])?;

    println!("Basic Technical Indicators Example\n");

    // Calculate and display moving averages
    println!("\n----- Moving Averages -----");
    let sma_10 = calculate_sma(&df, "close", 10)?;
    let ema_10 = calculate_ema(&df, "close", 10)?;
    let wma_10 = calculate_wma(&df, "close", 10)?;
    
    println!("SMA(10): {}", sma_10.f64()?.get(15).unwrap_or(f64::NAN));
    println!("EMA(10): {}", ema_10.f64()?.get(15).unwrap_or(f64::NAN));
    println!("WMA(10): {}", wma_10.f64()?.get(15).unwrap_or(f64::NAN));

    // Calculate and display RSI
    println!("\n----- Relative Strength Index -----");
    let rsi_14 = calculate_rsi(&df, 14, "close")?;
    println!("RSI(14): {}", rsi_14.f64()?.get(20).unwrap_or(f64::NAN));
    println!("RSI Interpretation:");
    println!("  Over 70: Potentially overbought");
    println!("  Under 30: Potentially oversold");

    // Calculate and display Bollinger Bands
    println!("\n----- Bollinger Bands -----");
    let (bb_middle, bb_upper, bb_lower) = calculate_bollinger_bands(&df, 20, 2.0, "close")?;
    println!("BB Middle(20,2): {}", bb_middle.f64()?.get(25).unwrap_or(f64::NAN));
    println!("BB Upper(20,2): {}", bb_upper.f64()?.get(25).unwrap_or(f64::NAN));
    println!("BB Lower(20,2): {}", bb_lower.f64()?.get(25).unwrap_or(f64::NAN));

    // Calculate and display MACD
    println!("\n----- MACD -----");
    let (macd_line, signal_line) = calculate_macd(&df, 12, 26, 9, "close")?;
    println!("MACD Line: {}", macd_line.f64()?.get(28).unwrap_or(f64::NAN));
    println!("Signal Line: {}", signal_line.f64()?.get(28).unwrap_or(f64::NAN));
    println!("Histogram: {}", (macd_line.f64()? - signal_line.f64()?).get(28).unwrap_or(f64::NAN));
    println!("MACD Interpretation:");
    println!("  MACD Line crosses above Signal Line: Bullish signal");
    println!("  MACD Line crosses below Signal Line: Bearish signal");

    println!("\nThis example demonstrates basic technical indicators calculation.");
    println!("In a real trading strategy, you would combine multiple indicators and apply proper risk management.");

    Ok(())
} 