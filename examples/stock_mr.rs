// Example: Mean Reversion Strategy with simplified approach
// This example demonstrates a simple mean reversion strategy

use polars::prelude::*;

fn main() -> Result<(), PolarsError> {
    println!("Mean Reversion Strategy Example");
    println!("===============================\n");

    // Create sample dates - 20 days
    let mut dates = Vec::with_capacity(20);
    for day in 1..=20 {
        dates.push(format!("2023-01-{:02}", day));
    }

    // Create sample price data with mean-reverting pattern
    let mut closes = Vec::with_capacity(20);
    let mut price = 100.0;

    // First 10 days - uptrend
    for _ in 0..10 {
        price += 2.0 + (rand() * 0.5);
        closes.push(price);
    }

    // Next 10 days - downtrend (mean reversion)
    for _ in 10..20 {
        price -= 1.5 + (rand() * 0.5);
        closes.push(price);
    }

    // Create DataFrame
    let mut df = DataFrame::new(vec![
        Series::new("date".into(), dates).into(),
        Series::new("close".into(), closes.clone()).into(),
    ])?;

    println!("Price data created with {} days", df.height());

    // Calculate SMA (simple moving average)
    let window_size = 5;
    let mut sma_vals = vec![f64::NAN; closes.len()];

    for i in window_size - 1..closes.len() {
        let mut sum = 0.0;
        for j in 0..window_size {
            sum += closes[i - j];
        }
        sma_vals[i] = sum / window_size as f64;
    }

    // Calculate Bollinger Bands
    let mut upper_band = vec![f64::NAN; closes.len()];
    let mut lower_band = vec![f64::NAN; closes.len()];

    for i in window_size - 1..closes.len() {
        let sma = sma_vals[i];
        let mut sum_sq_dev = 0.0;
        for j in 0..window_size {
            let dev = closes[i - j] - sma;
            sum_sq_dev += dev * dev;
        }
        let std_dev = (sum_sq_dev / window_size as f64).sqrt();
        upper_band[i] = sma + 2.0 * std_dev;
        lower_band[i] = sma - 2.0 * std_dev;
    }

    // Calculate RSI
    let rsi_period = 5;
    let mut gains = vec![0.0; closes.len()];
    let mut losses = vec![0.0; closes.len()];
    let mut rsi_vals = vec![f64::NAN; closes.len()];

    for i in 1..closes.len() {
        let change = closes[i] - closes[i - 1];
        if change > 0.0 {
            gains[i] = change;
        } else {
            losses[i] = -change;
        }
    }

    for i in rsi_period..closes.len() {
        let avg_gain: f64 = gains[i - rsi_period + 1..=i].iter().sum::<f64>() / rsi_period as f64;
        let avg_loss: f64 = losses[i - rsi_period + 1..=i].iter().sum::<f64>() / rsi_period as f64;

        if avg_loss == 0.0 {
            rsi_vals[i] = 100.0;
        } else {
            let rs = avg_gain / avg_loss;
            rsi_vals[i] = 100.0 - (100.0 / (1.0 + rs));
        }
    }

    // Calculate Z-score
    let mut z_scores = vec![f64::NAN; closes.len()];

    for i in 0..closes.len() {
        if !sma_vals[i].is_nan() && !upper_band[i].is_nan() && !lower_band[i].is_nan() {
            let band_width = upper_band[i] - lower_band[i];
            if band_width > 0.0 {
                z_scores[i] = (closes[i] - sma_vals[i]) / (band_width / 2.0);
            }
        }
    }

    // Generate trading signals
    let mut signals = vec![0; closes.len()];

    for i in 0..closes.len() {
        if !z_scores[i].is_nan() && !rsi_vals[i].is_nan() {
            if z_scores[i] <= -1.5 && rsi_vals[i] <= 30.0 {
                signals[i] = 1; // Buy signal
            } else if z_scores[i] >= 1.5 && rsi_vals[i] >= 70.0 {
                signals[i] = -1; // Sell signal
            }
        }
    }

    // Add calculated columns to DataFrame
    df.with_column(Series::new("sma".into(), sma_vals))?;
    df.with_column(Series::new("upper_band".into(), upper_band))?;
    df.with_column(Series::new("lower_band".into(), lower_band))?;
    df.with_column(Series::new("rsi".into(), rsi_vals))?;
    df.with_column(Series::new("z_score".into(), z_scores))?;

    // Clone signals before adding to DataFrame
    let signals_copy = signals.clone();
    df.with_column(Series::new("signal".into(), signals))?;

    // Print results
    println!("\nMean Reversion Analysis:");
    println!("{}", df);

    // Count signals
    let buy_signals = signals_copy.iter().filter(|&&s| s == 1).count();
    let sell_signals = signals_copy.iter().filter(|&&s| s == -1).count();

    println!(
        "\nTrading signals found: {} buy, {} sell",
        buy_signals, sell_signals
    );

    // Show strategy explanation
    println!("\nMean Reversion Strategy Explanation:");
    println!("1. Z-Score measures how far a price has deviated from its mean");
    println!("   in terms of standard deviations.");
    println!("2. Strong buy signals occur when Z-Score < -1.5 (oversold) and RSI < 30.");
    println!("3. Strong sell signals occur when Z-Score > 1.5 (overbought) and RSI > 70.");
    println!("4. Bollinger Bands help visualize the mean and standard deviation boundaries.");
    println!("5. This strategy aims to profit from price movements returning to the mean.");

    Ok(())
}

// Simple random number generator
fn rand() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as f64;
    (t.sin() + 1.0) / 2.0
}
