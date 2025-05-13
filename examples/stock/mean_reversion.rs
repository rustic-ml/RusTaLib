// Example: Mean Reversion Strategy
// This example demonstrates a simple mean reversion strategy using technical indicators

use polars::prelude::*;
use rustalib::indicators::moving_averages::calculate_sma;
use rustalib::indicators::oscillators::calculate_rsi;
use rustalib::indicators::volatility::calculate_bollinger_bands;

fn main() -> Result<(), PolarsError> {
    println!("Mean Reversion Strategy Example");
    println!("===============================\n");

    // Create sample price data
    let dates = Series::new(
        "date".into(),
        &[
            "2023-01-01",
            "2023-01-02",
            "2023-01-03",
            "2023-01-04",
            "2023-01-05",
            "2023-01-06",
            "2023-01-07",
            "2023-01-08",
            "2023-01-09",
            "2023-01-10",
            "2023-01-11",
            "2023-01-12",
            "2023-01-13",
            "2023-01-14",
            "2023-01-15",
            "2023-01-16",
            "2023-01-17",
            "2023-01-18",
            "2023-01-19",
            "2023-01-20",
            "2023-01-21",
            "2023-01-22",
            "2023-01-23",
            "2023-01-24",
            "2023-01-25",
            "2023-01-26",
            "2023-01-27",
            "2023-01-28",
            "2023-01-29",
            "2023-01-30",
            "2023-01-31",
            "2023-02-01",
            "2023-02-02",
            "2023-02-03",
            "2023-02-04",
            "2023-02-05",
            "2023-02-06",
            "2023-02-07",
            "2023-02-08",
            "2023-02-09",
            "2023-02-10",
            "2023-02-11",
            "2023-02-12",
            "2023-02-13",
            "2023-02-14",
            "2023-02-15",
            "2023-02-16",
            "2023-02-17",
            "2023-02-18",
            "2023-02-19",
            "2023-02-20",
            "2023-02-21",
            "2023-02-22",
            "2023-02-23",
            "2023-02-24",
            "2023-02-25",
            "2023-02-26",
            "2023-02-27",
            "2023-02-28",
            "2023-03-01",
            "2023-03-02",
            "2023-03-03",
            "2023-03-04",
            "2023-03-05",
            "2023-03-06",
            "2023-03-07",
            "2023-03-08",
            "2023-03-09",
            "2023-03-10",
        ],
    );

    // Count the number of dates for consistency checking
    let date_count = dates.len();
    println!("Number of dates: {}", date_count);

    // Create price data with a mean-reverting pattern
    // First a steady trend, then a spike up, then a return to the mean
    let opens = Series::new(
        "open".into(),
        &[
            100.0, 101.0, 102.0, 103.0, 102.5, 101.8, 102.3, 103.1, 104.2, 103.8, 103.5, 104.1,
            104.8, 105.2, 105.8, 106.3, 107.0, 106.5, 107.2, 107.8, 108.3, 109.0, 109.5, 110.2,
            110.8, 111.5, 112.0, 112.8, 113.5, 114.0, 114.5, 115.0, 115.5, 116.0, 116.5, 117.0,
            117.5, 118.0, 119.0, 121.0, 124.0, 127.0, 130.0, 132.0, 131.0, 129.0, 127.0, 125.0,
            123.0, 122.0, 121.0, 120.0, 118.5, 117.0, 116.0, 115.5, 115.0, 114.5, 114.0, 113.5,
            113.0, 112.5, 112.0, 111.5, 111.0, 110.5, 110.0, 109.5, 109.0,
        ],
    );

    let highs = Series::new(
        "high".into(),
        &[
            101.5, 102.5, 103.0, 103.5, 103.0, 102.5, 103.5, 104.5, 105.0, 104.5, 104.0, 105.0,
            105.5, 106.0, 106.5, 107.5, 108.0, 107.5, 108.0, 108.5, 109.5, 110.0, 110.5, 111.0,
            111.5, 112.5, 113.0, 113.5, 114.5, 115.0, 115.5, 116.0, 116.5, 117.0, 117.5, 118.0,
            118.5, 119.5, 121.0, 123.0, 126.0, 129.0, 132.0, 133.0, 132.0, 130.0, 128.0, 126.0,
            124.0, 123.0, 122.0, 121.0, 119.5, 118.0, 117.0, 116.5, 116.0, 115.5, 115.0, 114.5,
            114.0, 113.5, 113.0, 112.5, 112.0, 111.5, 111.0, 110.5, 110.0,
        ],
    );

    let lows = Series::new(
        "low".into(),
        &[
            99.5, 100.5, 101.0, 102.0, 101.5, 101.0, 102.0, 102.5, 103.0, 103.0, 102.5, 103.0,
            104.0, 104.5, 105.0, 106.0, 106.0, 106.0, 106.5, 107.0, 108.0, 108.5, 109.0, 109.5,
            110.0, 111.0, 111.5, 112.0, 113.0, 113.5, 114.0, 114.5, 115.0, 115.5, 116.0, 116.5,
            117.0, 117.5, 118.0, 120.0, 123.0, 126.0, 129.0, 130.0, 128.0, 126.0, 124.0, 122.0,
            121.0, 120.0, 119.0, 118.0, 117.0, 116.0, 115.0, 114.5, 114.0, 113.5, 113.0, 112.5,
            112.0, 111.5, 111.0, 110.5, 110.0, 109.5, 109.0, 108.5, 108.0,
        ],
    );

    let closes = Series::new(
        "close".into(),
        &[
            101.0, 102.0, 103.0, 102.5, 101.8, 102.3, 103.1, 104.2, 103.8, 103.5, 104.1, 104.8,
            105.2, 105.8, 106.3, 107.0, 106.5, 107.2, 107.8, 108.3, 109.0, 109.5, 110.2, 110.8,
            111.5, 112.0, 112.8, 113.5, 114.0, 114.5, 115.0, 115.5, 116.0, 116.5, 117.0, 117.5,
            118.0, 119.0, 121.0, 123.0, 126.0, 129.0, 132.0, 131.0, 129.0, 127.0, 125.0, 123.0,
            122.0, 121.0, 120.0, 118.5, 117.0, 116.0, 115.5, 115.0, 114.5, 114.0, 113.5, 113.0,
            112.5, 112.0, 111.5, 111.0, 110.5, 110.0, 109.5, 109.0, 108.5,
        ],
    );

    let volumes = Series::new(
        "volume".into(),
        &[
            150000, 155000, 160000, 158000, 152000, 148000, 155000, 165000, 160000, 152000, 158000,
            162000, 170000, 172000, 175000, 180000, 178000, 182000, 185000, 188000, 190000, 195000,
            198000, 200000, 205000, 210000, 215000, 220000, 225000, 230000, 235000, 240000, 245000,
            250000, 255000, 260000, 265000, 270000, 280000, 300000, 350000, 400000, 450000, 500000,
            400000, 350000, 300000, 275000, 250000, 225000, 200000, 190000, 180000, 170000, 165000,
            160000, 155000, 150000, 145000, 140000, 135000, 130000, 125000, 120000, 115000, 110000,
            105000, 100000, 95000,
        ],
    );

    // Print counts to debug the length mismatch
    println!("Data shape check:");
    println!("  Date count: {}", dates.len());
    println!("  Open count: {}", opens.len());
    println!("  High count: {}", highs.len());
    println!("  Low count: {}", lows.len());
    println!("  Close count: {}", closes.len());
    println!("  Volume count: {}", volumes.len());

    // Create DataFrame
    let mut df = DataFrame::new(vec![
        dates.into(),
        opens.into(),
        highs.into(),
        lows.into(),
        closes.clone().into(),
        volumes.into(),
    ])?;

    // Calculate indicators
    // 1. Calculate SMA
    let sma_20 = calculate_sma(&df, "close", 20)?;
    println!(
        "SMA length: {}, DataFrame height: {}",
        sma_20.len(),
        df.height()
    );
    df.with_column(sma_20)?;

    // 2. Calculate Bollinger Bands
    let (middle, upper, lower) = calculate_bollinger_bands(&df, 20, 2.0, "close")?;
    println!("Middle Band length: {}, Upper Band length: {}, Lower Band length: {}, DataFrame height: {}", 
             middle.len(), upper.len(), lower.len(), df.height());
    df.with_column(middle)?;
    df.with_column(upper)?;
    df.with_column(lower)?;

    // 3. Calculate RSI
    let rsi = calculate_rsi(&df, 14, "close")?;
    println!(
        "RSI length: {}, DataFrame height: {}",
        rsi.len(),
        df.height()
    );

    // Fix RSI length issue by adding a proper column to match the length
    if rsi.len() < df.height() {
        // Get RSI values
        let rsi_values = rsi.f64()?.to_vec();

        // Create a new vector with leading NaN values to match DataFrame height
        let mut padded_rsi_values = Vec::with_capacity(df.height());

        // Add (df.height() - rsi.len()) NaN values at the beginning
        for _ in 0..(df.height() - rsi.len()) {
            padded_rsi_values.push(None);
        }

        // Add the actual RSI values
        for val in rsi_values {
            padded_rsi_values.push(val);
        }

        // Create and add the new RSI series
        let padded_rsi = Series::new("rsi".into(), padded_rsi_values);
        println!(
            "Padded RSI length: {}, DataFrame height: {}",
            padded_rsi.len(),
            df.height()
        );
        df.with_column(padded_rsi)?;
    } else {
        // Rename RSI column to ensure consistency
        let rsi_values = rsi.f64()?.to_vec();
        let renamed_rsi = Series::new("rsi".into(), rsi_values);
        df.with_column(renamed_rsi)?;
    }

    // 4. Calculate Z-Score (a simpler measure of mean reversion)
    calculate_z_score(&mut df, 20)?;

    // 5. Generate mean reversion signals
    calculate_mean_reversion_signals(&mut df)?;

    // Display results for the last 15 days
    let result_df = df.tail(Some(15));
    println!("Mean Reversion Analysis (Last 15 days):");
    println!(
        "{}",
        result_df.select([
            "date",
            "close",
            "bb_middle",
            "bb_upper",
            "bb_lower",
            "rsi",
            "z_score",
            "signal"
        ])?
    );

    // Analyze signals
    println!("\nMean Reversion Trading Signals:");

    // Filter for non-zero signals using a simple expression
    let gt_zero = df.column("signal")?.i32()?.gt(0);
    let lt_zero = df.column("signal")?.i32()?.lt(0);
    let signals_expr = gt_zero | lt_zero;
    let signals_df = df.filter(&signals_expr)?;
    if signals_df.height() > 0 {
        println!("Found {} trading signals:", signals_df.height());
        println!(
            "{}",
            signals_df.select(["date", "close", "z_score", "rsi", "signal"])?
        );
    } else {
        println!("No mean reversion signals found in this period.");
    }

    // Educational explanation
    println!("\nMean Reversion Strategy Explanation:");
    println!("1. Z-Score measures how far a price has deviated from its mean");
    println!("   in terms of standard deviations.");
    println!("2. Strong buy signals occur when Z-Score < -2.0 (oversold) and RSI < 30.");
    println!("3. Strong sell signals occur when Z-Score > 2.0 (overbought) and RSI > 70.");
    println!("4. Bollinger Bands help visualize the mean and standard deviation boundaries.");
    println!("5. This strategy aims to profit from price movements returning to the mean.");

    Ok(())
}

// Calculate Z-Score (number of standard deviations from the mean)
fn calculate_z_score(df: &mut DataFrame, window: usize) -> Result<(), PolarsError> {
    // Get the close prices
    let close = df.column("close")?.f64()?.clone();
    let height = df.height();

    // Create vector to store results with exact capacity
    let mut z_scores = Vec::with_capacity(height);

    // Pre-fill with NaNs to match DataFrame height
    for i in 0..height {
        if i < window {
            z_scores.push(f64::NAN);
        } else {
            let window_slice = close.slice((i - window) as i64, window);
            let window_vec: Vec<f64> = window_slice.iter().filter_map(|x| x).collect();

            if window_vec.is_empty() {
                z_scores.push(f64::NAN);
                continue;
            }

            // Calculate mean
            let mean: f64 = window_vec.iter().sum::<f64>() / window_vec.len() as f64;

            // Calculate standard deviation
            let variance: f64 = window_vec.iter().map(|&x| (x - mean).powi(2)).sum::<f64>()
                / window_vec.len() as f64;
            let std_dev = variance.sqrt();

            // Calculate z-score
            if let Some(current_price) = close.get(i) {
                if std_dev > 0.0 {
                    z_scores.push((current_price - mean) / std_dev);
                } else {
                    z_scores.push(0.0);
                }
            } else {
                z_scores.push(f64::NAN);
            }
        }
    }

    // Ensure z_scores has exactly `height` elements
    while z_scores.len() < height {
        z_scores.push(f64::NAN); // Pad with NaN if needed
    }

    // If somehow we got too many elements (shouldn't happen), truncate
    if z_scores.len() > height {
        z_scores.truncate(height);
    }

    // Add the z-scores to the dataframe
    println!(
        "Z-score length: {}, DataFrame height: {}",
        z_scores.len(),
        height
    );
    df.with_column(Series::new("z_score".into(), z_scores))?;

    Ok(())
}

// Generate mean reversion signals based on z-score and RSI
fn calculate_mean_reversion_signals(df: &mut DataFrame) -> Result<(), PolarsError> {
    // Get the data we need
    let z_score = df.column("z_score")?.f64()?;
    let rsi = df.column("rsi")?.f64()?;
    let height = df.height();

    // Create vector to store signals with exact capacity
    let mut signals = Vec::with_capacity(height);

    // Process each row
    for i in 0..height {
        let z = z_score.get(i).unwrap_or(f64::NAN);
        let r = rsi.get(i).unwrap_or(f64::NAN);

        if z.is_nan() || r.is_nan() {
            signals.push(0);
        } else if z <= -2.0 && r <= 30.0 {
            // Oversold condition - buy signal
            signals.push(1);
        } else if z >= 2.0 && r >= 70.0 {
            // Overbought condition - sell signal
            signals.push(-1);
        } else {
            signals.push(0);
        }
    }

    // Make sure signals has the right length
    println!(
        "Signals length: {}, DataFrame height: {}",
        signals.len(),
        height
    );

    // Add signals to dataframe
    df.with_column(Series::new("signal".into(), signals))?;

    Ok(())
}
