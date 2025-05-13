// Example: Trend Following Strategy
// This example demonstrates a simple trend following strategy using technical indicators

use polars::prelude::*;
use rustalib::indicators::moving_averages::calculate_sma;
use rustalib::indicators::oscillators::{calculate_macd, calculate_rsi};
use rustalib::indicators::trend::calculate_adx;

fn main() -> Result<(), PolarsError> {
    println!("Trend Following Strategy Example");
    println!("===============================\n");

    // Create sample price data for a trending market (uptrend then downtrend)
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

    // Create price data with trending patterns
    // First a steady uptrend, then consolidation, then a downtrend
    let opens = Series::new(
        "open".into(),
        &[
            100.0, 101.5, 103.0, 104.2, 105.7, 107.0, 108.2, 109.6, 111.2, 112.5, 113.8, 115.0,
            116.5, 118.0, 119.3, 120.6, 122.0, 123.5, 124.8, 126.0, 127.2, 128.5, 129.7, 131.0,
            132.2, 133.3, 134.5, 135.6, 136.8, 138.0, 139.0, 139.8, 140.5, 141.2, 141.8, 142.5,
            142.9, 143.5, 143.8, 144.0, 144.2, 143.9, 144.1, 143.8, 143.5, 142.9, 142.0, 141.2,
            140.3, 139.0, 137.5, 136.0, 134.5, 133.0, 131.5, 130.0, 128.5, 127.0, 125.5, 123.5,
            121.5, 120.0, 118.3, 116.5, 115.0, 113.5, 112.0, 110.5, 109.0,
        ],
    );

    let highs = Series::new(
        "high".into(),
        &[
            102.0, 103.5, 105.0, 106.2, 107.7, 109.0, 110.2, 111.6, 113.2, 114.5, 115.8, 117.0,
            118.5, 120.0, 121.3, 122.6, 124.0, 125.5, 126.8, 128.0, 129.2, 130.5, 131.7, 133.0,
            134.2, 135.3, 136.5, 137.6, 138.8, 140.0, 141.0, 141.8, 142.5, 143.2, 143.8, 144.5,
            144.9, 145.5, 145.8, 146.0, 146.2, 145.9, 146.1, 145.8, 145.5, 144.9, 144.0, 143.2,
            142.3, 141.0, 139.5, 138.0, 136.5, 135.0, 133.5, 132.0, 130.5, 129.0, 127.5, 125.5,
            123.5, 122.0, 120.3, 118.5, 117.0, 115.5, 114.0, 112.5, 111.0,
        ],
    );

    let lows = Series::new(
        "low".into(),
        &[
            99.0, 100.5, 102.0, 103.2, 104.7, 106.0, 107.2, 108.6, 110.2, 111.5, 112.8, 114.0,
            115.5, 117.0, 118.3, 119.6, 121.0, 122.5, 123.8, 125.0, 126.2, 127.5, 128.7, 130.0,
            131.2, 132.3, 133.5, 134.6, 135.8, 137.0, 138.0, 138.8, 139.5, 140.2, 140.8, 141.5,
            141.9, 142.5, 142.8, 143.0, 143.2, 142.9, 143.1, 142.8, 142.5, 141.9, 141.0, 140.2,
            139.3, 138.0, 136.5, 135.0, 133.5, 132.0, 130.5, 129.0, 127.5, 126.0, 124.5, 122.5,
            120.5, 119.0, 117.3, 115.5, 114.0, 112.5, 111.0, 109.5, 108.0,
        ],
    );

    let closes = Series::new(
        "close".into(),
        &[
            101.5, 103.0, 104.2, 105.7, 107.0, 108.2, 109.6, 111.2, 112.5, 113.8, 115.0, 116.5,
            118.0, 119.3, 120.6, 122.0, 123.5, 124.8, 126.0, 127.2, 128.5, 129.7, 131.0, 132.2,
            133.3, 134.5, 135.6, 136.8, 138.0, 139.0, 139.8, 140.5, 141.2, 141.8, 142.5, 142.9,
            143.5, 143.8, 144.0, 144.2, 143.9, 144.1, 143.8, 143.5, 142.9, 142.0, 141.2, 140.3,
            139.0, 137.5, 136.0, 134.5, 133.0, 131.5, 130.0, 128.5, 127.0, 125.5, 123.5, 121.5,
            120.0, 118.3, 116.5, 115.0, 113.5, 112.0, 110.5, 109.0, 107.5,
        ],
    );

    let volumes = Series::new(
        "volume".into(),
        &[
            150000, 160000, 165000, 170000, 175000, 180000, 185000, 190000, 195000, 200000, 205000,
            210000, 215000, 220000, 225000, 230000, 235000, 240000, 245000, 250000, 255000, 260000,
            265000, 270000, 275000, 280000, 285000, 290000, 295000, 300000, 310000, 320000, 330000,
            340000, 350000, 360000, 370000, 380000, 390000, 400000, 410000, 420000, 430000, 440000,
            450000, 460000, 470000, 480000, 490000, 500000, 490000, 480000, 470000, 460000, 450000,
            440000, 430000, 420000, 410000, 400000, 390000, 380000, 370000, 360000, 350000, 340000,
            330000, 320000, 310000,
        ],
    );

    // Print counts to debug any length mismatch
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

    // Calculate indicators for trend following
    // 1. Calculate short and long SMAs
    let sma_20 = calculate_sma(&df, "close", 20)?;
    let sma_50 = calculate_sma(&df, "close", 50)?;

    // Rename the SMAs for clarity
    let sma_20_values = sma_20.f64()?.to_vec();
    let sma_50_values = sma_50.f64()?.to_vec();

    // Add to dataframe
    df.with_column(Series::new("sma_20".into(), sma_20_values))?;
    df.with_column(Series::new("sma_50".into(), sma_50_values))?;

    // 2. Calculate MACD
    let (macd_line, signal_line) = calculate_macd(&df, 12, 26, 9, "close")?;

    // Add MACD and signal line to the dataframe
    df.with_column(macd_line.with_name("macd".into()))?;
    df.with_column(signal_line.with_name("macd_signal".into()))?;

    // Calculate MACD histogram
    // First get the values as f64 arrays
    let macd_vals = df.column("macd")?.f64()?;
    let signal_vals = df.column("macd_signal")?.f64()?;

    // Then calculate the difference
    let mut histogram = Vec::with_capacity(macd_vals.len());
    for i in 0..macd_vals.len() {
        let macd = macd_vals.get(i).unwrap_or(f64::NAN);
        let signal = signal_vals.get(i).unwrap_or(f64::NAN);
        histogram.push(macd - signal);
    }

    // Add the histogram to the dataframe
    df.with_column(Series::new("macd_histogram".into(), histogram))?;

    // 3. Calculate ADX (Average Directional Index) for trend strength
    let adx = calculate_adx(&df, 14)?;

    // Handle potential length issues with ADX
    if adx.len() < df.height() {
        let adx_values = adx.f64()?.to_vec();
        let mut padded_adx = Vec::with_capacity(df.height());

        // Add padding at the beginning
        for _ in 0..(df.height() - adx.len()) {
            padded_adx.push(None);
        }

        // Add actual values
        for val in adx_values {
            padded_adx.push(val);
        }

        df.with_column(Series::new("adx".into(), padded_adx))?;
    } else {
        let adx_values = adx.f64()?.to_vec();
        df.with_column(Series::new("adx".into(), adx_values))?;
    }

    // 4. Calculate RSI
    let rsi = calculate_rsi(&df, 14, "close")?;

    // Handle potential length issues with RSI
    if rsi.len() < df.height() {
        let rsi_values = rsi.f64()?.to_vec();
        let mut padded_rsi = Vec::with_capacity(df.height());

        // Add padding at the beginning
        for _ in 0..(df.height() - rsi.len()) {
            padded_rsi.push(None);
        }

        // Add actual values
        for val in rsi_values {
            padded_rsi.push(val);
        }

        df.with_column(Series::new("rsi".into(), padded_rsi))?;
    } else {
        let rsi_values = rsi.f64()?.to_vec();
        df.with_column(Series::new("rsi".into(), rsi_values))?;
    }

    // 5. Generate trend signals
    calculate_trend_signals(&mut df)?;

    // Display results for the last 15 days
    let result_df = df.tail(Some(15));
    println!("Trend Following Analysis (Last 15 days):");
    println!(
        "{}",
        result_df.select([
            "date",
            "close",
            "sma_20",
            "sma_50",
            "macd",
            "macd_signal",
            "adx",
            "signal"
        ])?
    );

    // Analyze signals
    println!("\nTrend Following Trading Signals:");

    // Filter for non-zero signals
    let gt_zero = df.column("signal")?.i32()?.gt(0);
    let lt_zero = df.column("signal")?.i32()?.lt(0);
    let signals_expr = gt_zero | lt_zero;
    let signals_df = df.filter(&signals_expr)?;

    if signals_df.height() > 0 {
        println!("Found {} trading signals:", signals_df.height());
        println!(
            "{}",
            signals_df.select(["date", "close", "sma_20", "sma_50", "macd", "adx", "signal"])?
        );
    } else {
        println!("No trend following signals found in this period.");
    }

    // Educational explanation
    println!("\nTrend Following Strategy Explanation:");
    println!("1. Moving Average Crossovers: When the shorter-term moving average crosses");
    println!("   above the longer-term moving average, it signals an uptrend (buy).");
    println!("2. MACD: When the MACD line crosses above its signal line, it confirms");
    println!("   bullish momentum (buy). Crossing below indicates bearish momentum (sell).");
    println!("3. ADX: Measures the strength of a trend. Values above 25 indicate a strong trend.");
    println!("4. This strategy aims to identify and follow established trends in the market,");
    println!("   capturing the middle portion of significant price moves.");

    Ok(())
}

// Generate trend following signals based on moving averages, MACD, and ADX
fn calculate_trend_signals(df: &mut DataFrame) -> Result<(), PolarsError> {
    // Get the data we need
    let sma_20 = df.column("sma_20")?.f64()?;
    let sma_50 = df.column("sma_50")?.f64()?;
    let macd = df.column("macd")?.f64()?;
    let macd_signal = df.column("macd_signal")?.f64()?;
    let adx = df.column("adx")?.f64()?;
    let close = df.column("close")?.f64()?;

    let height = df.height();

    // Create vector to store signals
    let mut signals = Vec::with_capacity(height);
    let mut prev_signal = 0; // To track current position

    // Process each row
    for i in 0..height {
        let sma20 = sma_20.get(i).unwrap_or(f64::NAN);
        let sma50 = sma_50.get(i).unwrap_or(f64::NAN);
        let m = macd.get(i).unwrap_or(f64::NAN);
        let m_signal = macd_signal.get(i).unwrap_or(f64::NAN);
        let a = adx.get(i).unwrap_or(f64::NAN);
        let _c = close.get(i).unwrap_or(f64::NAN);

        // Check for NaN values
        if sma20.is_nan() || sma50.is_nan() || m.is_nan() || m_signal.is_nan() || a.is_nan() {
            signals.push(0);
            continue;
        }

        let signal;

        // Strong uptrend conditions
        if sma20 > sma50 && m > m_signal && a >= 25.0 && prev_signal <= 0 {
            signal = 1; // Buy signal
        }
        // Strong downtrend conditions
        else if sma20 < sma50 && m < m_signal && a >= 25.0 && prev_signal >= 0 {
            signal = -1; // Sell signal
        }
        // If we're in a position, maintain it unless trend changes
        else {
            signal = prev_signal;
        }

        signals.push(signal);
        prev_signal = signal;
    }

    // Add signals to dataframe
    df.with_column(Series::new("signal".into(), signals))?;

    Ok(())
}
