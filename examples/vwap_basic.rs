// Example: Volume Weighted Average Price (VWAP)
// This example demonstrates how to calculate and visualize VWAP

use polars::prelude::*;
use rustalib::indicators::moving_averages::calculate_vwap;

fn main() -> Result<(), PolarsError> {
    println!("VWAP (Volume Weighted Average Price) Example");
    println!("===========================================\n");

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
        ],
    );

    let opens = Series::new(
        "open".into(),
        &[
            100.0, 101.5, 102.3, 103.8, 102.5, 101.2, 102.5, 103.3, 104.0, 103.1, 102.8, 103.5,
            104.2, 105.0, 104.5,
        ],
    );

    let highs = Series::new(
        "high".into(),
        &[
            101.8, 102.8, 103.5, 104.2, 103.1, 102.5, 103.8, 104.5, 105.2, 104.0, 103.5, 104.2,
            105.1, 106.0, 105.3,
        ],
    );

    let lows = Series::new(
        "low".into(),
        &[
            99.5, 101.0, 101.8, 102.9, 101.8, 100.5, 102.0, 102.8, 103.2, 102.5, 102.1, 102.8,
            103.8, 104.2, 103.8,
        ],
    );

    let closes = Series::new(
        "close".into(),
        &[
            101.5, 102.3, 103.2, 102.5, 101.8, 102.4, 103.2, 104.0, 103.5, 102.9, 103.2, 104.2,
            104.8, 104.5, 105.1,
        ],
    );

    // Create volume series with f64 data type
    let volumes = Series::new(
        "volume".into(),
        &[
            150000.0, 180000.0, 160000.0, 140000.0, 130000.0, 120000.0, 140000.0, 160000.0,
            190000.0, 150000.0, 140000.0, 160000.0, 180000.0, 210000.0, 190000.0,
        ],
    );

    // Create DataFrame
    let df = DataFrame::new(vec![
        dates.clone().into(),
        opens.clone().into(),
        highs.clone().into(),
        lows.clone().into(),
        closes.clone().into(),
        volumes.clone().into(),
    ])?;

    // Display the original data
    println!("Original OHLCV Data:");
    println!("{}", df);

    // Calculate VWAP with different lookback periods
    let vwap_full = calculate_vwap(&df, 0)?;
    let vwap_5 = calculate_vwap(&df, 5)?;

    // Create a new DataFrame with the original data plus VWAP
    let mut df_with_vwap = df.clone();
    df_with_vwap = df_with_vwap.hstack(&[vwap_full.into()])?.clone();

    // Extract the values from vwap_5 and create a new Series with a different name
    let vwap_5_values: Vec<f64> = vwap_5
        .f64()?
        .iter()
        .map(|opt_val| opt_val.unwrap_or(f64::NAN))
        .collect();
    let vwap_5_renamed = Series::new("vwap_5day".into(), vwap_5_values);

    df_with_vwap = df_with_vwap.hstack(&[vwap_5_renamed.into()])?.clone();

    // Display results
    println!("\nVWAP Analysis:");
    println!(
        "{}",
        df_with_vwap.select(["date", "close", "volume", "vwap", "vwap_5day"])?
    );

    // Compare VWAP to closing price
    let close_vals = df_with_vwap.column("close")?.f64()?;
    let vwap_vals = df_with_vwap.column("vwap")?.f64()?;
    let height = df_with_vwap.height();

    let mut price_vs_vwap = Vec::with_capacity(height);

    for i in 0..height {
        let close_val = close_vals.get(i).unwrap_or(f64::NAN);
        let vwap_val = vwap_vals.get(i).unwrap_or(f64::NAN);

        if close_val.is_nan() || vwap_val.is_nan() {
            price_vs_vwap.push("N/A");
        } else if close_val > vwap_val {
            price_vs_vwap.push("ABOVE VWAP");
        } else if close_val < vwap_val {
            price_vs_vwap.push("BELOW VWAP");
        } else {
            price_vs_vwap.push("AT VWAP");
        }
    }

    // Add the price_vs_vwap column
    let status_series = Series::new("price_vs_vwap".into(), price_vs_vwap);
    let final_df = df_with_vwap.hstack(&[status_series.into()])?.clone();

    println!("\nPrice vs VWAP Analysis:");
    println!(
        "{}",
        final_df.select(["date", "close", "vwap", "price_vs_vwap"])?
    );

    // Educational explanation
    println!("\nVWAP Indicator Explanation:");
    println!("1. VWAP stands for Volume-Weighted Average Price.");
    println!("2. It is calculated by adding up the dollars traded for every transaction");
    println!("   (price multiplied by the volume) and then dividing by the total volume.");
    println!("3. VWAP is especially important for day traders and institutional traders.");
    println!("4. Trading above VWAP often indicates bullish sentiment for the day.");
    println!("5. Trading below VWAP often indicates bearish sentiment for the day.");
    println!("6. Institutional traders use VWAP to minimize market impact of large orders.");

    Ok(())
}
