use crate::indicators::{
    moving_averages::{calculate_ema, calculate_sma},
    oscillators::{calculate_macd, calculate_rsi},
    volatility::{
        calculate_atr, calculate_bb_b, calculate_bollinger_bands, calculate_gk_volatility,
    },
};
use crate::util::dataframe_utils::ensure_f64_column;
use crate::util::time_utils::create_cyclical_time_features;
use polars::prelude::*;

/// Adds all technical indicators to the DataFrame
///
/// # Arguments
///
/// * `df` - DataFrame to add indicators to
///
/// # Returns
///
/// Returns a PolarsResult containing the enhanced DataFrame
pub fn add_technical_indicators(df: &mut DataFrame) -> PolarsResult<DataFrame> {
    // Convert numeric columns to Float64 by mutating in-place via Column
    let numeric_columns = ["open", "high", "low", "close", "volume"];
    for col_name in numeric_columns {
        // Skip if column doesn't exist
        if !df.schema().contains(col_name) {
            continue;
        }

        ensure_f64_column(df, col_name)?;
    }

    // Calculate moving averages
    let sma20 = calculate_sma(df, "close", 20)?.with_name("sma_20".into());
    let sma50 = calculate_sma(df, "close", 50)?.with_name("sma_50".into());
    let ema20 = calculate_ema(df, "close", 20)?.with_name("ema_20".into());

    // Calculate oscillators
    let rsi = calculate_rsi(df, 14, "close")?.with_name("rsi_14".into());
    let (macd, macd_signal) = calculate_macd(df, 12, 26, 9, "close")?;
    let macd = macd.with_name("macd".into());
    let macd_signal = macd_signal.with_name("macd_signal".into());

    // Calculate volatility indicators
    let (bb_middle, bb_upper, bb_lower) = calculate_bollinger_bands(df, 20, 2.0, "close")?;
    let bb_middle = bb_middle.with_name("bb_middle".into());
    let bb_upper = bb_upper.with_name("bb_upper".into());
    let bb_lower = bb_lower.with_name("bb_lower".into());
    let bb_b = calculate_bb_b(df, 20, 2.0, "close")?.with_name("bb_b".into());
    let atr = calculate_atr(df, 14)?.with_name("atr_14".into());
    let gk_vol = calculate_gk_volatility(df, 10)?.with_name("gk_volatility".into());

    // Calculate price dynamics
    let close = df.column("close")?.f64()?;
    let prev_close = close.shift(1);

    // Calculate percentage returns
    let returns = ((close.clone() - prev_close.clone()) / prev_close.clone())
        .with_name("returns".into())
        .into_series();

    // Calculate daily price range
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let price_range = ((high.clone() - low.clone()) / close.clone())
        .with_name("price_range".into())
        .into_series();

    // Add lag features
    let close_lag_5 = close.shift(5).with_name("close_lag_5".into());
    let close_lag_15 = close.shift(15).with_name("close_lag_15".into());
    let close_lag_30 = close.shift(30).with_name("close_lag_30".into());

    // Returns over different time windows
    let close_lag_5_clone = close_lag_5.clone();
    let returns_5min = ((close.clone() - close_lag_5_clone.clone()) / close_lag_5_clone)
        .with_name("returns_5min".into())
        .into_series();

    // Shorter-term volatility (15-min window)
    let mut vol_15min = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        if i < 15 {
            vol_15min.push(0.0);
            continue;
        }

        let mut returns = Vec::with_capacity(15);
        for j in (i - 15)..i {
            // Safely access the current price value
            let current_opt = close.get(j);
            // Safely access the previous price value, checking if j-1 is valid
            let previous_opt = if j > 0 { close.get(j - 1) } else { None };

            // Only calculate return if both values are valid and previous is not zero
            if let (Some(current), Some(previous)) = (current_opt, previous_opt) {
                if previous != 0.0 {
                    returns.push((current - previous) / previous);
                }
            }
        }

        // Calculate standard deviation of returns
        if returns.is_empty() {
            vol_15min.push(0.0);
            continue;
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance =
            returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
        vol_15min.push(variance.sqrt());
    }
    let volatility_15min = Series::new("volatility_15min".into(), vol_15min);

    // Time-based features
    let mut time_features = Vec::new();
    if df.schema().contains("time") {
        time_features = create_cyclical_time_features(df, "time", "%Y-%m-%d %H:%M:%S UTC")?;
    }

    // Add all features to the DataFrame
    let mut features_to_add = vec![
        sma20,
        sma50,
        ema20,
        rsi,
        macd,
        macd_signal,
        bb_middle,
        bb_upper,
        bb_lower,
        bb_b,
        atr,
        gk_vol,
        returns,
        price_range,
        close_lag_5.into_series(),
        close_lag_15.into_series(),
        close_lag_30.into_series(),
        returns_5min,
        volatility_15min,
    ];

    // Add time features if available
    features_to_add.extend(time_features);

    for feature in features_to_add {
        df.with_column(feature)?;
    }

    Ok(df.clone())
}
