use polars::prelude::*;
use crate::indicators::moving_averages::{calculate_ema, calculate_sma};
use crate::indicators::oscillators::calculate_rsi;

/// Simulate higher timeframe by aggregating data
///
/// This function creates a simulated higher timeframe from the current
/// data by aggregating N periods together. Useful for multi-timeframe analysis.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `aggregation_factor` - Number of periods to aggregate (e.g., 4 for daily to weekly)
///
/// # Returns
///
/// * `PolarsResult<DataFrame>` - Aggregated DataFrame with OHLCV data
fn create_higher_timeframe(
    df: &DataFrame,
    aggregation_factor: usize,
) -> PolarsResult<DataFrame> {
    if aggregation_factor <= 1 {
        return Err(PolarsError::ComputeError(
            "Aggregation factor must be greater than 1".into(),
        ));
    }
    
    // Ensure necessary columns exist
    for col in ["open", "high", "low", "close", "volume"].iter() {
        if !df.schema().contains(*col) {
            return Err(PolarsError::ComputeError(
                format!("Required column '{}' not found", col).into(),
            ));
        }
    }
    
    // Get OHLCV data
    let open = df.column("open")?.f64()?;
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    let volume = df.column("volume")?.f64()?;
    
    // Calculate number of aggregated periods
    let num_periods = df.height() / aggregation_factor;
    
    // Create vectors for aggregated data
    let mut agg_open = Vec::with_capacity(num_periods);
    let mut agg_high = Vec::with_capacity(num_periods);
    let mut agg_low = Vec::with_capacity(num_periods);
    let mut agg_close = Vec::with_capacity(num_periods);
    let mut agg_volume = Vec::with_capacity(num_periods);
    
    // Aggregate data into higher timeframe
    for i in 0..num_periods {
        let start_idx = i * aggregation_factor;
        let end_idx = ((i + 1) * aggregation_factor).min(df.height());
        
        // Open of the period is the first open
        let o = open.get(start_idx).unwrap_or(f64::NAN);
        
        // High of the period is the highest high
        let mut h = f64::MIN;
        for j in start_idx..end_idx {
            let h_val = high.get(j).unwrap_or(f64::NAN);
            if !h_val.is_nan() {
                h = h.max(h_val);
            }
        }
        
        // Low of the period is the lowest low
        let mut l = f64::MAX;
        for j in start_idx..end_idx {
            let l_val = low.get(j).unwrap_or(f64::NAN);
            if !l_val.is_nan() {
                l = l.min(l_val);
            }
        }
        
        // Close of the period is the last close
        let c = close.get(end_idx - 1).unwrap_or(f64::NAN);
        
        // Volume of the period is the sum of volumes
        let mut v = 0.0;
        for j in start_idx..end_idx {
            let v_val = volume.get(j).unwrap_or(f64::NAN);
            if !v_val.is_nan() {
                v += v_val;
            }
        }
        
        agg_open.push(o);
        agg_high.push(h);
        agg_low.push(l);
        agg_close.push(c);
        agg_volume.push(v);
    }
    
    // Create aggregated DataFrame
    DataFrame::new(vec![
        Series::new("open".into(), agg_open).into(),
        Series::new("high".into(), agg_high).into(),
        Series::new("low".into(), agg_low).into(),
        Series::new("close".into(), agg_close).into(),
        Series::new("volume".into(), agg_volume).into(),
    ])
}

/// Calculate multi-timeframe trend alignment
///
/// This function assesses if trends are aligned across multiple timeframes,
/// which is a strong confirmation signal for short-term traders.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `agg_factor1` - First aggregation factor (e.g., 4 for daily to weekly)
/// * `agg_factor2` - Second aggregation factor (e.g., 20 for daily to monthly)
/// * `ma_period` - Moving average period for trend determination (default: 20)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with alignment values (2: strong alignment,
///                            1: moderate alignment, 0: no alignment, -1: moderate misalignment,
///                            -2: strong misalignment)
pub fn calculate_multi_timeframe_alignment(
    df: &DataFrame,
    agg_factor1: usize,
    agg_factor2: usize,
    ma_period: Option<usize>,
) -> PolarsResult<Series> {
    let period = ma_period.unwrap_or(20);
    
    // Create higher timeframes
    let higher_tf1 = create_higher_timeframe(df, agg_factor1)?;
    let higher_tf2 = create_higher_timeframe(df, agg_factor2)?;
    
    // Calculate EMAs for all timeframes
    let current_ma = calculate_ema(df, "close", period)?;
    let higher_ma1 = calculate_ema(&higher_tf1, "close", period)?;
    let higher_ma2 = calculate_ema(&higher_tf2, "close", period)?;
    
    // Get closing prices for all timeframes
    let close = df.column("close")?.f64()?;
    let higher_close1 = higher_tf1.column("close")?.f64()?;
    let higher_close2 = higher_tf2.column("close")?.f64()?;
    
    // Get MA values
    let current_ma_vals = current_ma.f64()?;
    
    // Determine trends for each timeframe
    let current_trend = detect_trend(close, current_ma_vals, df.height())?;
    
    // For higher timeframes, we need to expand the values back to original timeframe length
    let expanded_trend1 = expand_higher_timeframe_data(&current_trend, &higher_close1, &higher_tf1, agg_factor1, df.height())?;
    let expanded_trend2 = expand_higher_timeframe_data(&current_trend, &higher_close2, &higher_tf2, agg_factor2, df.height())?;
    
    // Calculate alignment
    let mut alignment = Vec::with_capacity(df.height());
    
    // Fill initial values with no alignment
    for i in 0..period.min(df.height()) {
        alignment.push(0);
    }
    
    // Assess alignment for each point
    for i in period..df.height() {
        let current = current_trend[i];
        let higher1 = expanded_trend1[i];
        let higher2 = expanded_trend2[i];
        
        // Count how many timeframes agree with the current trend
        let agreement_count = if current == higher1 { 1 } else { 0 } + 
                              if current == higher2 { 1 } else { 0 };
        
        // Determine alignment score
        if current > 0 {
            // Bullish current trend
            if agreement_count == 2 {
                alignment.push(2); // Strong bullish alignment
            } else if agreement_count == 1 {
                alignment.push(1); // Moderate bullish alignment
            } else {
                alignment.push(-1); // Misalignment (current bullish, higher bearish)
            }
        } else if current < 0 {
            // Bearish current trend
            if agreement_count == 2 {
                alignment.push(-2); // Strong bearish alignment
            } else if agreement_count == 1 {
                alignment.push(-1); // Moderate bearish alignment
            } else {
                alignment.push(1); // Misalignment (current bearish, higher bullish)
            }
        } else {
            alignment.push(0); // No clear trend
        }
    }
    
    Ok(Series::new("multi_timeframe_alignment".into(), alignment))
}

/// Helper function to detect trend direction
///
/// # Arguments
///
/// * `price` - Price series
/// * `ma` - Moving average series
/// * `length` - Length of the series
///
/// # Returns
///
/// * `PolarsResult<Vec<i32>>` - Vector with trend direction (1: up, -1: down, 0: flat)
fn detect_trend(
    price: &ChunkedArray<Float64Type>,
    ma: &ChunkedArray<Float64Type>,
    length: usize,
) -> PolarsResult<Vec<i32>> {
    let mut trend = Vec::with_capacity(length);
    
    for i in 0..length {
        let p = price.get(i).unwrap_or(f64::NAN);
        let m = ma.get(i).unwrap_or(f64::NAN);
        
        if p.is_nan() || m.is_nan() {
            trend.push(0);
        } else if p > m * 1.01 {
            trend.push(1); // Uptrend (price > MA by at least 1%)
        } else if p < m * 0.99 {
            trend.push(-1); // Downtrend (price < MA by at least 1%)
        } else {
            trend.push(0); // No clear trend
        }
    }
    
    Ok(trend)
}

/// Helper function to expand higher timeframe data to match original timeframe
///
/// # Arguments
///
/// * `base_trend` - Trend data from base timeframe (for initialization)
/// * `higher_data` - Data series from higher timeframe
/// * `higher_df` - Entire higher timeframe DataFrame
/// * `agg_factor` - Aggregation factor used
/// * `original_length` - Length of original series
///
/// # Returns
///
/// * `PolarsResult<Vec<i32>>` - Expanded vector matching original length
fn expand_higher_timeframe_data(
    base_trend: &[i32],
    higher_data: &ChunkedArray<Float64Type>,
    higher_df: &DataFrame,
    agg_factor: usize,
    original_length: usize,
) -> PolarsResult<Vec<i32>> {
    let mut expanded = Vec::with_capacity(original_length);
    
    // Calculate EMAs for higher timeframe
    let higher_ma = calculate_ema(higher_df, "close", 20)?;
    let higher_ma_vals = higher_ma.f64()?;
    
    // Detect trend in higher timeframe
    let higher_trend = detect_trend(higher_data, higher_ma_vals, higher_df.height())?;
    
    // Expand higher timeframe trend to original timeframe
    for i in 0..original_length {
        let higher_idx = i / agg_factor;
        
        if higher_idx < higher_trend.len() {
            expanded.push(higher_trend[higher_idx]);
        } else {
            // Use base trend as fallback if index is out of bounds
            expanded.push(base_trend[i.min(base_trend.len() - 1)]);
        }
    }
    
    Ok(expanded)
}

/// Calculate multi-timeframe RSI divergence
///
/// This function detects divergences between price action and RSI
/// across multiple timeframes, a powerful signal for potential reversals.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `rsi_period` - Period for RSI calculation (default: 14)
/// * `agg_factor` - Aggregation factor for higher timeframe (default: 4)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with divergence signals (1: bullish, -1: bearish, 0: none)
pub fn calculate_multi_timeframe_rsi_divergence(
    df: &DataFrame,
    rsi_period: Option<usize>,
    agg_factor: Option<usize>,
) -> PolarsResult<Series> {
    let period = rsi_period.unwrap_or(14);
    let agg = agg_factor.unwrap_or(4);
    
    // Calculate RSI on current timeframe
    let rsi = calculate_rsi(df, period, "close")?;
    let rsi_vals = rsi.f64()?;
    
    // Create higher timeframe
    let higher_tf = create_higher_timeframe(df, agg)?;
    
    // Calculate RSI on higher timeframe
    let higher_rsi = calculate_rsi(&higher_tf, period, "close")?;
    let higher_rsi_vals = higher_rsi.f64()?;
    
    // Get price data
    let close = df.column("close")?.f64()?;
    let higher_close = higher_tf.column("close")?.f64()?;
    
    let mut divergence_signals = Vec::with_capacity(df.height());
    
    // First values will have no signal until we have enough data
    let lookback = 5; // Look back 5 bars for peaks/troughs
    for i in 0..period.max(lookback).min(df.height()) {
        divergence_signals.push(0);
    }
    
    // Detect divergences
    for i in period.max(lookback)..df.height() {
        // Check if we can detect a price peak/trough
        let mut price_peak = true;
        let mut price_trough = true;
        
        for j in 1..=lookback {
            if i < j || close.get(i).unwrap_or(f64::NAN) <= close.get(i - j).unwrap_or(f64::NAN) {
                price_peak = false;
            }
            if i < j || close.get(i).unwrap_or(f64::NAN) >= close.get(i - j).unwrap_or(f64::NAN) {
                price_trough = false;
            }
        }
        
        // Get current higher timeframe position
        let higher_idx = i / agg;
        
        if higher_idx >= higher_tf.height() {
            divergence_signals.push(0);
            continue;
        }
        
        // Check for RSI divergence
        if price_peak {
            // Price is at a peak - check for bearish divergence (lower RSI)
            if higher_idx > 0 && 
               higher_close.get(higher_idx).unwrap_or(f64::NAN) > higher_close.get(higher_idx - 1).unwrap_or(f64::NAN) &&
               higher_rsi_vals.get(higher_idx).unwrap_or(f64::NAN) < higher_rsi_vals.get(higher_idx - 1).unwrap_or(f64::NAN) {
                divergence_signals.push(-1); // Bearish divergence on higher timeframe
                continue;
            }
        } else if price_trough {
            // Price is at a trough - check for bullish divergence (higher RSI)
            if higher_idx > 0 &&
               higher_close.get(higher_idx).unwrap_or(f64::NAN) < higher_close.get(higher_idx - 1).unwrap_or(f64::NAN) &&
               higher_rsi_vals.get(higher_idx).unwrap_or(f64::NAN) > higher_rsi_vals.get(higher_idx - 1).unwrap_or(f64::NAN) {
                divergence_signals.push(1); // Bullish divergence on higher timeframe
                continue;
            }
        }
        
        // No divergence
        divergence_signals.push(0);
    }
    
    Ok(Series::new("multi_tf_rsi_divergence".into(), divergence_signals))
}

/// Add multi-timeframe analysis to DataFrame
///
/// # Arguments
///
/// * `df` - Mutable reference to DataFrame
/// * `daily_to_weekly` - Aggregation factor for daily to weekly (default: 5)
/// * `daily_to_monthly` - Aggregation factor for daily to monthly (default: 20)
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_multi_timeframe_analysis(
    df: &mut DataFrame,
    daily_to_weekly: Option<usize>,
    daily_to_monthly: Option<usize>,
) -> PolarsResult<()> {
    let weekly_factor = daily_to_weekly.unwrap_or(5);
    let monthly_factor = daily_to_monthly.unwrap_or(20);
    
    let alignment = calculate_multi_timeframe_alignment(df, weekly_factor, monthly_factor, None)?;
    let divergence = calculate_multi_timeframe_rsi_divergence(df, None, Some(weekly_factor))?;
    
    df.with_column(alignment)?;
    df.with_column(divergence)?;
    
    Ok(())
} 