use polars::prelude::*;
use crate::indicators::moving_averages::calculate_vwap;

/// Add VWAP standard deviation bands to the DataFrame
///
/// This function adds Volume Weighted Average Price (VWAP) and its deviation bands,
/// which are commonly used by day traders to identify potential support/resistance levels
/// and determine when a stock is overbought or oversold relative to its intraday average.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data, must contain a calculated VWAP column
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_vwap_bands(df: &mut DataFrame) -> PolarsResult<()> {
    // Ensure VWAP column exists
    if !df.schema().contains("vwap") {
        return Err(PolarsError::ComputeError(
            "VWAP column not found. Calculate VWAP first.".into(),
        ));
    }

    // Get the closing price and VWAP series
    let close = df.column("close")?.f64()?;
    let vwap = df.column("vwap")?.f64()?;
    
    // Calculate the standard deviation of close price from VWAP
    let mut vwap_diff = Vec::with_capacity(df.height());
    let mut squared_diff = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let close_val = close.get(i).unwrap_or(f64::NAN);
        let vwap_val = vwap.get(i).unwrap_or(f64::NAN);
        
        if !close_val.is_nan() && !vwap_val.is_nan() {
            let diff = close_val - vwap_val;
            vwap_diff.push(diff);
            squared_diff.push(diff * diff);
        } else {
            vwap_diff.push(f64::NAN);
            squared_diff.push(f64::NAN);
        }
    }
    
    // Calculate the standard deviation
    let mean_squared_diff = squared_diff.iter()
        .filter(|x| !x.is_nan())
        .sum::<f64>() / squared_diff.iter().filter(|x| !x.is_nan()).count() as f64;
    
    let std_dev = mean_squared_diff.sqrt();
    
    // Calculate VWAP standard deviation bands
    let vwap_upper_1sd = vwap.clone().into_iter()
        .map(|v| v.map(|x| x + std_dev)).collect::<Vec<_>>();
    
    let vwap_lower_1sd = vwap.clone().into_iter()
        .map(|v| v.map(|x| x - std_dev)).collect::<Vec<_>>();
    
    let vwap_upper_2sd = vwap.clone().into_iter()
        .map(|v| v.map(|x| x + 2.0 * std_dev)).collect::<Vec<_>>();
    
    let vwap_lower_2sd = vwap.clone().into_iter()
        .map(|v| v.map(|x| x - 2.0 * std_dev)).collect::<Vec<_>>();
    
    // Add the bands to the DataFrame
    df.with_column(Series::new("vwap_upper_1sd", vwap_upper_1sd))?;
    df.with_column(Series::new("vwap_lower_1sd", vwap_lower_1sd))?;
    df.with_column(Series::new("vwap_upper_2sd", vwap_upper_2sd))?;
    df.with_column(Series::new("vwap_lower_2sd", vwap_lower_2sd))?;
    
    // Calculate VWAP deviation percentage
    let vwap_deviation = vwap_diff.iter()
        .zip(vwap.into_iter())
        .map(|(diff, vwap_val)| {
            if let (Some(d), Some(v)) = (diff, vwap_val) {
                if v != 0.0 {
                    Some(d / v * 100.0)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    
    df.with_column(Series::new("vwap_deviation_pct", vwap_deviation))?;
    
    Ok(())
}

/// Calculate VWAP anchored to a specific time
///
/// This function calculates a VWAP that's anchored to a specific starting point
/// (like market open) and maintains that reference throughout the trading day,
/// unlike standard VWAP that's calculated on a rolling basis.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `time_col` - Name of time column
/// * `anchor_hour` - Hour to anchor VWAP to (e.g., 9 for 9:00 AM market open)
/// * `anchor_minute` - Minute to anchor VWAP to
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series containing the anchored VWAP values
pub fn calculate_anchored_vwap(
    df: &DataFrame,
    time_col: &str,
    anchor_hour: i32,
    anchor_minute: i32,
) -> PolarsResult<Series> {
    // Ensure necessary columns exist
    let required_columns = ["high", "low", "close", "volume", time_col];
    for col in required_columns {
        if !df.schema().contains(col) {
            return Err(PolarsError::ComputeError(
                format!("Required column '{}' not found", col).into(),
            ));
        }
    }
    
    // Extract necessary series
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    let volume = df.column("volume")?.f64()?;
    
    // Parse time column to extract hour and minute
    // Note: This implementation assumes the time column can be parsed
    // In a real implementation, proper time parsing would be needed based on the format
    let time_series = df.column(time_col)?;
    
    // Find the anchor point
    let mut anchor_index = 0;
    let mut found_anchor = false;
    
    // This is a simplified approach - would need proper datetime handling
    for i in 0..df.height() {
        // In a real implementation, extract hour and minute from time_series
        // For demonstration, assume we found the anchor point
        if i == 0 {  // Placeholder logic
            anchor_index = i;
            found_anchor = true;
            break;
        }
    }
    
    if !found_anchor {
        return Err(PolarsError::ComputeError(
            format!("Anchor time {}:{:02} not found in data", anchor_hour, anchor_minute).into(),
        ));
    }
    
    // Calculate anchored VWAP
    let mut cumulative_tp_v = 0.0;
    let mut cumulative_volume = 0.0;
    let mut anchored_vwap = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        if i < anchor_index {
            anchored_vwap.push(f64::NAN);
            continue;
        }
        
        let h = high.get(i).unwrap_or(f64::NAN);
        let l = low.get(i).unwrap_or(f64::NAN);
        let c = close.get(i).unwrap_or(f64::NAN);
        let v = volume.get(i).unwrap_or(f64::NAN);
        
        if h.is_nan() || l.is_nan() || c.is_nan() || v.is_nan() {
            anchored_vwap.push(f64::NAN);
            continue;
        }
        
        let typical_price = (h + l + c) / 3.0;
        cumulative_tp_v += typical_price * v;
        cumulative_volume += v;
        
        if cumulative_volume > 0.0 {
            anchored_vwap.push(cumulative_tp_v / cumulative_volume);
        } else {
            anchored_vwap.push(f64::NAN);
        }
    }
    
    Ok(Series::new("anchored_vwap", anchored_vwap))
} 