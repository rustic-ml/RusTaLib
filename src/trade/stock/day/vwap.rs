//! Volume Weighted Average Price (VWAP) Indicators
//! 
//! VWAP is one of the most important indicators for day traders, showing the average
//! price weighted by volume. It serves as a benchmark for intraday fair value.

use polars::prelude::*;

/// Calculate Volume Weighted Average Price (VWAP)
/// 
/// VWAP is calculated by summing the dollars traded for every transaction
/// (price multiplied by the number of shares traded) and then dividing
/// by the total shares traded.
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLCV data
/// * `reset_daily` - Whether to reset calculations at the start of each trading day
/// * `datetime_col` - Name of the datetime column (for daily reset)
///
/// # Returns
///
/// * `PolarsResult<Series>` - VWAP values as a Series
pub fn calculate_vwap(
    df: &DataFrame,
    reset_daily: bool,
    datetime_col: Option<&str>,
) -> PolarsResult<Series> {
    // Ensure required columns exist
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    let volume = df.column("volume")?.f64()?;
    
    // Calculate typical price: (high + low + close) / 3
    let mut typical_price = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let h = high.get(i).unwrap_or(f64::NAN);
        let l = low.get(i).unwrap_or(f64::NAN);
        let c = close.get(i).unwrap_or(f64::NAN);
        
        typical_price.push((h + l + c) / 3.0);
    }
    
    // Calculate price-volume (typicalPrice * volume)
    let mut price_volume = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let tp = typical_price[i];
        let vol = volume.get(i).unwrap_or(f64::NAN);
        
        price_volume.push(tp * vol);
    }
    
    // Calculate cumulative values based on reset preference
    let mut cum_price_volume = Vec::with_capacity(df.height());
    let mut cum_volume = Vec::with_capacity(df.height());
    let mut vwap = Vec::with_capacity(df.height());
    
    if reset_daily && datetime_col.is_some() {
        // Reset calculations at the start of each day
        let date_col = df.column(datetime_col.unwrap())?;
        let mut current_date = String::new();
        
        for i in 0..df.height() {
            let date_str = match date_col.dtype() {
                DataType::Utf8 => date_col.utf8()?.get(i).unwrap_or("").to_string(),
                DataType::Date => format!("{}", date_col.date()?.get(i).unwrap_or(0)),
                DataType::Datetime(_, _) => {
                    let dt = date_col.datetime()?.get(i);
                    format!("{}", dt.timestamp() / 86400) // Convert to days
                }
                _ => "".to_string(),
            };
            
            // Extract just the date part (ignore time)
            let date_part = date_str.split_whitespace().next().unwrap_or("");
            
            if date_part != current_date && !date_part.is_empty() {
                // Reset for new day
                current_date = date_part.to_string();
                cum_price_volume.push(price_volume[i]);
                cum_volume.push(volume.get(i).unwrap_or(0.0));
            } else {
                // Accumulate within the same day
                let prev_cum_pv = if i > 0 { cum_price_volume[i-1] } else { 0.0 };
                let prev_cum_vol = if i > 0 { cum_volume[i-1] } else { 0.0 };
                
                cum_price_volume.push(prev_cum_pv + price_volume[i]);
                cum_volume.push(prev_cum_vol + volume.get(i).unwrap_or(0.0));
            }
            
            // Calculate VWAP
            if cum_volume[i] > 0.0 {
                vwap.push(cum_price_volume[i] / cum_volume[i]);
            } else {
                vwap.push(f64::NAN);
            }
        }
    } else {
        // No daily reset, calculate cumulative for the entire period
        let mut cum_pv = 0.0;
        let mut cum_vol = 0.0;
        
        for i in 0..df.height() {
            cum_pv += price_volume[i];
            cum_vol += volume.get(i).unwrap_or(0.0);
            
            cum_price_volume.push(cum_pv);
            cum_volume.push(cum_vol);
            
            if cum_vol > 0.0 {
                vwap.push(cum_pv / cum_vol);
            } else {
                vwap.push(f64::NAN);
            }
        }
    }
    
    Ok(Series::new("vwap", vwap))
}

/// Calculate VWAP Bands
/// 
/// VWAP bands are statistical extensions around VWAP, similar to Bollinger Bands,
/// that provide potential support and resistance levels.
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLCV data
/// * `std_dev_multipliers` - Vector of standard deviation multipliers for bands
/// * `reset_daily` - Whether to reset calculations at the start of each trading day
/// * `datetime_col` - Name of the datetime column (for daily reset)
///
/// # Returns
///
/// * `PolarsResult<Vec<Series>>` - Vector of Series containing VWAP and bands
pub fn calculate_vwap_bands(
    df: &DataFrame,
    std_dev_multipliers: Vec<f64>,
    reset_daily: bool,
    datetime_col: Option<&str>,
) -> PolarsResult<Vec<Series>> {
    // Calculate base VWAP
    let vwap = calculate_vwap(df, reset_daily, datetime_col)?;
    let vwap_values = vwap.f64()?;
    
    // Prepare high and close for band calculations
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    
    // Calculate standard deviation of closes from VWAP
    let mut squared_diffs = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let c = close.get(i).unwrap_or(f64::NAN);
        let v = vwap_values.get(i).unwrap_or(f64::NAN);
        
        if !v.is_nan() && !c.is_nan() {
            squared_diffs.push((c - v).powi(2));
        } else {
            squared_diffs.push(f64::NAN);
        }
    }
    
    // Calculate standard deviation using a rolling window
    let window_size = 20.min(df.height()); // Use 20-period window or smaller if not enough data
    let mut std_dev = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let window_start = if i >= window_size { i - window_size + 1 } else { 0 };
        let mut valid_count = 0;
        let mut sum = 0.0;
        
        for j in window_start..=i {
            if !squared_diffs[j].is_nan() {
                sum += squared_diffs[j];
                valid_count += 1;
            }
        }
        
        if valid_count > 0 {
            std_dev.push((sum / valid_count as f64).sqrt());
        } else {
            std_dev.push(f64::NAN);
        }
    }
    
    // Create result Series vector starting with the base VWAP
    let mut result = vec![vwap];
    
    // Add bands for each multiplier
    for &multiplier in &std_dev_multipliers {
        let mut upper_band = Vec::with_capacity(df.height());
        let mut lower_band = Vec::with_capacity(df.height());
        
        for i in 0..df.height() {
            let v = vwap_values.get(i).unwrap_or(f64::NAN);
            let sd = std_dev[i];
            
            if !v.is_nan() && !sd.is_nan() {
                upper_band.push(v + multiplier * sd);
                lower_band.push(v - multiplier * sd);
            } else {
                upper_band.push(f64::NAN);
                lower_band.push(f64::NAN);
            }
        }
        
        result.push(Series::new(&format!("vwap_upper_{}", multiplier), upper_band));
        result.push(Series::new(&format!("vwap_lower_{}", multiplier), lower_band));
    }
    
    Ok(result)
}

/// Calculate VWAP Anchored to a Specific Time Point
/// 
/// Unlike regular VWAP which typically resets daily, anchored VWAP is calculated
/// from a specific point in time like market open, a significant high/low, or a news event.
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLCV data
/// * `anchor_index` - Index position to start the VWAP calculation
///
/// # Returns
///
/// * `PolarsResult<Series>` - Anchored VWAP values as a Series
pub fn calculate_anchored_vwap(
    df: &DataFrame,
    anchor_index: usize,
) -> PolarsResult<Series> {
    if anchor_index >= df.height() {
        return Err(PolarsError::ComputeError(
            format!("Anchor index {} is out of bounds for DataFrame with {} rows", anchor_index, df.height()).into()
        ));
    }
    
    // Ensure required columns exist
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    let volume = df.column("volume")?.f64()?;
    
    // Calculate typical price: (high + low + close) / 3
    let mut typical_price = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let h = high.get(i).unwrap_or(f64::NAN);
        let l = low.get(i).unwrap_or(f64::NAN);
        let c = close.get(i).unwrap_or(f64::NAN);
        
        typical_price.push((h + l + c) / 3.0);
    }
    
    // Calculate anchored VWAP
    let mut cum_price_volume = 0.0;
    let mut cum_volume = 0.0;
    let mut anchored_vwap = Vec::with_capacity(df.height());
    
    // Fill NaN for rows before the anchor
    for _ in 0..anchor_index {
        anchored_vwap.push(f64::NAN);
    }
    
    // Calculate VWAP from the anchor point
    for i in anchor_index..df.height() {
        let tp = typical_price[i];
        let vol = volume.get(i).unwrap_or(0.0);
        
        cum_price_volume += tp * vol;
        cum_volume += vol;
        
        if cum_volume > 0.0 {
            anchored_vwap.push(cum_price_volume / cum_volume);
        } else {
            anchored_vwap.push(f64::NAN);
        }
    }
    
    Ok(Series::new("anchored_vwap", anchored_vwap))
} 