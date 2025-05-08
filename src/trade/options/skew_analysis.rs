//! Options Volatility Skew Analysis
//! 
//! This module provides indicators and utilities for analyzing options volatility skew,
//! which reflects market sentiment and expected price movements across strikes.

use polars::prelude::*;
use polars::frame::DataFrame;
use std::collections::HashMap;

/// Calculate volatility skew across strikes
///
/// Measures the difference in implied volatility between OTM puts and OTM calls.
/// High positive skew indicates market concerns about downside risk.
///
/// # Arguments
/// * `df` - DataFrame with options data
/// * `iv_column` - Column name for implied volatility
/// * `strike_column` - Column name for strike price
/// * `price_column` - Column name for underlying price
/// * `is_call_column` - Column name indicating if option is a call (true) or put (false)
///
/// # Returns
/// * `PolarsResult<Series>` - Series with volatility skew values
pub fn calculate_strike_skew(
    df: &DataFrame,
    iv_column: &str,
    strike_column: &str,
    price_column: &str,
    is_call_column: &str,
) -> PolarsResult<Series> {
    // Extract required columns
    let iv = df.column(iv_column)?.f64()?;
    let strike = df.column(strike_column)?.f64()?;
    let price = df.column(price_column)?.f64()?;
    let is_call = df.column(is_call_column)?.bool()?;
    
    let len = df.height();
    let mut skew = vec![f64::NAN; len];
    
    // First pass: Group IV by relative strike (% OTM)
    let mut put_ivs: HashMap<i32, Vec<f64>> = HashMap::new();
    let mut call_ivs: HashMap<i32, Vec<f64>> = HashMap::new();
    
    for i in 0..len {
        let iv_val = iv.get(i).unwrap_or(f64::NAN);
        let strike_val = strike.get(i).unwrap_or(f64::NAN);
        let price_val = price.get(i).unwrap_or(f64::NAN);
        let call = is_call.get(i).unwrap_or(false);
        
        if iv_val.is_nan() || strike_val.is_nan() || price_val.is_nan() || price_val <= 0.0 {
            continue;
        }
        
        // Calculate % OTM and use as bucket key
        let otm_pct = ((strike_val - price_val) / price_val * 100.0).round() as i32;
        
        if call {
            call_ivs.entry(otm_pct).or_insert_with(Vec::new).push(iv_val);
        } else {
            put_ivs.entry(otm_pct).or_insert_with(Vec::new).push(iv_val);
        }
    }
    
    // Calculate average IV per OTM bucket
    let mut put_avg_ivs: HashMap<i32, f64> = HashMap::new();
    let mut call_avg_ivs: HashMap<i32, f64> = HashMap::new();
    
    for (pct, ivs) in &put_ivs {
        if !ivs.is_empty() {
            let avg = ivs.iter().sum::<f64>() / ivs.len() as f64;
            put_avg_ivs.insert(*pct, avg);
        }
    }
    
    for (pct, ivs) in &call_ivs {
        if !ivs.is_empty() {
            let avg = ivs.iter().sum::<f64>() / ivs.len() as f64;
            call_avg_ivs.insert(*pct, avg);
        }
    }
    
    // Calculate skew for each option
    for i in 0..len {
        let strike_val = strike.get(i).unwrap_or(f64::NAN);
        let price_val = price.get(i).unwrap_or(f64::NAN);
        
        if strike_val.is_nan() || price_val.is_nan() || price_val <= 0.0 {
            continue;
        }
        
        // Calculate % OTM
        let otm_pct = ((strike_val - price_val) / price_val * 100.0).round() as i32;
        
        // Find equidistant strikes on opposite side
        let opposite_pct = -otm_pct;
        
        // Calculate skew as difference between put and call IV at equidistant strikes
        if otm_pct < 0 && put_avg_ivs.contains_key(&otm_pct) && call_avg_ivs.contains_key(&opposite_pct) {
            // For puts
            skew[i] = put_avg_ivs[&otm_pct] - call_avg_ivs[&opposite_pct];
        } else if otm_pct > 0 && call_avg_ivs.contains_key(&otm_pct) && put_avg_ivs.contains_key(&opposite_pct) {
            // For calls
            skew[i] = put_avg_ivs[&opposite_pct] - call_avg_ivs[&otm_pct];
        } else {
            // Use static skew measurement (25-delta put vs 25-delta call)
            // We just need to find the closest buckets to 25-delta equivalent
            // In reality, this would be more sophisticated
            let put_25d = put_avg_ivs.get(&-10).or_else(|| put_avg_ivs.get(&-15));
            let call_25d = call_avg_ivs.get(&10).or_else(|| call_avg_ivs.get(&15));
            
            if let (Some(&put_iv), Some(&call_iv)) = (put_25d, call_25d) {
                skew[i] = put_iv - call_iv;
            }
        }
    }
    
    Ok(Series::new("strike_skew", skew))
}

/// Calculate wing skew ratio
///
/// Measures the ratio of far OTM put IV to ATM IV, indicating tail risk pricing.
///
/// # Arguments
/// * `df` - DataFrame with options data
/// * `iv_column` - Column name for implied volatility
/// * `strike_column` - Column name for strike price
/// * `price_column` - Column name for underlying price
/// * `is_call_column` - Column name indicating if option is a call (true) or put (false)
///
/// # Returns
/// * `PolarsResult<Series>` - Series with wing skew ratio values
pub fn calculate_wing_skew(
    df: &DataFrame,
    iv_column: &str,
    strike_column: &str,
    price_column: &str,
    is_call_column: &str,
) -> PolarsResult<Series> {
    // Extract required columns
    let iv = df.column(iv_column)?.f64()?;
    let strike = df.column(strike_column)?.f64()?;
    let price = df.column(price_column)?.f64()?;
    let is_call = df.column(is_call_column)?.bool()?;
    
    let len = df.height();
    let mut wing_skew = vec![f64::NAN; len];
    
    // First pass: Group IV by relative strike (% OTM)
    let mut atm_ivs: Vec<f64> = Vec::new();
    let mut far_otm_put_ivs: Vec<f64> = Vec::new();
    
    for i in 0..len {
        let iv_val = iv.get(i).unwrap_or(f64::NAN);
        let strike_val = strike.get(i).unwrap_or(f64::NAN);
        let price_val = price.get(i).unwrap_or(f64::NAN);
        let call = is_call.get(i).unwrap_or(false);
        
        if iv_val.is_nan() || strike_val.is_nan() || price_val.is_nan() || price_val <= 0.0 {
            continue;
        }
        
        // Calculate % OTM 
        let otm_pct = (strike_val - price_val) / price_val * 100.0;
        
        // Collect ATM options (both puts and calls)
        if otm_pct.abs() < 2.5 {
            atm_ivs.push(iv_val);
        }
        
        // Collect far OTM puts only
        if !call && otm_pct <= -15.0 {
            far_otm_put_ivs.push(iv_val);
        }
    }
    
    // Calculate average IVs
    if atm_ivs.is_empty() || far_otm_put_ivs.is_empty() {
        return Ok(Series::new("wing_skew", wing_skew));
    }
    
    let avg_atm_iv = atm_ivs.iter().sum::<f64>() / atm_ivs.len() as f64;
    let avg_far_otm_put_iv = far_otm_put_ivs.iter().sum::<f64>() / far_otm_put_ivs.len() as f64;
    
    // Calculate wing skew ratio
    let wing_skew_ratio = avg_far_otm_put_iv / avg_atm_iv;
    
    // Assign the ratio to all rows
    for i in 0..len {
        wing_skew[i] = wing_skew_ratio;
    }
    
    Ok(Series::new("wing_skew", wing_skew))
}

/// Calculate skew term structure
///
/// Analyzes how volatility skew changes across different expiration dates.
///
/// # Arguments
/// * `df` - DataFrame with options data
/// * `iv_column` - Column name for implied volatility
/// * `strike_column` - Column name for strike price
/// * `price_column` - Column name for underlying price
/// * `is_call_column` - Column name indicating if option is a call (true) or put (false)
/// * `expiry_column` - Column name for expiration date
///
/// # Returns
/// * `PolarsResult<Series>` - Series with skew term structure values
pub fn calculate_skew_term_structure(
    df: &DataFrame,
    iv_column: &str,
    strike_column: &str,
    price_column: &str,
    is_call_column: &str,
    expiry_column: &str,
) -> PolarsResult<Series> {
    // Extract required columns
    let iv = df.column(iv_column)?.f64()?;
    let strike = df.column(strike_column)?.f64()?;
    let price = df.column(price_column)?.f64()?;
    let is_call = df.column(is_call_column)?.bool()?;
    let expiry = df.column(expiry_column)?;
    
    let len = df.height();
    let mut term_structure = vec![f64::NAN; len];
    
    // Group data by expiry
    let mut expiry_groups: HashMap<String, Vec<usize>> = HashMap::new();
    
    for i in 0..len {
        if let Some(exp) = expiry.get(i) {
            let exp_str = exp.to_string();
            expiry_groups.entry(exp_str).or_insert_with(Vec::new).push(i);
        }
    }
    
    // Calculate skew for each expiry
    let mut expiry_skews: HashMap<String, f64> = HashMap::new();
    
    for (exp, indices) in &expiry_groups {
        // For each expiry, find the skew (25-delta put minus 25-delta call IV)
        let mut otm_put_ivs: Vec<f64> = Vec::new();
        let mut otm_call_ivs: Vec<f64> = Vec::new();
        
        for &idx in indices {
            let iv_val = iv.get(idx).unwrap_or(f64::NAN);
            let strike_val = strike.get(idx).unwrap_or(f64::NAN);
            let price_val = price.get(idx).unwrap_or(f64::NAN);
            let call = is_call.get(idx).unwrap_or(false);
            
            if iv_val.is_nan() || strike_val.is_nan() || price_val.is_nan() || price_val <= 0.0 {
                continue;
            }
            
            // Calculate % OTM 
            let otm_pct = (strike_val - price_val) / price_val * 100.0;
            
            // Approximate 25-delta area (could be more sophisticated in reality)
            if !call && otm_pct <= -10.0 && otm_pct > -15.0 {
                otm_put_ivs.push(iv_val);
            } else if call && otm_pct >= 10.0 && otm_pct < 15.0 {
                otm_call_ivs.push(iv_val);
            }
        }
        
        // Calculate skew if we have enough data
        if !otm_put_ivs.is_empty() && !otm_call_ivs.is_empty() {
            let avg_put_iv = otm_put_ivs.iter().sum::<f64>() / otm_put_ivs.len() as f64;
            let avg_call_iv = otm_call_ivs.iter().sum::<f64>() / otm_call_ivs.len() as f64;
            
            expiry_skews.insert(exp.clone(), avg_put_iv - avg_call_iv);
        }
    }
    
    // Sort expirations by time-to-expiry (simplified here)
    let mut expirations: Vec<(String, f64)> = expiry_skews
        .into_iter()
        .collect();
    
    // In a real implementation, we would parse dates and sort by time to expiry
    // For simplicity, we're just sorting by the string
    expirations.sort_by(|a, b| a.0.cmp(&b.0));
    
    // Calculate term structure slope with linear regression
    if expirations.len() >= 2 {
        // Calculate slope of skew vs time
        let n = expirations.len() as f64;
        let sum_x = (0..expirations.len()).sum::<usize>() as f64;
        let sum_y = expirations.iter().map(|(_, skew)| skew).sum::<f64>();
        let sum_xy = expirations.iter().enumerate()
            .map(|(i, (_, skew))| i as f64 * skew)
            .sum::<f64>();
        let sum_xx = (0..expirations.len()).map(|i| (i * i) as f64).sum::<f64>();
        
        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x);
        
        // Assign the slope to all rows matching each expiry
        for i in 0..len {
            if let Some(exp) = expiry.get(i) {
                let exp_str = exp.to_string();
                if let Some(exp_idx) = expirations.iter().position(|(e, _)| e == &exp_str) {
                    term_structure[i] = expirations[exp_idx].1;
                }
            }
        }
    }
    
    Ok(Series::new("skew_term_structure", term_structure))
}

/// Calculate skew breakpoints
///
/// Identifies points where volatility skew changes dramatically across strikes,
/// which can indicate market positioning or expected price targets.
///
/// # Arguments
/// * `df` - DataFrame with options data
/// * `iv_column` - Column name for implied volatility
/// * `strike_column` - Column name for strike price
/// * `price_column` - Column name for underlying price
/// * `is_call_column` - Column name indicating if option is a call (true) or put (false)
///
/// # Returns
/// * `PolarsResult<DataFrame>` - DataFrame with skew breakpoints
pub fn calculate_skew_breakpoints(
    df: &DataFrame,
    iv_column: &str,
    strike_column: &str,
    price_column: &str,
    is_call_column: &str,
) -> PolarsResult<DataFrame> {
    // Extract required columns
    let iv = df.column(iv_column)?.f64()?;
    let strike = df.column(strike_column)?.f64()?;
    let price = df.column(price_column)?.f64()?;
    let is_call = df.column(is_call_column)?.bool()?;
    
    // Create vectors to store breakpoint data
    let mut breakpoint_strikes = Vec::new();
    let mut breakpoint_magnitudes = Vec::new();
    let mut breakpoint_directions = Vec::new();
    
    // Group options by strike price
    let mut strike_ivs: HashMap<f64, Vec<f64>> = HashMap::new();
    
    for i in 0..df.height() {
        let iv_val = iv.get(i).unwrap_or(f64::NAN);
        let strike_val = strike.get(i).unwrap_or(f64::NAN);
        
        if iv_val.is_nan() || strike_val.is_nan() {
            continue;
        }
        
        strike_ivs.entry(strike_val).or_insert_with(Vec::new).push(iv_val);
    }
    
    // Calculate average IV per strike
    let mut strike_avg_iv: Vec<(f64, f64)> = Vec::new();
    for (strike_val, ivs) in strike_ivs {
        if ivs.is_empty() {
            continue;
        }
        
        let avg_iv = ivs.iter().sum::<f64>() / ivs.len() as f64;
        strike_avg_iv.push((strike_val, avg_iv));
    }
    
    // Sort by strike
    strike_avg_iv.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
    // Look for significant changes in IV between adjacent strikes
    if strike_avg_iv.len() >= 3 {
        for i in 1..(strike_avg_iv.len() - 1) {
            let prev_strike = strike_avg_iv[i-1].0;
            let curr_strike = strike_avg_iv[i].0;
            let next_strike = strike_avg_iv[i+1].0;
            
            let prev_iv = strike_avg_iv[i-1].1;
            let curr_iv = strike_avg_iv[i].1;
            let next_iv = strike_avg_iv[i+1].1;
            
            // Calculate IV rate of change
            let prev_change = (curr_iv - prev_iv) / (curr_strike - prev_strike);
            let next_change = (next_iv - curr_iv) / (next_strike - curr_strike);
            
            // Check for significant change in slope
            let slope_change = next_change - prev_change;
            
            // Threshold for significant change - this would be calibrated in a real system
            let threshold = 0.01;
            
            if slope_change.abs() > threshold {
                breakpoint_strikes.push(curr_strike);
                breakpoint_magnitudes.push(slope_change.abs());
                breakpoint_directions.push(if slope_change > 0.0 { "steepening" } else { "flattening" });
            }
        }
    }
    
    // Create result DataFrame
    let result_df = DataFrame::new(vec![
        Series::new("strike", breakpoint_strikes),
        Series::new("magnitude", breakpoint_magnitudes),
        Series::new("direction", breakpoint_directions),
    ])?;
    
    Ok(result_df)
}

/// Add all skew indicators to the DataFrame
///
/// # Arguments
/// * `df` - DataFrame to add indicators to
///
/// # Returns
/// * `PolarsResult<()>` - Result of the operation
pub fn add_skew_indicators(df: &mut DataFrame) -> PolarsResult<()> {
    // Check if we have the required columns
    let required_columns = [
        "iv", "strike", "price", "is_call"
    ];
    
    for &col in required_columns.iter() {
        if !df.schema().contains(col) {
            return Err(PolarsError::ComputeError(
                format!("Required column '{}' not found", col).into(),
            ));
        }
    }
    
    // Add strike skew
    let skew = calculate_strike_skew(df, "iv", "strike", "price", "is_call")?;
    df.with_column(skew)?;
    
    // Add wing skew
    let wing = calculate_wing_skew(df, "iv", "strike", "price", "is_call")?;
    df.with_column(wing)?;
    
    // Add skew term structure if expiry information is available
    if df.schema().contains("expiry") {
        let term = calculate_skew_term_structure(
            df, "iv", "strike", "price", "is_call", "expiry"
        )?;
        df.with_column(term)?;
    }
    
    // Breakpoints are stored separately and not added to the main dataframe
    // because they represent metadata about the entire option chain
    
    Ok(())
} 