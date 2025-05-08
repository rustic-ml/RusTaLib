//! Volatility Analysis for Options Trading
//! 
//! This module provides indicators and utilities for analyzing implied volatility
//! patterns in options markets.

use polars::prelude::*;
use polars::frame::DataFrame;
use std::collections::HashMap;

/// Calculate implied volatility percentile
///
/// Computes where the current implied volatility stands relative to its historical range
///
/// # Arguments
/// * `df` - DataFrame with historical implied volatility data
/// * `iv_column` - Name of the implied volatility column
/// * `lookback_period` - Historical period to consider (default: 252 trading days)
///
/// # Returns
/// * `PolarsResult<Series>` - Series with IV percentile values
pub fn calculate_iv_percentile(
    df: &DataFrame,
    iv_column: &str,
    lookback_period: usize,
) -> PolarsResult<Series> {
    let iv = df.column(iv_column)?.f64()?;
    let len = df.height();
    let mut iv_percentile = vec![f64::NAN; len];
    
    for i in (lookback_period - 1)..len {
        let mut iv_history = Vec::with_capacity(lookback_period);
        let start_idx = i - (lookback_period - 1);
        
        for j in start_idx..=i {
            if let Some(val) = iv.get(j) {
                if !val.is_nan() {
                    iv_history.push(val);
                }
            }
        }
        
        if iv_history.is_empty() {
            continue;
        }
        
        iv_history.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let current_iv = iv.get(i).unwrap_or(f64::NAN);
        if current_iv.is_nan() {
            continue;
        }
        
        let count_below = iv_history.iter().filter(|&&x| x < current_iv).count();
        iv_percentile[i] = 100.0 * (count_below as f64) / (iv_history.len() as f64);
    }
    
    Ok(Series::new("iv_percentile", iv_percentile))
}

/// Calculate implied volatility term structure
///
/// Analyzes the relationship between IV across different expiration dates
///
/// # Arguments
/// * `df` - DataFrame with options data for different expirations
/// * `iv_column` - Column name containing implied volatility values
/// * `expiry_column` - Column name containing expiration dates
///
/// # Returns
/// * `PolarsResult<Series>` - Series with IV term structure slope
pub fn calculate_iv_term_structure(
    df: &DataFrame,
    iv_column: &str,
    expiry_column: &str,
) -> PolarsResult<Series> {
    // Extract expiry dates and IVs
    let expiry = df.column(expiry_column)?;
    let iv = df.column(iv_column)?.f64()?;
    
    // Group IV by expiry dates and calculate average IV for each expiry
    let mut expiry_to_iv: HashMap<String, Vec<f64>> = HashMap::new();
    let mut expiry_days: HashMap<String, f64> = HashMap::new();
    
    for i in 0..df.height() {
        if let Some(exp) = expiry.get(i) {
            let exp_str = exp.to_string();
            let iv_val = iv.get(i).unwrap_or(f64::NAN);
            
            if !iv_val.is_nan() {
                expiry_to_iv.entry(exp_str.clone()).or_insert_with(Vec::new).push(iv_val);
                
                // Parse days to expiry - this is simplified, in practice you would
                // calculate actual days between now and expiry date
                if let Ok(days) = exp_str.parse::<f64>() {
                    expiry_days.insert(exp_str, days);
                }
            }
        }
    }
    
    // Calculate average IV per expiry
    let mut expiry_avg_iv: Vec<(f64, f64)> = Vec::new();
    for (exp, ivs) in expiry_to_iv {
        if let Some(&days) = expiry_days.get(&exp) {
            let avg_iv = ivs.iter().sum::<f64>() / ivs.len() as f64;
            expiry_avg_iv.push((days, avg_iv));
        }
    }
    
    // Sort by days to expiry
    expiry_avg_iv.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
    // Calculate term structure slope for each point in the dataframe
    let mut term_structure = vec![f64::NAN; df.height()];
    
    if expiry_avg_iv.len() >= 2 {
        // Simple linear regression to find slope
        let n = expiry_avg_iv.len() as f64;
        let sum_x = expiry_avg_iv.iter().map(|(days, _)| days).sum::<f64>();
        let sum_y = expiry_avg_iv.iter().map(|(_, iv)| iv).sum::<f64>();
        let sum_xy = expiry_avg_iv.iter().map(|(days, iv)| days * iv).sum::<f64>();
        let sum_xx = expiry_avg_iv.iter().map(|(days, _)| days * days).sum::<f64>();
        
        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x);
        
        // Assign the slope to all rows
        for i in 0..term_structure.len() {
            term_structure[i] = slope;
        }
    }
    
    Ok(Series::new("iv_term_structure", term_structure))
}

/// Calculate implied volatility forecast
///
/// Uses GARCH-like approach to forecast future implied volatility
///
/// # Arguments
/// * `df` - DataFrame with historical implied volatility data
/// * `iv_column` - Name of the implied volatility column
/// * `forecast_period` - Number of periods ahead to forecast
///
/// # Returns
/// * `PolarsResult<Series>` - Series with IV forecast values
pub fn calculate_iv_forecast(
    df: &DataFrame, 
    iv_column: &str,
    forecast_period: usize,
) -> PolarsResult<Series> {
    let iv = df.column(iv_column)?.f64()?;
    let len = df.height();
    let mut iv_forecast = vec![f64::NAN; len];
    
    // Simple model parameters (would be optimized in a full implementation)
    let alpha = 0.1; // Weight for current IV
    let beta = 0.8;  // Weight for long-term IV
    
    // Need at least 30 data points for a reasonable forecast
    if len < 30 {
        return Ok(Series::new("iv_forecast", iv_forecast));
    }
    
    // Calculate long-term average IV
    let mut valid_iv_sum = 0.0;
    let mut valid_iv_count = 0;
    
    for i in 0..len {
        if let Some(val) = iv.get(i) {
            if !val.is_nan() {
                valid_iv_sum += val;
                valid_iv_count += 1;
            }
        }
    }
    
    if valid_iv_count == 0 {
        return Ok(Series::new("iv_forecast", iv_forecast));
    }
    
    let long_term_iv = valid_iv_sum / valid_iv_count as f64;
    
    // Calculate IV forecast
    for i in 29..len {
        let current_iv = iv.get(i).unwrap_or(f64::NAN);
        if current_iv.is_nan() {
            continue;
        }
        
        // Simple mean-reverting forecast model
        let forecast = alpha * current_iv + beta * long_term_iv + (1.0 - alpha - beta) * iv.get(i-1).unwrap_or(current_iv);
        iv_forecast[i] = forecast;
    }
    
    Ok(Series::new("iv_forecast", iv_forecast))
}

/// Add all volatility indicators to the DataFrame
///
/// # Arguments
/// * `df` - DataFrame to add indicators to
///
/// # Returns
/// * `PolarsResult<()>` - Result of the operation
pub fn add_volatility_indicators(df: &mut DataFrame) -> PolarsResult<()> {
    // Check if we have the required IV column
    if !df.schema().contains("iv") {
        return Err(PolarsError::ComputeError(
            "Required column 'iv' not found".into(),
        ));
    }
    
    // Calculate all volatility indicators
    let iv_percentile = calculate_iv_percentile(df, "iv", 252)?;
    df.with_column(iv_percentile)?;
    
    // Only add term structure if we have expiry information
    if df.schema().contains("expiry") {
        let iv_term = calculate_iv_term_structure(df, "iv", "expiry")?;
        df.with_column(iv_term)?;
    }
    
    let iv_forecast = calculate_iv_forecast(df, "iv", 5)?;
    df.with_column(iv_forecast)?;
    
    Ok(())
} 