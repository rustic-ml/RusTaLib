//! Options Volume Analysis
//! 
//! This module provides indicators and utilities for analyzing options trading volume,
//! open interest, and related metrics.

use polars::prelude::*;
use polars::frame::DataFrame;
use std::collections::HashMap;

/// Calculate volume to open interest ratio
///
/// Measures the amount of trading activity relative to outstanding contracts.
/// High ratios indicate increased activity and potential price movement.
///
/// # Arguments
/// * `df` - DataFrame with options data
/// * `volume_column` - Column name for trading volume
/// * `open_interest_column` - Column name for open interest
///
/// # Returns
/// * `PolarsResult<Series>` - Series with volume/OI ratio values
pub fn calculate_volume_oi_ratio(
    df: &DataFrame,
    volume_column: &str,
    open_interest_column: &str,
) -> PolarsResult<Series> {
    // Extract required columns
    let volume = df.column(volume_column)?.f64()?;
    let open_interest = df.column(open_interest_column)?.f64()?;
    
    let len = df.height();
    let mut ratio = vec![f64::NAN; len];
    
    for i in 0..len {
        let vol = volume.get(i).unwrap_or(f64::NAN);
        let oi = open_interest.get(i).unwrap_or(f64::NAN);
        
        if vol.is_nan() || oi.is_nan() || oi <= 0.0 {
            continue;
        }
        
        ratio[i] = vol / oi;
    }
    
    Ok(Series::new("volume_oi_ratio", ratio))
}

/// Calculate put-call volume ratio
///
/// Measures the ratio of put option volume to call option volume.
/// Used as a sentiment indicator - high values may indicate bearish sentiment.
///
/// # Arguments
/// * `df` - DataFrame with options data
/// * `volume_column` - Column name for trading volume
/// * `is_call_column` - Column name indicating if option is a call (true) or put (false)
/// * `window` - Rolling window size for calculation
///
/// # Returns
/// * `PolarsResult<Series>` - Series with put/call volume ratio values
pub fn calculate_put_call_ratio(
    df: &DataFrame,
    volume_column: &str,
    is_call_column: &str,
    window: usize,
) -> PolarsResult<Series> {
    // Extract required columns
    let volume = df.column(volume_column)?.f64()?;
    let is_call = df.column(is_call_column)?.bool()?;
    
    let len = df.height();
    let mut put_call_ratio = vec![f64::NAN; len];
    
    // Calculate put and call volumes for each time period
    let mut call_volumes = Vec::with_capacity(len);
    let mut put_volumes = Vec::with_capacity(len);
    
    for i in 0..len {
        let vol = volume.get(i).unwrap_or(f64::NAN);
        let call = is_call.get(i).unwrap_or(false);
        
        if vol.is_nan() {
            call_volumes.push(0.0);
            put_volumes.push(0.0);
            continue;
        }
        
        if call {
            call_volumes.push(vol);
            put_volumes.push(0.0);
        } else {
            call_volumes.push(0.0);
            put_volumes.push(vol);
        }
    }
    
    // Calculate rolling put/call ratio
    for i in 0..len {
        if i < window - 1 {
            continue;
        }
        
        let mut call_sum = 0.0;
        let mut put_sum = 0.0;
        
        for j in (i - (window - 1))..=i {
            call_sum += call_volumes[j];
            put_sum += put_volumes[j];
        }
        
        if call_sum > 0.0 {
            put_call_ratio[i] = put_sum / call_sum;
        }
    }
    
    Ok(Series::new("put_call_ratio", put_call_ratio))
}

/// Calculate unusual options activity indicator
///
/// Identifies options with unusually high volume relative to historical averages
/// and open interest, which may indicate informed trading.
///
/// # Arguments
/// * `df` - DataFrame with options data
/// * `volume_column` - Column name for trading volume
/// * `avg_volume_column` - Column name for average historical volume
/// * `open_interest_column` - Column name for open interest
///
/// # Returns
/// * `PolarsResult<Series>` - Series with unusual activity scores (higher = more unusual)
pub fn calculate_unusual_activity(
    df: &DataFrame,
    volume_column: &str,
    avg_volume_column: &str,
    open_interest_column: &str,
) -> PolarsResult<Series> {
    // Extract required columns
    let volume = df.column(volume_column)?.f64()?;
    let avg_volume = df.column(avg_volume_column)?.f64()?;
    let open_interest = df.column(open_interest_column)?.f64()?;
    
    let len = df.height();
    let mut unusual_score = vec![f64::NAN; len];
    
    for i in 0..len {
        let vol = volume.get(i).unwrap_or(f64::NAN);
        let avg_vol = avg_volume.get(i).unwrap_or(f64::NAN);
        let oi = open_interest.get(i).unwrap_or(f64::NAN);
        
        if vol.is_nan() || avg_vol.is_nan() || oi.is_nan() || avg_vol <= 0.0 || oi <= 0.0 {
            continue;
        }
        
        // Calculate volume multiple relative to average
        let volume_multiple = vol / avg_vol;
        
        // Calculate volume to open interest ratio
        let vol_oi_ratio = vol / oi;
        
        // Combine into unusual activity score
        // Higher score = more unusual
        unusual_score[i] = volume_multiple * vol_oi_ratio;
    }
    
    Ok(Series::new("unusual_activity", unusual_score))
}

/// Calculate open interest change
///
/// Tracks the daily change in open interest to identify accumulation or distribution
/// of options positions.
///
/// # Arguments
/// * `df` - DataFrame with options data
/// * `open_interest_column` - Column name for open interest
///
/// # Returns
/// * `PolarsResult<Series>` - Series with OI change values
pub fn calculate_oi_change(
    df: &DataFrame,
    open_interest_column: &str,
) -> PolarsResult<Series> {
    // Extract required column
    let open_interest = df.column(open_interest_column)?.f64()?;
    
    let len = df.height();
    let mut oi_change = vec![f64::NAN; len];
    
    for i in 1..len {
        let current_oi = open_interest.get(i).unwrap_or(f64::NAN);
        let prev_oi = open_interest.get(i-1).unwrap_or(f64::NAN);
        
        if current_oi.is_nan() || prev_oi.is_nan() || prev_oi <= 0.0 {
            continue;
        }
        
        // Calculate absolute change in OI
        let absolute_change = current_oi - prev_oi;
        
        // Calculate percentage change
        oi_change[i] = (absolute_change / prev_oi) * 100.0;
    }
    
    Ok(Series::new("oi_change_pct", oi_change))
}

/// Calculate money flow indicator for options
///
/// Measures the dollar value of options flow to identify where money is flowing
/// in the options market (bullish or bearish).
///
/// # Arguments
/// * `df` - DataFrame with options data
/// * `volume_column` - Column name for trading volume
/// * `price_column` - Column name for option price
/// * `is_call_column` - Column name indicating if option is a call (true) or put (false)
/// * `buy_probability_column` - Column name with probability trade was a buy (0.0-1.0)
///
/// # Returns
/// * `PolarsResult<Series>` - Series with money flow values (positive = bullish, negative = bearish)
pub fn calculate_options_money_flow(
    df: &DataFrame,
    volume_column: &str,
    price_column: &str,
    is_call_column: &str,
    buy_probability_column: &str,
) -> PolarsResult<Series> {
    // Extract required columns
    let volume = df.column(volume_column)?.f64()?;
    let price = df.column(price_column)?.f64()?;
    let is_call = df.column(is_call_column)?.bool()?;
    let buy_probability = df.column(buy_probability_column)?.f64()?;
    
    let len = df.height();
    let mut money_flow = vec![f64::NAN; len];
    
    for i in 0..len {
        let vol = volume.get(i).unwrap_or(f64::NAN);
        let pr = price.get(i).unwrap_or(f64::NAN);
        let call = is_call.get(i).unwrap_or(false);
        let buy_prob = buy_probability.get(i).unwrap_or(f64::NAN);
        
        if vol.is_nan() || pr.is_nan() || buy_prob.is_nan() {
            continue;
        }
        
        // Calculate dollar value
        let dollar_value = vol * pr * 100.0; // Assuming standard 100 multiplier
        
        // Determine direction based on option type and buy/sell
        let direction = if call {
            // Calls: buying = bullish, selling = bearish
            buy_prob * 2.0 - 1.0
        } else {
            // Puts: buying = bearish, selling = bullish
            1.0 - buy_prob * 2.0
        };
        
        // Calculate flow
        money_flow[i] = dollar_value * direction;
    }
    
    Ok(Series::new("options_money_flow", money_flow))
}

/// Add all volume indicators to the DataFrame
///
/// # Arguments
/// * `df` - DataFrame to add indicators to
///
/// # Returns
/// * `PolarsResult<()>` - Result of the operation
pub fn add_volume_indicators(df: &mut DataFrame) -> PolarsResult<()> {
    // Check if we have essential volume and open interest columns
    if df.schema().contains("volume") && df.schema().contains("open_interest") {
        // Add volume/OI ratio
        let vol_oi = calculate_volume_oi_ratio(df, "volume", "open_interest")?;
        df.with_column(vol_oi)?;
        
        // Add OI change if we have time-series data
        let oi_change = calculate_oi_change(df, "open_interest")?;
        df.with_column(oi_change)?;
        
        // Add put/call ratio if we have option type data
        if df.schema().contains("is_call") {
            let put_call = calculate_put_call_ratio(df, "volume", "is_call", 5)?;
            df.with_column(put_call)?;
        }
        
        // Add unusual activity if we have average volume data
        if df.schema().contains("avg_volume") {
            let unusual = calculate_unusual_activity(df, "volume", "avg_volume", "open_interest")?;
            df.with_column(unusual)?;
        }
        
        // Add money flow if we have all required data
        if df.schema().contains("price") && df.schema().contains("is_call") && 
           df.schema().contains("buy_probability") {
            let money_flow = calculate_options_money_flow(
                df, "volume", "price", "is_call", "buy_probability"
            )?;
            df.with_column(money_flow)?;
        }
    }
    
    Ok(())
} 