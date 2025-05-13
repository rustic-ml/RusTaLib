//! Options Spread Analysis
//! 
//! This module provides indicators and utilities for analyzing options spreads
//! including vertical spreads, calendar spreads, and other multi-leg strategies.

use polars::prelude::*;
use polars::frame::DataFrame;
use std::collections::HashMap;

/// Calculate vertical spread values
///
/// Analyzes vertical spread metrics like risk/reward ratio, max profit/loss, etc.
///
/// # Arguments
/// * `df` - DataFrame with options data
/// * `short_strike_column` - Column name for short strike
/// * `long_strike_column` - Column name for long strike
/// * `short_price_column` - Column name for short option price
/// * `long_price_column` - Column name for long option price
/// * `is_call_column` - Column name indicating if spread uses calls (true) or puts (false)
///
/// # Returns
/// * `PolarsResult<DataFrame>` - DataFrame with spread metrics
pub fn calculate_vertical_spread_metrics(
    df: &DataFrame,
    short_strike_column: &str,
    long_strike_column: &str,
    short_price_column: &str,
    long_price_column: &str,
    is_call_column: &str,
) -> PolarsResult<DataFrame> {
    // Extract required columns
    let short_strike = df.column(short_strike_column)?.f64()?;
    let long_strike = df.column(long_strike_column)?.f64()?;
    let short_price = df.column(short_price_column)?.f64()?;
    let long_price = df.column(long_price_column)?.f64()?;
    let is_call = df.column(is_call_column)?.bool()?;
    
    let len = df.height();
    let mut max_profit = vec![f64::NAN; len];
    let mut max_loss = vec![f64::NAN; len];
    let mut breakeven = vec![f64::NAN; len];
    let mut risk_reward = vec![f64::NAN; len];
    let mut strike_width = vec![f64::NAN; len];
    
    for i in 0..len {
        let ss = short_strike.get(i).unwrap_or(f64::NAN);
        let ls = long_strike.get(i).unwrap_or(f64::NAN);
        let sp = short_price.get(i).unwrap_or(f64::NAN);
        let lp = long_price.get(i).unwrap_or(f64::NAN);
        let call = is_call.get(i).unwrap_or(false);
        
        if ss.is_nan() || ls.is_nan() || sp.is_nan() || lp.is_nan() {
            continue;
        }
        
        // Calculate width between strikes
        strike_width[i] = (ss - ls).abs();
        
        // Calculate net premium
        let net_premium = sp - lp;
        
        // Calculate metrics based on call or put vertical
        if call {
            // Call vertical
            if ss > ls {
                // Short call vertical (bear call spread)
                max_profit[i] = net_premium;
                max_loss[i] = strike_width[i] - net_premium;
                breakeven[i] = ls + net_premium;
            } else {
                // Long call vertical (bull call spread)
                max_profit[i] = strike_width[i] - net_premium;
                max_loss[i] = net_premium;
                breakeven[i] = ls + net_premium;
            }
        } else {
            // Put vertical
            if ss > ls {
                // Long put vertical (bear put spread)
                max_profit[i] = strike_width[i] - net_premium;
                max_loss[i] = net_premium;
                breakeven[i] = ss - net_premium;
            } else {
                // Short put vertical (bull put spread)
                max_profit[i] = net_premium;
                max_loss[i] = strike_width[i] - net_premium;
                breakeven[i] = ss - net_premium;
            }
        }
        
        // Calculate risk/reward ratio
        if max_profit[i] > 0.0 && max_loss[i] > 0.0 {
            risk_reward[i] = max_profit[i] / max_loss[i];
        }
    }
    
    // Compile metrics into a DataFrame
    let metrics = vec![
        Series::new("max_profit".into(), max_profit).into(),
        Series::new("max_loss".into(), max_loss).into(),
        Series::new("breakeven".into(), breakeven).into(),
        Series::new("risk_reward".into(), risk_reward).into(),
        Series::new("strike_width".into(), strike_width).into(),
    ];
    
    DataFrame::new(metrics)
}

/// Calculate calendar spread metrics
///
/// Analyzes time spread metrics like time decay advantage, max risk, etc.
///
/// # Arguments
/// * `df` - DataFrame with calendar spread data
/// * `near_price_column` - Column name for near-term option price
/// * `far_price_column` - Column name for far-term option price
/// * `near_iv_column` - Column name for near-term IV
/// * `far_iv_column` - Column name for far-term IV
/// * `near_time_column` - Column name for near-term time to expiry
/// * `far_time_column` - Column name for far-term time to expiry
///
/// # Returns
/// * `PolarsResult<DataFrame>` - DataFrame with calendar spread metrics
pub fn calculate_calendar_spread_metrics(
    df: &DataFrame,
    near_price_column: &str,
    far_price_column: &str,
    near_iv_column: &str,
    far_iv_column: &str,
    near_time_column: &str,
    far_time_column: &str,
) -> PolarsResult<DataFrame> {
    // Extract required columns
    let near_price = df.column(near_price_column)?.f64()?;
    let far_price = df.column(far_price_column)?.f64()?;
    let near_iv = df.column(near_iv_column)?.f64()?;
    let far_iv = df.column(far_iv_column)?.f64()?;
    let near_time = df.column(near_time_column)?.f64()?;
    let far_time = df.column(far_time_column)?.f64()?;
    
    let len = df.height();
    let mut net_debit = vec![f64::NAN; len];
    let mut iv_skew = vec![f64::NAN; len];
    let mut time_decay_advantage = vec![f64::NAN; len];
    let mut theta_ratio = vec![f64::NAN; len];
    let mut expiry_gap = vec![f64::NAN; len];
    
    for i in 0..len {
        let np = near_price.get(i).unwrap_or(f64::NAN);
        let fp = far_price.get(i).unwrap_or(f64::NAN);
        let niv = near_iv.get(i).unwrap_or(f64::NAN);
        let fiv = far_iv.get(i).unwrap_or(f64::NAN);
        let nt = near_time.get(i).unwrap_or(f64::NAN);
        let ft = far_time.get(i).unwrap_or(f64::NAN);
        
        if np.is_nan() || fp.is_nan() || niv.is_nan() || fiv.is_nan() || nt.is_nan() || ft.is_nan() {
            continue;
        }
        
        // Calculate calendar spread metrics
        net_debit[i] = fp - np;
        iv_skew[i] = fiv - niv;
        expiry_gap[i] = ft - nt;
        
        // Calculate approximate theta values (simplified)
        let near_theta = np / (nt * 365.0);
        let far_theta = fp / (ft * 365.0);
        
        // Calculate theta advantages
        if near_theta != 0.0 {
            theta_ratio[i] = far_theta / near_theta;
            time_decay_advantage[i] = near_theta - far_theta;
        }
    }
    
    // Compile metrics into a DataFrame
    let metrics = vec![
        Series::new("net_debit".into(), net_debit).into(),
        Series::new("iv_skew".into(), iv_skew).into(),
        Series::new("time_decay_advantage".into(), time_decay_advantage).into(),
        Series::new("theta_ratio".into(), theta_ratio).into(),
        Series::new("expiry_gap".into(), expiry_gap).into(),
    ];
    
    DataFrame::new(metrics)
}

/// Calculate iron condor metrics
///
/// Analyzes iron condor spread metrics like wings width, body width, etc.
///
/// # Arguments
/// * `df` - DataFrame with iron condor data
/// * `put_short_strike_column` - Column name for put short strike
/// * `put_long_strike_column` - Column name for put long strike
/// * `call_short_strike_column` - Column name for call short strike
/// * `call_long_strike_column` - Column name for call long strike
/// * `put_short_price_column` - Column name for put short price
/// * `put_long_price_column` - Column name for put long price
/// * `call_short_price_column` - Column name for call short price
/// * `call_long_price_column` - Column name for call long price
///
/// # Returns
/// * `PolarsResult<DataFrame>` - DataFrame with iron condor metrics
pub fn calculate_iron_condor_metrics(
    df: &DataFrame,
    put_short_strike_column: &str,
    put_long_strike_column: &str,
    call_short_strike_column: &str,
    call_long_strike_column: &str,
    put_short_price_column: &str,
    put_long_price_column: &str,
    call_short_price_column: &str,
    call_long_price_column: &str,
) -> PolarsResult<DataFrame> {
    // Extract required columns
    let put_short_strike = df.column(put_short_strike_column)?.f64()?;
    let put_long_strike = df.column(put_long_strike_column)?.f64()?;
    let call_short_strike = df.column(call_short_strike_column)?.f64()?;
    let call_long_strike = df.column(call_long_strike_column)?.f64()?;
    let put_short_price = df.column(put_short_price_column)?.f64()?;
    let put_long_price = df.column(put_long_price_column)?.f64()?;
    let call_short_price = df.column(call_short_price_column)?.f64()?;
    let call_long_price = df.column(call_long_price_column)?.f64()?;
    
    let len = df.height();
    let mut max_profit = vec![f64::NAN; len];
    let mut max_loss = vec![f64::NAN; len];
    let mut put_breakeven = vec![f64::NAN; len];
    let mut call_breakeven = vec![f64::NAN; len];
    let mut body_width = vec![f64::NAN; len];
    let mut put_wing_width = vec![f64::NAN; len];
    let mut call_wing_width = vec![f64::NAN; len];
    let mut profit_probability = vec![f64::NAN; len];
    
    for i in 0..len {
        let pss = put_short_strike.get(i).unwrap_or(f64::NAN);
        let pls = put_long_strike.get(i).unwrap_or(f64::NAN);
        let css = call_short_strike.get(i).unwrap_or(f64::NAN);
        let cls = call_long_strike.get(i).unwrap_or(f64::NAN);
        let psp = put_short_price.get(i).unwrap_or(f64::NAN);
        let plp = put_long_price.get(i).unwrap_or(f64::NAN);
        let csp = call_short_price.get(i).unwrap_or(f64::NAN);
        let clp = call_long_price.get(i).unwrap_or(f64::NAN);
        
        if pss.is_nan() || pls.is_nan() || css.is_nan() || cls.is_nan() || 
           psp.is_nan() || plp.is_nan() || csp.is_nan() || clp.is_nan() {
            continue;
        }
        
        // Calculate net premium collected
        let net_premium = (psp - plp) + (csp - clp);
        
        // Calculate wing widths
        put_wing_width[i] = pss - pls;
        call_wing_width[i] = cls - css;
        
        // Calculate body width (distance between short strikes)
        body_width[i] = css - pss;
        
        // Calculate max profit and loss
        max_profit[i] = net_premium;
        
        // Max loss occurs when price moves beyond long strikes
        // Use minimum of the wing widths for calculating max loss
        let max_loss_width = put_wing_width[i].min(call_wing_width[i]);
        max_loss[i] = max_loss_width - net_premium;
        
        // Calculate breakeven points
        put_breakeven[i] = pss - net_premium;
        call_breakeven[i] = css + net_premium;
        
        // Calculate approximate probability of profit
        // (body width + net premium) / (total width)
        let total_width = cls - pls;
        if total_width > 0.0 {
            profit_probability[i] = (body_width[i] + net_premium) / total_width;
            // Clamp probability between 0 and 1
            profit_probability[i] = profit_probability[i].min(1.0).max(0.0);
        }
    }
    
    // Compile metrics into a DataFrame
    let metrics = vec![
        Series::new("max_profit".into(), max_profit).into(),
        Series::new("max_loss".into(), max_loss).into(),
        Series::new("put_breakeven".into(), put_breakeven).into(),
        Series::new("call_breakeven".into(), call_breakeven).into(),
        Series::new("body_width".into(), body_width).into(),
        Series::new("put_wing_width".into(), put_wing_width).into(),
        Series::new("call_wing_width".into(), call_wing_width).into(),
        Series::new("profit_probability".into(), profit_probability).into(),
    ];
    
    DataFrame::new(metrics)
}

/// Add all spread indicators to the DataFrame
///
/// # Arguments
/// * `df` - DataFrame to add indicators to
///
/// # Returns
/// * `PolarsResult<()>` - Result of the operation
pub fn add_spread_indicators(df: &mut DataFrame) -> PolarsResult<()> {
    // This function is a placeholder for the actual implementation
    // In a real implementation, we would check what kind of spread data
    // is in the dataframe and calculate appropriate metrics
    
    // For example, if the dataframe contains vertical spread data:
    if df.schema().contains("short_strike") && df.schema().contains("long_strike") &&
       df.schema().contains("short_price") && df.schema().contains("long_price") &&
       df.schema().contains("is_call") {
        
        let spread_metrics = calculate_vertical_spread_metrics(
            df, "short_strike", "long_strike", "short_price", "long_price", "is_call"
        )?;
        
        // Add metrics to original dataframe
        for col in spread_metrics.get_columns() {
            df.with_column(col.clone())?;
        }
    }
    
    // If the dataframe contains calendar spread data:
    if df.schema().contains("near_price") && df.schema().contains("far_price") &&
       df.schema().contains("near_iv") && df.schema().contains("far_iv") &&
       df.schema().contains("near_time") && df.schema().contains("far_time") {
        
        let calendar_metrics = calculate_calendar_spread_metrics(
            df, "near_price", "far_price", "near_iv", "far_iv", "near_time", "far_time"
        )?;
        
        // Add metrics to original dataframe
        for col in calendar_metrics.get_columns() {
            df.with_column(col.clone())?;
        }
    }
    
    // If the dataframe contains iron condor data:
    if df.schema().contains("put_short_strike") && df.schema().contains("call_short_strike") {
        
        let condor_metrics = calculate_iron_condor_metrics(
            df, 
            "put_short_strike", "put_long_strike", 
            "call_short_strike", "call_long_strike",
            "put_short_price", "put_long_price",
            "call_short_price", "call_long_price"
        )?;
        
        // Add metrics to original dataframe
        for col in condor_metrics.get_columns() {
            df.with_column(col.clone())?;
        }
    }
    
    Ok(())
} 