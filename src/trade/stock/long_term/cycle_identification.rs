use polars::prelude::*;
use crate::indicators::moving_averages::{calculate_sma, calculate_ema};
use crate::indicators::oscillators::{calculate_rsi, calculate_stochastic};

/// Identify market cycle phases
///
/// This function identifies the current market cycle phase:
/// 1. Accumulation (basing after downtrend)
/// 2. Markup (uptrend)
/// 3. Distribution (topping after uptrend)
/// 4. Markdown (downtrend)
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `long_ma_period` - Long-term moving average period (default: 200)
/// * `medium_ma_period` - Medium-term moving average period (default: 50)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with cycle phases (1-4)
pub fn identify_market_cycle_phase(
    df: &DataFrame,
    long_ma_period: Option<usize>,
    medium_ma_period: Option<usize>,
) -> PolarsResult<Series> {
    let long_period = long_ma_period.unwrap_or(200);
    let medium_period = medium_ma_period.unwrap_or(50);
    
    // Calculate moving averages
    let long_ma = calculate_sma(df, "close", long_period)?;
    let medium_ma = calculate_sma(df, "close", medium_period)?;
    
    let long_ma_vals = long_ma.f64()?;
    let medium_ma_vals = medium_ma.f64()?;
    
    // Get price data
    let close = df.column("close")?.f64()?;
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    
    // Calculate RSI for confirmation
    let rsi = calculate_rsi(df, 14, "close")?;
    let rsi_vals = rsi.f64()?;
    
    let mut cycle_phase = Vec::with_capacity(df.height());
    
    // First values will be undefined until we have enough data
    let min_periods = long_period;
    for i in 0..min_periods.min(df.height()) {
        cycle_phase.push(0); // 0 means undefined phase
    }
    
    // Identify cycle phase for each point
    for i in min_periods..df.height() {
        let long_ma_val = long_ma_vals.get(i).unwrap_or(f64::NAN);
        let medium_ma_val = medium_ma_vals.get(i).unwrap_or(f64::NAN);
        let close_val = close.get(i).unwrap_or(f64::NAN);
        let rsi_val = rsi_vals.get(i).unwrap_or(f64::NAN);
        
        if long_ma_val.is_nan() || medium_ma_val.is_nan() || close_val.is_nan() || rsi_val.is_nan() {
            cycle_phase.push(0);
            continue;
        }
        
        // Calculate long MA slope (20-bar lookback)
        let lookback = 20.min(i);
        let long_ma_prev = long_ma_vals.get(i - lookback).unwrap_or(long_ma_val);
        let long_slope = (long_ma_val - long_ma_prev) / long_ma_prev * 100.0;
        
        // Check volatility contraction or expansion
        let mut volatility_ratio = 0.0;
        let atr_lookback = 20.min(i);
        
        // Calculate recent ATR
        let mut recent_atr_sum = 0.0;
        for j in (i - atr_lookback + 1)..=i {
            let h = high.get(j).unwrap_or(f64::NAN);
            let l = low.get(j).unwrap_or(f64::NAN);
            let c_prev = close.get(j - 1).unwrap_or(f64::NAN);
            
            if h.is_nan() || l.is_nan() || c_prev.is_nan() {
                continue;
            }
            
            let tr = (h - l).max((h - c_prev).abs()).max((l - c_prev).abs());
            recent_atr_sum += tr;
        }
        
        // Calculate older ATR
        let mut older_atr_sum = 0.0;
        for j in (i - 2 * atr_lookback + 1)..(i - atr_lookback + 1) {
            if j < 0 {
                continue;
            }
            
            let h = high.get(j).unwrap_or(f64::NAN);
            let l = low.get(j).unwrap_or(f64::NAN);
            let c_prev = close.get(j - 1).unwrap_or(f64::NAN);
            
            if h.is_nan() || l.is_nan() || c_prev.is_nan() {
                continue;
            }
            
            let tr = (h - l).max((h - c_prev).abs()).max((l - c_prev).abs());
            older_atr_sum += tr;
        }
        
        // Calculate volatility ratio
        if older_atr_sum > 0.0 {
            volatility_ratio = recent_atr_sum / older_atr_sum;
        }
        
        // Identify phase based on indicators
        if close_val > long_ma_val && close_val > medium_ma_val && long_slope > 0.0 {
            // Markup phase (uptrend)
            cycle_phase.push(2);
        } else if close_val < long_ma_val && close_val < medium_ma_val && long_slope < 0.0 {
            // Markdown phase (downtrend)
            cycle_phase.push(4);
        } else if close_val < long_ma_val && medium_ma_val < long_ma_val && 
                  long_slope <= 0.1 && long_slope >= -0.3 && volatility_ratio < 0.8 {
            // Accumulation phase (basing after downtrend)
            cycle_phase.push(1);
        } else if close_val > long_ma_val && medium_ma_val > long_ma_val && 
                  long_slope <= 0.3 && long_slope >= -0.1 && volatility_ratio < 0.8 {
            // Distribution phase (topping after uptrend)
            cycle_phase.push(3);
        } else {
            // Transition between phases or unclear
            // Use previous phase if available
            if i > 0 && cycle_phase[i - 1] != 0 {
                cycle_phase.push(cycle_phase[i - 1]);
            } else {
                cycle_phase.push(0);
            }
        }
    }
    
    Ok(Series::new("cycle_phase", cycle_phase))
}

/// Calculate cycle position percentage
///
/// This function estimates the current position within a market cycle
/// as a percentage (0-100), where 0% is the beginning and 100% is the end.
///
/// # Arguments
///
/// * `df` - DataFrame with cycle_phase already calculated
/// * `cycle_length` - Estimated cycle length in bars (default: 250)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with cycle position percentage
pub fn calculate_cycle_position(
    df: &DataFrame,
    cycle_length: Option<usize>,
) -> PolarsResult<Series> {
    let estimated_length = cycle_length.unwrap_or(250);
    
    // Check if cycle phase is already calculated
    if !df.schema().contains("cycle_phase") {
        return Err(PolarsError::ComputeError(
            "cycle_phase column not found. Calculate cycle phase first.".into(),
        ));
    }
    
    let phase = df.column("cycle_phase")?.i32()?;
    let mut position_pct = Vec::with_capacity(df.height());
    
    // Count how long we've been in the current phase
    let mut current_phase = 0;
    let mut phase_duration = 0;
    
    for i in 0..df.height() {
        let current = phase.get(i).unwrap_or(0);
        
        if current == 0 {
            // Unknown phase, use default 50%
            position_pct.push(50.0);
            continue;
        }
        
        if current != current_phase {
            // Phase transition
            current_phase = current;
            phase_duration = 1;
        } else {
            // Continue in same phase
            phase_duration += 1;
        }
        
        // Calculate position within cycle based on current phase and duration
        match current {
            1 => { // Accumulation
                // Typical accumulation lasts about 25% of the cycle
                let pct = (phase_duration as f64 / (estimated_length as f64 * 0.25)).min(1.0) * 25.0;
                position_pct.push(pct);
            },
            2 => { // Markup
                // Typical markup lasts about 30% of the cycle
                let pct = 25.0 + (phase_duration as f64 / (estimated_length as f64 * 0.3)).min(1.0) * 30.0;
                position_pct.push(pct);
            },
            3 => { // Distribution
                // Typical distribution lasts about 20% of the cycle
                let pct = 55.0 + (phase_duration as f64 / (estimated_length as f64 * 0.2)).min(1.0) * 20.0;
                position_pct.push(pct);
            },
            4 => { // Markdown
                // Typical markdown lasts about 25% of the cycle
                let pct = 75.0 + (phase_duration as f64 / (estimated_length as f64 * 0.25)).min(1.0) * 25.0;
                position_pct.push(pct);
            },
            _ => position_pct.push(50.0), // Default to middle
        }
    }
    
    Ok(Series::new("cycle_position", position_pct))
}

/// Calculate cycle trend strength
///
/// This function measures how strongly the price action confirms
/// the current market cycle phase.
///
/// # Arguments
///
/// * `df` - DataFrame with cycle_phase already calculated
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with cycle confirmation strength (0-100)
pub fn calculate_cycle_confirmation(df: &DataFrame) -> PolarsResult<Series> {
    // Check if cycle phase is already calculated
    if !df.schema().contains("cycle_phase") {
        return Err(PolarsError::ComputeError(
            "cycle_phase column not found. Calculate cycle phase first.".into(),
        ));
    }
    
    let phase = df.column("cycle_phase")?.i32()?;
    
    // Calculate technical indicators for confirmation
    let rsi = calculate_rsi(df, 14, "close")?;
    let (stoch_k, _) = calculate_stochastic(df, 14, 3, None)?;
    
    let long_ma = calculate_sma(df, "close", 200)?;
    let short_ma = calculate_sma(df, "close", 50)?;
    
    // Get values
    let rsi_vals = rsi.f64()?;
    let stoch_vals = stoch_k.f64()?;
    let long_ma_vals = long_ma.f64()?;
    let short_ma_vals = short_ma.f64()?;
    let close = df.column("close")?.f64()?;
    
    let mut confirmation = Vec::with_capacity(df.height());
    
    // First values will have no confirmation until we have enough data
    let min_periods = 200;
    for i in 0..min_periods.min(df.height()) {
        confirmation.push(0.0);
    }
    
    // Calculate confirmation for each point
    for i in min_periods..df.height() {
        let current_phase = phase.get(i).unwrap_or(0);
        let rsi_val = rsi_vals.get(i).unwrap_or(f64::NAN);
        let stoch_val = stoch_vals.get(i).unwrap_or(f64::NAN);
        let long_ma_val = long_ma_vals.get(i).unwrap_or(f64::NAN);
        let short_ma_val = short_ma_vals.get(i).unwrap_or(f64::NAN);
        let close_val = close.get(i).unwrap_or(f64::NAN);
        
        if current_phase == 0 || rsi_val.is_nan() || stoch_val.is_nan() || 
           long_ma_val.is_nan() || short_ma_val.is_nan() || close_val.is_nan() {
            confirmation.push(0.0);
            continue;
        }
        
        // Calculate MA slopes
        let lookback = 20.min(i);
        let long_ma_prev = long_ma_vals.get(i - lookback).unwrap_or(long_ma_val);
        let long_slope = (long_ma_val - long_ma_prev) / long_ma_prev * 100.0;
        
        let short_ma_prev = short_ma_vals.get(i - lookback).unwrap_or(short_ma_val);
        let short_slope = (short_ma_val - short_ma_prev) / short_ma_prev * 100.0;
        
        // Base confirmation score
        let mut confirm_score = 50.0;
        
        match current_phase {
            1 => { // Accumulation
                // Accumulation should show RSI starting to rise from oversold,
                // price basing near lows, and decreasing downward momentum
                
                // Check if RSI is rising from oversold
                if rsi_val > 30.0 && rsi_val < 50.0 {
                    confirm_score += 10.0;
                }
                
                // Check if price is stabilizing (reduced volatility)
                if short_slope.abs() < 0.3 && long_slope.abs() < 0.2 {
                    confirm_score += 15.0;
                }
                
                // Check if Stochastic is rising from oversold
                if stoch_val > 20.0 && stoch_val < 50.0 {
                    confirm_score += 10.0;
                }
                
                // Penalty for strong downtrend continuation
                if long_slope < -0.5 {
                    confirm_score -= 15.0;
                }
            },
            2 => { // Markup
                // Markup should show rising RSI, price above MAs, and positive slopes
                
                // Check if price is above MAs
                if close_val > short_ma_val && short_ma_val > long_ma_val {
                    confirm_score += 15.0;
                }
                
                // Check if slopes are positive
                if short_slope > 0.3 && long_slope > 0.1 {
                    confirm_score += 15.0;
                }
                
                // Check if RSI is strong
                if rsi_val > 50.0 && rsi_val < 80.0 {
                    confirm_score += 10.0;
                }
                
                // Penalty for overbought conditions that might lead to correction
                if rsi_val > 80.0 && stoch_val > 80.0 {
                    confirm_score -= 10.0;
                }
            },
            3 => { // Distribution
                // Distribution should show weakening momentum, bearish divergences,
                // lower highs/lows but still near highs
                
                // Check if RSI is weakening from overbought
                if rsi_val < 70.0 && rsi_val > 50.0 {
                    confirm_score += 10.0;
                }
                
                // Check if price is still above long MA but momentum slowing
                if close_val > long_ma_val && short_slope < 0.2 && short_slope > -0.2 {
                    confirm_score += 15.0;
                }
                
                // Check if stochastic is showing weakness
                if stoch_val < 80.0 && stoch_val > 40.0 {
                    confirm_score += 10.0;
                }
                
                // Penalty for strong uptrend continuation
                if short_slope > 0.5 && long_slope > 0.3 {
                    confirm_score -= 15.0;
                }
            },
            4 => { // Markdown
                // Markdown should show declining RSI, price below MAs, and negative slopes
                
                // Check if price is below MAs
                if close_val < short_ma_val && short_ma_val < long_ma_val {
                    confirm_score += 15.0;
                }
                
                // Check if slopes are negative
                if short_slope < -0.3 && long_slope < -0.1 {
                    confirm_score += 15.0;
                }
                
                // Check if RSI is weak
                if rsi_val < 50.0 && rsi_val > 20.0 {
                    confirm_score += 10.0;
                }
                
                // Penalty for oversold conditions that might lead to bounce
                if rsi_val < 20.0 && stoch_val < 20.0 {
                    confirm_score -= 10.0;
                }
            },
            _ => {}
        }
        
        // Ensure score is within bounds
        confirmation.push(confirm_score.max(0.0).min(100.0));
    }
    
    Ok(Series::new("cycle_confirmation", confirmation))
}

/// Add cycle analysis to DataFrame
///
/// # Arguments
///
/// * `df` - Mutable reference to DataFrame
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_cycle_analysis(df: &mut DataFrame) -> PolarsResult<()> {
    let cycle_phase = identify_market_cycle_phase(df, None, None)?;
    df.with_column(cycle_phase)?;
    
    let cycle_position = calculate_cycle_position(df, None)?;
    df.with_column(cycle_position)?;
    
    let cycle_confirmation = calculate_cycle_confirmation(df)?;
    df.with_column(cycle_confirmation)?;
    
    Ok(())
} 