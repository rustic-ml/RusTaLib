use polars::prelude::*;
use crate::indicators::oscillators::{calculate_rsi, calculate_stochastic};
use crate::indicators::moving_averages::{calculate_ema, calculate_sma};

/// Detect swing trading opportunities
///
/// This function identifies potential swing entry points by detecting
/// pullbacks within a trend that meet specific criteria optimized for
/// multi-day holding periods.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `trend_ma_period` - Period for trend MA (default: 50)
/// * `pullback_threshold` - Minimum pullback percentage (default: 3.0)
/// * `rsi_period` - RSI period (default: 14)
/// * `stoch_period` - Stochastic period (default: 14)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with swing signals (1: buy, -1: sell, 0: no signal)
pub fn detect_swing_opportunities(
    df: &DataFrame,
    trend_ma_period: Option<usize>,
    pullback_threshold: Option<f64>,
    rsi_period: Option<usize>,
    stoch_period: Option<usize>,
) -> PolarsResult<Series> {
    let ma_period = trend_ma_period.unwrap_or(50);
    let pullback_pct = pullback_threshold.unwrap_or(3.0);
    let rsi_len = rsi_period.unwrap_or(14);
    let stoch_len = stoch_period.unwrap_or(14);
    
    // Calculate indicators
    let trend_ma = calculate_ema(df, "close", ma_period)?;
    let rsi = calculate_rsi(df, rsi_len, "close")?;
    let (stoch_k, _) = calculate_stochastic(df, stoch_len, 3, None)?;
    
    // Get price data
    let close = df.column("close")?.f64()?;
    let low = df.column("low")?.f64()?;
    let high = df.column("high")?.f64()?;
    
    // Extract indicator values
    let ma_vals = trend_ma.f64()?;
    let rsi_vals = rsi.f64()?;
    let stoch_vals = stoch_k.f64()?;
    
    let mut swing_signals = Vec::with_capacity(df.height());
    
    // We need some history to detect swings
    let lookback = 5; // Look back 5 bars for local extremes
    let min_periods = ma_period.max(rsi_len).max(stoch_len) + lookback;
    
    // Fill initial values with no signal
    for i in 0..min_periods.min(df.height()) {
        swing_signals.push(0);
    }
    
    // Scan for swing opportunities
    for i in min_periods..df.height() {
        let ma_val = ma_vals.get(i).unwrap_or(f64::NAN);
        let close_val = close.get(i).unwrap_or(f64::NAN);
        let rsi_val = rsi_vals.get(i).unwrap_or(f64::NAN);
        let stoch_val = stoch_vals.get(i).unwrap_or(f64::NAN);
        
        if ma_val.is_nan() || close_val.is_nan() || rsi_val.is_nan() || stoch_val.is_nan() {
            swing_signals.push(0);
            continue;
        }
        
        // Determine trend direction
        let trend_direction = if close_val > ma_val { 1 } else { -1 };
        
        // Calculate pullback percentage from recent extreme
        let mut recent_extreme = close_val;
        if trend_direction > 0 {
            // In uptrend, look for recent high
            for j in (i - lookback)..i {
                let h = high.get(j).unwrap_or(f64::NAN);
                if !h.is_nan() && h > recent_extreme {
                    recent_extreme = h;
                }
            }
        } else {
            // In downtrend, look for recent low
            recent_extreme = low.get(i).unwrap_or(f64::NAN);
            for j in (i - lookback)..i {
                let l = low.get(j).unwrap_or(f64::NAN);
                if !l.is_nan() && l < recent_extreme {
                    recent_extreme = l;
                }
            }
        }
        
        // Calculate pullback percentage
        let pullback = if trend_direction > 0 {
            // In uptrend, pullback is downward
            ((recent_extreme - close_val) / recent_extreme * 100.0).abs()
        } else {
            // In downtrend, pullback is upward
            ((close_val - recent_extreme) / recent_extreme * 100.0).abs()
        };
        
        // Generate signal based on conditions
        if trend_direction > 0 && pullback >= pullback_pct {
            // Bullish swing opportunity in uptrend
            // Check for oversold conditions in RSI and Stochastic
            if rsi_val < 40.0 && stoch_val < 30.0 {
                swing_signals.push(1); // Buy signal
            } else {
                swing_signals.push(0); // No signal
            }
        } else if trend_direction < 0 && pullback >= pullback_pct {
            // Bearish swing opportunity in downtrend
            // Check for overbought conditions in RSI and Stochastic
            if rsi_val > 60.0 && stoch_val > 70.0 {
                swing_signals.push(-1); // Sell signal
            } else {
                swing_signals.push(0); // No signal
            }
        } else {
            swing_signals.push(0); // No signal
        }
    }
    
    Ok(Series::new("swing_signal", swing_signals))
}

/// Calculate swing risk level
///
/// This function assesses the risk level of a swing trade based on
/// volatility, trend strength, and distance from key support/resistance.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `lookback_period` - Lookback period for volatility calculation (default: 20)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with risk levels (1: low, 2: medium, 3: high)
pub fn calculate_swing_risk_level(
    df: &DataFrame,
    lookback_period: Option<usize>,
) -> PolarsResult<Series> {
    let lookback = lookback_period.unwrap_or(20);
    
    // Calculate Average True Range for volatility measurement
    // ATR formula: (high - low, |high - prev_close|, |low - prev_close|)
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    
    let mut atr_values = Vec::with_capacity(df.height());
    atr_values.push(high.get(0).unwrap_or(f64::NAN) - low.get(0).unwrap_or(f64::NAN));
    
    for i in 1..df.height() {
        let h = high.get(i).unwrap_or(f64::NAN);
        let l = low.get(i).unwrap_or(f64::NAN);
        let c_prev = close.get(i - 1).unwrap_or(f64::NAN);
        
        if h.is_nan() || l.is_nan() || c_prev.is_nan() {
            atr_values.push(f64::NAN);
            continue;
        }
        
        let tr = (h - l)
            .max((h - c_prev).abs())
            .max((l - c_prev).abs());
        
        atr_values.push(tr);
    }
    
    // Calculate average ATR
    let mut risk_levels = Vec::with_capacity(df.height());
    
    // Fill initial values with medium risk
    for i in 0..lookback.min(df.height()) {
        risk_levels.push(2);
    }
    
    // Calculate risk levels for each point
    for i in lookback..df.height() {
        // Calculate average ATR over lookback period
        let mut atr_sum = 0.0;
        let mut atr_count = 0;
        
        for j in (i - lookback)..i {
            let atr_val = atr_values[j];
            if !atr_val.is_nan() {
                atr_sum += atr_val;
                atr_count += 1;
            }
        }
        
        let avg_atr = if atr_count > 0 { atr_sum / atr_count as f64 } else { f64::NAN };
        
        if avg_atr.is_nan() {
            risk_levels.push(2); // Default to medium risk
            continue;
        }
        
        // Calculate average price for normalization
        let mut price_sum = 0.0;
        let mut price_count = 0;
        
        for j in (i - lookback)..i {
            let c = close.get(j).unwrap_or(f64::NAN);
            if !c.is_nan() {
                price_sum += c;
                price_count += 1;
            }
        }
        
        let avg_price = if price_count > 0 { price_sum / price_count as f64 } else { f64::NAN };
        
        if avg_price.is_nan() || avg_price == 0.0 {
            risk_levels.push(2); // Default to medium risk
            continue;
        }
        
        // Normalized ATR (as percentage of price)
        let norm_atr = avg_atr / avg_price * 100.0;
        
        // Determine risk level based on normalized ATR
        if norm_atr < 1.0 {
            risk_levels.push(1); // Low risk
        } else if norm_atr < 3.0 {
            risk_levels.push(2); // Medium risk
        } else {
            risk_levels.push(3); // High risk
        }
    }
    
    Ok(Series::new("swing_risk_level", risk_levels))
}

/// Add swing detection analysis to DataFrame
///
/// # Arguments
///
/// * `df` - Mutable reference to DataFrame
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_swing_analysis(df: &mut DataFrame) -> PolarsResult<()> {
    let swing_signal = detect_swing_opportunities(df, None, None, None, None)?;
    let risk_level = calculate_swing_risk_level(df, None)?;
    
    df.with_column(swing_signal)?;
    df.with_column(risk_level)?;
    
    Ok(())
} 