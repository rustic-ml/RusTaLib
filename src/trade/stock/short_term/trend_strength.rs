use polars::prelude::*;
use crate::indicators::moving_averages::{calculate_sma, calculate_ema};
use crate::indicators::trend::calculate_adx;

/// Calculate ADX-based Trend Strength Indicator
///
/// This enhanced version of ADX for swing trading combines traditional ADX
/// with moving average analysis to provide a more robust trend strength measure.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `period` - ADX calculation period (default: 14)
/// * `smooth_period` - Additional smoothing period (default: 3)
/// * `ma_period` - Moving average period to verify trend direction (default: 50)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series containing trend strength values (0-100)
pub fn calculate_trend_strength(
    df: &DataFrame,
    period: Option<usize>,
    smooth_period: Option<usize>,
    ma_period: Option<usize>,
) -> PolarsResult<Series> {
    let adx_period = period.unwrap_or(14);
    let smoothing = smooth_period.unwrap_or(3);
    let ma_len = ma_period.unwrap_or(50);
    
    // Calculate ADX
    let adx = calculate_adx(df, adx_period)?;
    let adx_values = adx.f64()?;
    
    // Calculate moving averages to determine trend direction
    let sma = calculate_sma(df, "close", ma_len)?;
    let sma_vals = sma.f64()?;
    
    // Calculate shorter SMA for comparison
    let short_ma = calculate_sma(df, "close", ma_len / 4)?; // Use 1/4 of the main MA period
    let short_ma_vals = short_ma.f64()?;
    
    // Get closing prices
    let close = df.column("close")?.f64()?;
    
    let mut trend_strength = Vec::with_capacity(df.height());
    
    // First values will be NaN until we have enough data points
    let min_periods = adx_period.max(ma_len).max(smoothing);
    for i in 0..min_periods.min(df.height()) {
        trend_strength.push(f64::NAN);
    }
    
    // Calculate trend strength for each remaining point
    for i in min_periods..df.height() {
        let adx_val = adx_values.get(i).unwrap_or(f64::NAN);
        let sma_val = sma_vals.get(i).unwrap_or(f64::NAN);
        let short_sma_val = short_ma_vals.get(i).unwrap_or(f64::NAN);
        let close_val = close.get(i).unwrap_or(f64::NAN);
        
        if adx_val.is_nan() || sma_val.is_nan() || short_sma_val.is_nan() || close_val.is_nan() {
            trend_strength.push(f64::NAN);
            continue;
        }
        
        // Base value is ADX
        let mut strength = adx_val;
        
        // Adjust strength based on position of price relative to MAs
        let ma_alignment = if close_val > sma_val && short_sma_val > sma_val {
            // Strong uptrend - price and short MA above long MA
            1.25 // Amplify strength
        } else if close_val < sma_val && short_sma_val < sma_val {
            // Strong downtrend - price and short MA below long MA
            1.25 // Amplify strength
        } else if (close_val > sma_val && short_sma_val < sma_val) ||
                  (close_val < sma_val && short_sma_val > sma_val) {
            // Mixed signals - potential reversal or consolidation
            0.75 // Reduce strength
        } else {
            // Neutral
            1.0
        };
        
        // Apply adjustment
        strength *= ma_alignment;
        
        // Smooth the strength value using past values
        if i >= min_periods + smoothing {
            let mut sum = strength;
            for j in 1..=smoothing {
                sum += trend_strength[i - j];
            }
            strength = sum / (smoothing as f64 + 1.0);
        }
        
        // Cap at 100
        trend_strength.push(strength.min(100.0));
    }
    
    Ok(Series::new("trend_strength", trend_strength))
}

/// Classify trend based on strength and direction
///
/// # Arguments
///
/// * `df` - DataFrame with calculated trend_strength
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with trend classification
///   (2: strong uptrend, 1: moderate uptrend, 0: no trend/consolidation,
///    -1: moderate downtrend, -2: strong downtrend)
pub fn classify_trend(df: &DataFrame) -> PolarsResult<Series> {
    // Check if required columns exist
    for col in ["trend_strength", "close"].iter() {
        if !df.schema().contains(*col) {
            return Err(PolarsError::ComputeError(
                format!("Required column '{}' not found", col).into(),
            ));
        }
    }
    
    let strength = df.column("trend_strength")?.f64()?;
    let close = df.column("close")?.f64()?;
    
    // Create SMA to determine trend direction
    let sma_short = calculate_sma(df, "close", 20)?;
    let sma_medium = calculate_sma(df, "close", 50)?;
    
    let sma_short_vals = sma_short.f64()?;
    let sma_medium_vals = sma_medium.f64()?;
    
    let mut trend_class = Vec::with_capacity(df.height());
    
    // First values will be undetermined until we have enough data
    for i in 0..50.min(df.height()) {
        trend_class.push(0);
    }
    
    // Classify trend for each remaining point
    for i in 50..df.height() {
        let strength_val = strength.get(i).unwrap_or(f64::NAN);
        let close_val = close.get(i).unwrap_or(f64::NAN);
        let short_ma = sma_short_vals.get(i).unwrap_or(f64::NAN);
        let medium_ma = sma_medium_vals.get(i).unwrap_or(f64::NAN);
        
        if strength_val.is_nan() || close_val.is_nan() || short_ma.is_nan() || medium_ma.is_nan() {
            trend_class.push(0);
            continue;
        }
        
        let trend_direction = if close_val > medium_ma && short_ma > medium_ma {
            1 // Uptrend
        } else if close_val < medium_ma && short_ma < medium_ma {
            -1 // Downtrend
        } else {
            0 // No clear trend
        };
        
        // Classify based on strength and direction
        if trend_direction > 0 {
            if strength_val >= 30.0 {
                trend_class.push(2); // Strong uptrend
            } else {
                trend_class.push(1); // Moderate uptrend
            }
        } else if trend_direction < 0 {
            if strength_val >= 30.0 {
                trend_class.push(-2); // Strong downtrend
            } else {
                trend_class.push(-1); // Moderate downtrend
            }
        } else {
            trend_class.push(0); // No trend / consolidation
        }
    }
    
    Ok(Series::new("trend_classification", trend_class))
}

/// Add trend strength analysis to DataFrame
///
/// # Arguments
///
/// * `df` - Mutable reference to DataFrame
/// * `period` - ADX calculation period
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_trend_strength_analysis(df: &mut DataFrame, period: usize) -> PolarsResult<()> {
    let trend_strength = calculate_trend_strength(df, Some(period), None, None)?;
    df.with_column(trend_strength)?;
    
    let trend_class = classify_trend(df)?;
    df.with_column(trend_class)?;
    
    Ok(())
} 