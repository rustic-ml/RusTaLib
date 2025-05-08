use polars::prelude::*;
use crate::indicators::oscillators::calculate_rsi;

/// Adaptive RSI for Day Trading
///
/// This indicator modifies the standard RSI by dynamically adjusting
/// the period based on intraday volatility. It uses shorter periods
/// during higher volatility and longer periods during lower volatility.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `base_period` - Base RSI period (typically shorter for day trading, e.g., 7-14)
/// * `volatility_factor` - How much to adjust RSI period based on volatility (default: 0.5)
/// * `price_col` - Column name to calculate RSI on (default: "close")
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series containing adaptive RSI values
pub fn calculate_adaptive_rsi(
    df: &DataFrame,
    base_period: usize,
    volatility_factor: Option<f64>,
    price_col: Option<&str>,
) -> PolarsResult<Series> {
    let price_column = price_col.unwrap_or("close");
    let vol_factor = volatility_factor.unwrap_or(0.5);
    
    // Ensure necessary column exists
    if !df.schema().contains(price_column) {
        return Err(PolarsError::ComputeError(
            format!("Required column '{}' not found", price_column).into(),
        ));
    }
    
    // Calculate intraday volatility (using simple price range as a proxy)
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    
    let mut volatility = Vec::with_capacity(df.height());
    let window_size = 20.min(df.height()); // Use last 20 periods to measure volatility
    
    for i in 0..df.height() {
        if i < window_size - 1 {
            volatility.push(1.0); // Default volatility factor for initial periods
            continue;
        }
        
        // Calculate average true range as volatility measure
        let mut sum_range = 0.0;
        let mut count = 0;
        
        for j in (i - window_size + 1)..=i {
            let h = high.get(j).unwrap_or(f64::NAN);
            let l = low.get(j).unwrap_or(f64::NAN);
            
            if !h.is_nan() && !l.is_nan() {
                sum_range += (h - l);
                count += 1;
            }
        }
        
        let avg_range = if count > 0 { sum_range / count as f64 } else { 1.0 };
        
        // Normalize volatility
        let base_volatility = if i > window_size * 2 {
            let mut long_sum = 0.0;
            let mut long_count = 0;
            
            for j in (i - window_size * 2)..=i {
                let h = high.get(j).unwrap_or(f64::NAN);
                let l = low.get(j).unwrap_or(f64::NAN);
                
                if !h.is_nan() && !l.is_nan() {
                    long_sum += (h - l);
                    long_count += 1;
                }
            }
            
            if long_count > 0 { long_sum / long_count as f64 } else { avg_range }
        } else {
            avg_range
        };
        
        // Calculate relative volatility (>1 means higher volatility, <1 means lower)
        let relative_vol = if base_volatility > 0.0 { 
            avg_range / base_volatility 
        } else { 
            1.0 
        };
        
        volatility.push(relative_vol);
    }
    
    // Calculate adaptive periods
    let mut adaptive_periods = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let vol = volatility[i];
        // Adjust period inversely to volatility - higher volatility, shorter period
        let adjusted_period = (base_period as f64 / (vol * vol_factor).max(0.25)).max(2.0);
        adaptive_periods.push(adjusted_period.round() as usize);
    }
    
    // Calculate RSI with adaptive periods
    let price_series = df.column(price_column)?.f64()?;
    let mut adaptive_rsi = Vec::with_capacity(df.height());
    
    // First calculate returns
    let mut returns = Vec::with_capacity(df.height());
    returns.push(0.0); // First return is undefined
    
    for i in 1..df.height() {
        let current = price_series.get(i).unwrap_or(f64::NAN);
        let previous = price_series.get(i - 1).unwrap_or(f64::NAN);
        
        if !current.is_nan() && !previous.is_nan() && previous != 0.0 {
            returns.push(current - previous);
        } else {
            returns.push(f64::NAN);
        }
    }
    
    // Then calculate RSI with adaptive periods
    for i in 0..df.height() {
        let period = adaptive_periods[i];
        
        if i < period {
            adaptive_rsi.push(f64::NAN);
            continue;
        }
        
        let mut avg_gain = 0.0;
        let mut avg_loss = 0.0;
        
        for j in (i - period + 1)..=i {
            let ret = returns[j];
            if !ret.is_nan() {
                if ret > 0.0 {
                    avg_gain += ret;
                } else {
                    avg_loss += ret.abs();
                }
            }
        }
        
        avg_gain /= period as f64;
        avg_loss /= period as f64;
        
        let rs = if avg_loss > 0.0 { avg_gain / avg_loss } else { 100.0 };
        let rsi = 100.0 - (100.0 / (1.0 + rs));
        
        adaptive_rsi.push(rsi);
    }
    
    Ok(Series::new("adaptive_rsi", adaptive_rsi))
}

/// Add adaptive RSI indicator to DataFrame
///
/// # Arguments
///
/// * `df` - Mutable reference to DataFrame
/// * `base_period` - Base RSI period
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_adaptive_rsi(df: &mut DataFrame, base_period: usize) -> PolarsResult<()> {
    let adaptive_rsi = calculate_adaptive_rsi(df, base_period, None, None)?;
    df.with_column(adaptive_rsi)?;
    Ok(())
} 