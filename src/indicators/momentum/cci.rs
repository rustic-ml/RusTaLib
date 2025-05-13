use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Commodity Channel Index (CCI)
/// Formula: CCI = (Typical Price - SMA(Typical Price)) / (0.015 * Mean Deviation)
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLC data with "high", "low", and "close" columns
/// * `window` - Window size for CCI (typically 20)
///
/// # Returns
///
/// Returns a PolarsResult containing the CCI Series
pub fn calculate_cci(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    // Validate input
    check_window_size(df, window, "CCI")?;
    
    // Check that required columns exist
    if !df.schema().contains("high") || !df.schema().contains("low") || !df.schema().contains("close") {
        return Err(PolarsError::ShapeMismatch(
            "Missing required columns: high, low, close for CCI calculation".into(),
        ));
    }

    // Extract the required columns
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    
    // Calculate typical price: (high + low + close) / 3
    let mut typical_prices = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let h = high.get(i).unwrap_or(f64::NAN);
        let l = low.get(i).unwrap_or(f64::NAN);
        let c = close.get(i).unwrap_or(f64::NAN);
        
        if !h.is_nan() && !l.is_nan() && !c.is_nan() {
            typical_prices.push((h + l + c) / 3.0);
        } else {
            typical_prices.push(f64::NAN);
        }
    }
    
    // Calculate SMA of typical price
    let mut cci_values = Vec::with_capacity(df.height());
    
    // Fill initial values with NaN
    for _i in 0..window-1 {
        cci_values.push(f64::NAN);
    }
    
    // Calculate CCI for each period after the initial window
    for i in window-1..df.height() {
        // Calculate SMA of typical price for this window
        let mut sum_typical_price = 0.0;
        let mut valid_values = 0;
        
        for j in i-(window-1)..=i {
            let tp = typical_prices[j];
            if !tp.is_nan() {
                sum_typical_price += tp;
                valid_values += 1;
            }
        }
        
        // Skip if we don't have enough valid values
        if valid_values == 0 {
            cci_values.push(f64::NAN);
            continue;
        }
        
        let sma_typical_price = sum_typical_price / valid_values as f64;
        
        // Calculate mean deviation
        let mut sum_deviation = 0.0;
        for j in i-(window-1)..=i {
            let tp = typical_prices[j];
            if !tp.is_nan() {
                sum_deviation += (tp - sma_typical_price).abs();
            }
        }
        
        let mean_deviation = sum_deviation / valid_values as f64;
        
        // Calculate CCI
        // Using 0.015 as the constant multiplier
        let constant = 0.015;
        
        if mean_deviation.abs() < 1e-10 {
            // Avoid division by zero
            cci_values.push(0.0);
        } else {
            let cci = (typical_prices[i] - sma_typical_price) / (constant * mean_deviation);
            cci_values.push(cci);
        }
    }
    
    Ok(Series::new("cci".into(), cci_values))
} 