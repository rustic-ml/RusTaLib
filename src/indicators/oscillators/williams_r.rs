use polars::prelude::*;

/// Calculates the Williams %R oscillator
///
/// Williams %R is a momentum indicator that moves between 0 and -100 and
/// measures overbought/oversold levels. It's particularly useful for intraday trading
/// to identify potential reversals.
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLCV data with "high", "low", and "close" columns
/// * `window` - Lookback period for the calculation (typically 14)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series containing Williams %R values named "williams_r_{window}"
///
/// # Formula
///
/// Williams %R = ((Highest High - Close) / (Highest High - Lowest Low)) * -100
///
/// # Example
///
/// ```
/// use polars::prelude::*;
/// use ta_lib_in_rust::indicators::oscillators::calculate_williams_r;
///
/// // Create or load a DataFrame with OHLCV data
/// let df = DataFrame::default(); // Replace with actual data
///
/// // Calculate Williams %R with period 14
/// let williams_r = calculate_williams_r(&df, 14).unwrap();
/// ```
pub fn calculate_williams_r(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    // Validate required columns
    if !df.schema().contains("high") || !df.schema().contains("low") || !df.schema().contains("close") {
        return Err(PolarsError::ShapeMismatch(
            format!("Missing required columns for Williams %R calculation. Required: high, low, close").into()
        ));
    }

    // Extract required columns
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;

    // Calculate Williams %R
    let mut williams_r_values = Vec::with_capacity(df.height());
    
    // Fill initial values with NaN
    for _ in 0..window-1 {
        williams_r_values.push(f64::NAN);
    }
    
    // Calculate Williams %R for the remaining data points
    for i in window-1..df.height() {
        let mut highest_high = f64::NEG_INFINITY;
        let mut lowest_low = f64::INFINITY;
        let mut valid_data = true;
        
        // Find highest high and lowest low in the window
        for j in i-(window-1)..=i {
            let h = high.get(j).unwrap_or(f64::NAN);
            let l = low.get(j).unwrap_or(f64::NAN);
            
            if h.is_nan() || l.is_nan() {
                valid_data = false;
                break;
            }
            
            highest_high = highest_high.max(h);
            lowest_low = lowest_low.min(l);
        }
        
        if !valid_data || (highest_high - lowest_low).abs() < 1e-10 {
            williams_r_values.push(f64::NAN);
        } else {
            let c = close.get(i).unwrap_or(f64::NAN);
            if c.is_nan() {
                williams_r_values.push(f64::NAN);
            } else {
                let williams_r = ((highest_high - c) / (highest_high - lowest_low)) * -100.0;
                williams_r_values.push(williams_r);
            }
        }
    }
    
    // Create Series with Williams %R values
    let name = format!("williams_r_{}", window);
    Ok(Series::new(name.into(), williams_r_values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::test_util::create_test_ohlcv_df;

    #[test]
    fn test_calculate_williams_r() {
        let df = create_test_ohlcv_df();
        let williams_r = calculate_williams_r(&df, 14).unwrap();
        
        // Williams %R should be in the range [-100, 0]
        for i in 14..df.height() {
            let value = williams_r.f64().unwrap().get(i).unwrap();
            if !value.is_nan() {
                assert!(value >= -100.0 && value <= 0.0);
            }
        }
        
        // Williams %R for the first (window-1) periods should be NaN
        for i in 0..13 {
            assert!(williams_r.f64().unwrap().get(i).unwrap().is_nan());
        }
    }
} 