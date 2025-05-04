use polars::prelude::*;

/// Calculates the Parabolic SAR (Stop and Reverse) indicator
///
/// The Parabolic SAR is a trend-following indicator that provides entry and exit points.
/// It's particularly useful for intraday trading to determine trend direction and set
/// trailing stops.
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLCV data with "high" and "low" columns
/// * `af_step` - Acceleration factor step (typically 0.02)
/// * `af_max` - Maximum acceleration factor (typically 0.2)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series containing PSAR values named "psar_{af_step}_{af_max}"
///
/// # Example
///
/// ```
/// use polars::prelude::*;
/// use ta_lib_in_rust::indicators::trend::calculate_psar;
///
/// // Create or load a DataFrame with OHLCV data
/// let df = DataFrame::default(); // Replace with actual data
///
/// // Calculate Parabolic SAR with default parameters
/// let psar = calculate_psar(&df, 0.02, 0.2).unwrap();
/// ```
pub fn calculate_psar(df: &DataFrame, af_step: f64, af_max: f64) -> PolarsResult<Series> {
    // Validate required columns
    if !df.schema().contains("high") || !df.schema().contains("low") {
        return Err(PolarsError::ShapeMismatch(
            format!("Missing required columns for PSAR calculation. Required: high, low").into()
        ));
    }

    // Extract required columns
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;

    let height = df.height();
    if height < 2 {
        return Err(PolarsError::ShapeMismatch(
            format!("Not enough data points for PSAR calculation. Need at least 2.").into()
        ));
    }

    // Initialize PSAR values
    let mut psar_values = Vec::with_capacity(height);
    
    // First value is NaN since we need at least one prior candle
    psar_values.push(f64::NAN);
    
    // Variables to track PSAR calculation
    let mut is_uptrend = true; // Initial trend direction (assume uptrend)
    let mut current_psar = low.get(0).unwrap_or(0.0); // Starting PSAR value
    let mut extreme_point = high.get(0).unwrap_or(0.0); // Initial extreme point
    let mut acceleration_factor = af_step; // Starting acceleration factor
    
    // Calculate PSAR for each data point starting from second candle
    for i in 1..height {
        let high_val = high.get(i).unwrap_or(f64::NAN);
        let low_val = low.get(i).unwrap_or(f64::NAN);
        let prev_high = high.get(i-1).unwrap_or(f64::NAN);
        let prev_low = low.get(i-1).unwrap_or(f64::NAN);
        
        if high_val.is_nan() || low_val.is_nan() || prev_high.is_nan() || prev_low.is_nan() {
            psar_values.push(f64::NAN);
            continue;
        }
        
        // Calculate PSAR based on trend
        if is_uptrend {
            // In uptrend, PSAR decreases
            current_psar = current_psar + acceleration_factor * (extreme_point - current_psar);
            
            // Ensure PSAR is below the previous low
            current_psar = current_psar.min(prev_low).min(low_val);
            
            // Check if trend reverses
            if current_psar > low_val {
                // Trend reversal: Uptrend to Downtrend
                is_uptrend = false;
                current_psar = extreme_point;
                extreme_point = low_val;
                acceleration_factor = af_step;
            } else {
                // Continue uptrend
                if high_val > extreme_point {
                    extreme_point = high_val;
                    acceleration_factor = (acceleration_factor + af_step).min(af_max);
                }
            }
        } else {
            // In downtrend, PSAR increases
            current_psar = current_psar - acceleration_factor * (current_psar - extreme_point);
            
            // Ensure PSAR is above the previous high
            current_psar = current_psar.max(prev_high).max(high_val);
            
            // Check if trend reverses
            if current_psar < high_val {
                // Trend reversal: Downtrend to Uptrend
                is_uptrend = true;
                current_psar = extreme_point;
                extreme_point = high_val;
                acceleration_factor = af_step;
            } else {
                // Continue downtrend
                if low_val < extreme_point {
                    extreme_point = low_val;
                    acceleration_factor = (acceleration_factor + af_step).min(af_max);
                }
            }
        }
        
        psar_values.push(current_psar);
    }
    
    // Create Series with PSAR values
    let name = format!("psar_{:.2}_{:.2}", af_step, af_max).replace(".", "_");
    Ok(Series::new(name.into(), psar_values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::test_util::create_test_ohlcv_df;

    #[test]
    fn test_calculate_psar() {
        let df = create_test_ohlcv_df();
        let psar = calculate_psar(&df, 0.02, 0.2).unwrap();
        
        // First value should be NaN
        assert!(psar.f64().unwrap().get(0).unwrap().is_nan());
        
        // Check that values are within the price range
        let high = df.column("high").unwrap().f64().unwrap();
        let low = df.column("low").unwrap().f64().unwrap();
        
        for i in 1..df.height() {
            let psar_val = psar.f64().unwrap().get(i).unwrap();
            if !psar_val.is_nan() {
                // Find the min and max prices in a reasonable window
                let start_idx = i.saturating_sub(5);
                let mut min_price = f64::INFINITY;
                let mut max_price = f64::NEG_INFINITY;
                
                for j in start_idx..=i {
                    let high_val = high.get(j).unwrap_or(f64::INFINITY);
                    let low_val = low.get(j).unwrap_or(f64::NEG_INFINITY);
                    
                    if !high_val.is_nan() && !low_val.is_nan() {
                        min_price = min_price.min(low_val);
                        max_price = max_price.max(high_val);
                    }
                }
                
                // PSAR should be within a reasonable range of recent prices
                // Allow for a buffer of 20% beyond the range for newly established trends
                let buffer = (max_price - min_price) * 0.2;
                assert!(psar_val >= min_price - buffer && psar_val <= max_price + buffer);
            }
        }
    }
} 