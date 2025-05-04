use polars::prelude::*;

/// Calculates the Money Flow Index (MFI), a volume-weighted version of RSI
///
/// MFI is an oscillator that ranges from 0 to 100 and is particularly useful
/// for intraday trading as it combines price and volume to identify overbought
/// or oversold conditions.
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLCV data with "high", "low", "close", and "volume" columns
/// * `window` - Lookback period for calculating the MFI
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series containing MFI values named "mfi_{window}"
///
/// # Formula
///
/// The MFI is calculated using the following steps:
/// 1. Calculate the typical price: (high + low + close) / 3
/// 2. Calculate the money flow: typical price * volume
/// 3. Compare the typical price with previous period to determine positive or negative flow
/// 4. Calculate positive and negative money flow sums over the lookback period
/// 5. Calculate the money ratio: positive money flow / negative money flow
/// 6. Calculate MFI: 100 - (100 / (1 + money ratio))
///
/// # Example
///
/// ```
/// use polars::prelude::*;
/// use ta_lib_in_rust::indicators::volume::calculate_mfi;
///
/// // Create or load a DataFrame with OHLCV data
/// let df = DataFrame::default(); // Replace with actual data
///
/// // Calculate MFI with period 14
/// let mfi = calculate_mfi(&df, 14).unwrap();
/// ```
pub fn calculate_mfi(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    // Validate that necessary columns exist
    if !df.schema().contains("high") 
        || !df.schema().contains("low") 
        || !df.schema().contains("close") 
        || !df.schema().contains("volume") {
        return Err(PolarsError::ShapeMismatch(
            format!("Missing required columns for MFI calculation. Required: high, low, close, volume").into()
        ));
    }

    // Extract the required columns
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    let volume = df.column("volume")?.f64()?;

    // Calculate the typical price: (high + low + close) / 3
    let mut typical_prices = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let high_val = high.get(i).unwrap_or(f64::NAN);
        let low_val = low.get(i).unwrap_or(f64::NAN);
        let close_val = close.get(i).unwrap_or(f64::NAN);
        
        if high_val.is_nan() || low_val.is_nan() || close_val.is_nan() {
            typical_prices.push(f64::NAN);
        } else {
            typical_prices.push((high_val + low_val + close_val) / 3.0);
        }
    }

    // Calculate the money flow: typical price * volume
    let mut money_flows = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let tp = typical_prices[i];
        let vol = volume.get(i).unwrap_or(f64::NAN);
        
        if tp.is_nan() || vol.is_nan() {
            money_flows.push(f64::NAN);
        } else {
            money_flows.push(tp * vol);
        }
    }

    // Initialize positive and negative money flow vectors
    let mut positive_money_flows = Vec::with_capacity(df.height());
    let mut negative_money_flows = Vec::with_capacity(df.height());
    
    // First element has no previous value to compare
    positive_money_flows.push(0.0);
    negative_money_flows.push(0.0);
    
    // Determine positive and negative money flows
    for i in 1..df.height() {
        let current_tp = typical_prices[i];
        let prev_tp = typical_prices[i - 1];
        let current_mf = money_flows[i];
        
        if current_tp.is_nan() || prev_tp.is_nan() || current_mf.is_nan() {
            positive_money_flows.push(0.0);
            negative_money_flows.push(0.0);
        } else if current_tp > prev_tp {
            positive_money_flows.push(current_mf);
            negative_money_flows.push(0.0);
        } else if current_tp < prev_tp {
            positive_money_flows.push(0.0);
            negative_money_flows.push(current_mf);
        } else {
            // If typical prices are equal, treat as no change
            positive_money_flows.push(0.0);
            negative_money_flows.push(0.0);
        }
    }

    // Calculate MFI values
    let mut mfi_values = Vec::with_capacity(df.height());
    
    // Fill in NaN values for the initial window
    for _ in 0..window {
        mfi_values.push(f64::NAN);
    }
    
    // Calculate MFI for each period after the initial window
    for i in window..df.height() {
        let mut positive_flow_sum = 0.0;
        let mut negative_flow_sum = 0.0;
        
        // Sum up positive and negative money flows over the window
        for j in (i - window + 1)..=i {
            positive_flow_sum += positive_money_flows[j];
            negative_flow_sum += negative_money_flows[j];
        }
        
        if negative_flow_sum.abs() < 1e-10 {
            // Avoid division by zero or very small numbers
            if positive_flow_sum.abs() < 1e-10 {
                mfi_values.push(50.0); // No money flow in either direction
            } else {
                mfi_values.push(100.0); // All positive money flow
            }
        } else {
            let money_ratio = positive_flow_sum / negative_flow_sum;
            let mfi = 100.0 - (100.0 / (1.0 + money_ratio));
            mfi_values.push(mfi);
        }
    }
    
    // Create a Series with the MFI values
    let name = format!("mfi_{}", window);
    Ok(Series::new(name.into(), mfi_values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::test_util::create_test_ohlcv_df;

    #[test]
    fn test_calculate_mfi() {
        let df = create_test_ohlcv_df();
        let mfi = calculate_mfi(&df, 14).unwrap();
        
        // MFI should be in the range [0, 100]
        for i in 14..df.height() {
            let value = mfi.f64().unwrap().get(i).unwrap();
            assert!(value >= 0.0 && value <= 100.0);
        }
        
        // MFI for the first (window-1) periods should be NaN
        for i in 0..14 {
            assert!(mfi.f64().unwrap().get(i).unwrap().is_nan());
        }
    }
} 