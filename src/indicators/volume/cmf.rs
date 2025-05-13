use polars::prelude::*;

/// Calculates the Chaikin Money Flow (CMF) indicator
///
/// The Chaikin Money Flow measures the amount of Money Flow Volume over a specific period.
/// It's particularly useful for intraday trading as it helps identify buying and selling pressure.
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLCV data with "high", "low", "close", and "volume" columns
/// * `window` - Lookback period for calculating the CMF (typically 20 or 21)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series containing CMF values named "cmf_{window}"
///
/// # Formula
///
/// 1. Calculate Money Flow Multiplier: ((close - low) - (high - close)) / (high - low)
/// 2. Calculate Money Flow Volume: Money Flow Multiplier * volume
/// 3. Sum Money Flow Volume over the period and divide by sum of Volume over the period
///
/// # Example
///
/// ```
/// use polars::prelude::*;
/// use ta_lib_in_rust::indicators::volume::calculate_cmf;
///
/// // Create or load a DataFrame with OHLCV data
/// let df = DataFrame::default(); // Replace with actual data
///
/// // Calculate CMF with period 20
/// let cmf = calculate_cmf(&df, 20).unwrap();
/// ```
pub fn calculate_cmf(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    // Validate that necessary columns exist
    if !df.schema().contains("high")
        || !df.schema().contains("low")
        || !df.schema().contains("close")
        || !df.schema().contains("volume")
    {
        return Err(PolarsError::ShapeMismatch(
            "Missing required columns for CMF calculation. Required: high, low, close, volume"
                .to_string()
                .into(),
        ));
    }

    // Validate window size
    if window == 0 {
        return Err(PolarsError::ComputeError(
            "Window size must be greater than 0".into(),
        ));
    }

    // Extract the required columns
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    let volume = df.column("volume")?.f64()?;

    // Calculate Money Flow Multiplier for each period
    let mut money_flow_multipliers = Vec::with_capacity(df.height());
    let mut money_flow_volumes = Vec::with_capacity(df.height());

    // Use standard for loop with index
    for i in 0..df.height() {
        let high_val = high.get(i).unwrap_or(f64::NAN);
        let low_val = low.get(i).unwrap_or(f64::NAN);
        let close_val = close.get(i).unwrap_or(f64::NAN);
        let vol = volume.get(i).unwrap_or(f64::NAN);

        // Calculate money flow multiplier only if all values are valid
        if !high_val.is_nan() && !low_val.is_nan() && !close_val.is_nan() && high_val != low_val {
            let money_flow_multiplier =
                ((close_val - low_val) - (high_val - close_val)) / (high_val - low_val);
            money_flow_multipliers.push(money_flow_multiplier);

            // Money flow volume is the product of money flow multiplier and volume
            let money_flow_volume = money_flow_multiplier * vol;
            money_flow_volumes.push(money_flow_volume);
        } else {
            money_flow_multipliers.push(f64::NAN);
            money_flow_volumes.push(f64::NAN);
        }
    }

    // Calculate CMF using a sliding window
    let mut cmf_values = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        if i < window - 1 {
            cmf_values.push(f64::NAN);
            continue;
        }

        // Calculate the sum of money flow volumes in the window
        let mut sum_money_flow_volume = 0.0;
        let mut sum_volume = 0.0;

        // Use an iterator-based approach as suggested by Clippy
        let window_start = i - (window - 1);
        for (idx, money_flow_vol) in money_flow_volumes
            .iter()
            .enumerate()
            .skip(window_start)
            .take(window)
        {
            let vol = volume.get(idx).unwrap_or(f64::NAN);

            if !money_flow_vol.is_nan() && !vol.is_nan() {
                sum_money_flow_volume += money_flow_vol;
                sum_volume += vol;
            }
        }

        // Calculate CMF as the ratio of sum of money flow volumes to sum of volume
        if sum_volume > 0.0 {
            cmf_values.push(sum_money_flow_volume / sum_volume);
        } else {
            cmf_values.push(f64::NAN);
        }
    }

    // Return the CMF as a Polars Series
    Ok(Series::new(format!("cmf_{}", window).into(), cmf_values))
}
