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
    if window == 0 || window > df.height() {
        return Err(PolarsError::ComputeError(
            format!(
                "Invalid window size {} for dataset with {} rows",
                window,
                df.height()
            )
            .into(),
        ));
    }

    // Extract the required columns
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    let volume = df.column("volume")?.f64()?;

    // Calculate Money Flow Multiplier
    let mut money_flow_multipliers = Vec::with_capacity(df.height());
    for (i, _) in high.iter().enumerate().take(df.height()) {
        let high_val = high.get(i).unwrap_or(f64::NAN);
        let low_val = low.get(i).unwrap_or(f64::NAN);
        let close_val = close.get(i).unwrap_or(f64::NAN);

        if high_val.is_nan() || low_val.is_nan() || close_val.is_nan() || high_val == low_val {
            money_flow_multipliers.push(f64::NAN);
        } else {
            let multiplier =
                ((close_val - low_val) - (high_val - close_val)) / (high_val - low_val);
            money_flow_multipliers.push(multiplier);
        }
    }

    // Calculate Money Flow Volume
    let mut money_flow_volumes = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let mfm = money_flow_multipliers[i];
        let vol = volume.get(i).unwrap_or(f64::NAN);

        if mfm.is_nan() || vol.is_nan() {
            money_flow_volumes.push(f64::NAN);
        } else {
            money_flow_volumes.push(mfm * vol);
        }
    }

    // Calculate CMF values
    let mut cmf_values = Vec::with_capacity(df.height());

    // Fill in NaN values for the initial window
    for _ in 0..window.min(df.height()) {
        cmf_values.push(f64::NAN);
    }

    // Make sure we have enough data to calculate CMF
    if df.height() <= window {
        return Ok(Series::new(format!("cmf_{}", window).into(), cmf_values));
    }

    // Calculate CMF for each period after the initial window
    for i in window..df.height() {
        let mut money_flow_volume_sum = 0.0;
        let mut volume_sum = 0.0;
        let mut has_nan = false;

        // Sum up money flow volumes and volumes over the window
        for (j, &mfv) in money_flow_volumes.iter().enumerate().take(i).skip(i - window) {
            let vol = volume.get(j).unwrap_or(f64::NAN);

            if mfv.is_nan() || vol.is_nan() {
                has_nan = true;
                break;
            }

            money_flow_volume_sum += mfv;
            volume_sum += vol;
        }

        if has_nan || volume_sum.abs() < 1e-10 {
            cmf_values.push(f64::NAN);
        } else {
            let cmf = money_flow_volume_sum / volume_sum;
            cmf_values.push(cmf);
        }
    }

    // Create a Series with the CMF values
    let name = format!("cmf_{}", window);
    Ok(Series::new(name.into(), cmf_values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::test_util::create_test_ohlcv_df;

    #[test]
    fn test_calculate_cmf() {
        let df = create_test_ohlcv_df();
        let cmf = calculate_cmf(&df, 20).unwrap();

        // CMF should be in the range [-1, 1]
        for i in 20..df.height() {
            let value = cmf.f64().unwrap().get(i).unwrap();
            if !value.is_nan() {
                assert!(value >= -1.0 && value <= 1.0);
            }
        }

        // CMF for the first (window-1) periods should be NaN
        for i in 0..19 {
            assert!(cmf.f64().unwrap().get(i).unwrap().is_nan());
        }
    }

    #[test]
    fn test_cmf_small_dataset() {
        // Test with a dataset smaller than the window size
        let mut df = create_test_ohlcv_df();
        df = df.slice(0, 5); // Only use first 5 rows

        let cmf = calculate_cmf(&df, 10);
        assert!(cmf.is_ok());

        let cmf_series = cmf.unwrap();
        assert_eq!(cmf_series.len(), 5);

        // All values should be NaN since window > dataset size
        for i in 0..5 {
            assert!(cmf_series.f64().unwrap().get(i).unwrap().is_nan());
        }
    }
}
