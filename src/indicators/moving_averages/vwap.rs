use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Volume-Weighted Average Price (VWAP)
///
/// VWAP is calculated by adding up the dollars traded for every transaction
/// (price multiplied by the number of shares traded) and then dividing by the
/// total shares traded for the day.
///
/// # Arguments
///
/// * `df` - DataFrame containing high, low, close, and volume data
/// * `lookback` - Number of periods to look back (optional, for rolling VWAP)
///
/// # Returns
///
/// Returns a PolarsResult containing the VWAP Series
pub fn calculate_vwap(df: &DataFrame, lookback: usize) -> PolarsResult<Series> {
    // Check if required columns exist
    if !df.schema().contains("high")
        || !df.schema().contains("low")
        || !df.schema().contains("close")
        || !df.schema().contains("volume")
    {
        return Err(PolarsError::ComputeError(
            "VWAP calculation requires high, low, close and volume columns".into(),
        ));
    }

    // Check we have enough data for the lookback period
    check_window_size(df, lookback, "VWAP")?;

    // Get columns
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    let volume = df.column("volume")?.f64()?;

    // Calculate typical price (high + low + close) / 3 for each bar
    let mut typical_prices = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let h = high.get(i).unwrap_or(0.0);
        let l = low.get(i).unwrap_or(0.0);
        let c = close.get(i).unwrap_or(0.0);

        typical_prices.push((h + l + c) / 3.0);
    }

    // Calculate price * volume (cumulative money flow)
    let mut price_volume = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        price_volume.push(typical_prices[i] * volume.get(i).unwrap_or(0.0));
    }

    // For standard VWAP, calculate cumulative price*volume / cumulative volume
    let mut vwap_values = Vec::with_capacity(df.height());

    if lookback == 0 || lookback >= df.height() {
        // Calculate VWAP for the entire period
        let mut cumulative_pv = 0.0;
        let mut cumulative_volume = 0.0;

        for i in 0..df.height() {
            cumulative_pv += price_volume[i];
            cumulative_volume += volume.get(i).unwrap_or(0.0);

            if cumulative_volume > 0.0 {
                vwap_values.push(cumulative_pv / cumulative_volume);
            } else {
                vwap_values.push(close.get(i).unwrap_or(0.0)); // Fall back to close price if no volume
            }
        }
    } else {
        // Calculate rolling VWAP over the lookback period
        for i in 0..df.height() {
            let start_idx = if i >= lookback { i - lookback + 1 } else { 0 };

            let mut window_pv = 0.0;
            let mut window_volume = 0.0;

            for j in start_idx..=i {
                window_pv += price_volume[j];
                window_volume += volume.get(j).unwrap_or(0.0);
            }

            if window_volume > 0.0 {
                vwap_values.push(window_pv / window_volume);
            } else {
                vwap_values.push(close.get(i).unwrap_or(0.0)); // Fall back to close price if no volume
            }
        }
    }

    Ok(Series::new("vwap".into(), vwap_values))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_vwap_basic() {
        // Create test DataFrame
        let high = Series::new("high".into(), &[102.0, 104.0, 103.0, 106.0, 105.0]);
        let low = Series::new("low".into(), &[98.0, 99.0, 97.0, 101.0, 100.0]);
        let close = Series::new("close".into(), &[100.0, 102.0, 99.0, 104.0, 103.0]);
        let volume = Series::new("volume".into(), &[1000.0, 1500.0, 2000.0, 1800.0, 2200.0]);

        let df =
            DataFrame::new(vec![high.into(), low.into(), close.into(), volume.into()]).unwrap();

        // Calculate VWAP for entire period
        let vwap = calculate_vwap(&df, 0).unwrap();

        // First value: (100+98+102)/3 * 1000 / 1000 = 100.0
        assert!((vwap.f64().unwrap().get(0).unwrap() - 100.0).abs() < 1e-10);

        // Calculate typical prices and cumulative values for verification
        let typical_prices = vec![
            (102.0 + 98.0 + 100.0) / 3.0,  // 100.0
            (104.0 + 99.0 + 102.0) / 3.0,  // 101.67
            (103.0 + 97.0 + 99.0) / 3.0,   // 99.67
            (106.0 + 101.0 + 104.0) / 3.0, // 103.67
            (105.0 + 100.0 + 103.0) / 3.0, // 102.67
        ];

        let volumes = vec![1000.0, 1500.0, 2000.0, 1800.0, 2200.0];

        // Calculate manual VWAP values
        let mut cum_pv = 0.0;
        let mut cum_vol = 0.0;
        let mut manual_vwaps = Vec::new();

        for i in 0..5 {
            cum_pv += typical_prices[i] * volumes[i];
            cum_vol += volumes[i];
            manual_vwaps.push(cum_pv / cum_vol);
        }

        for i in 0..5 {
            assert!((vwap.f64().unwrap().get(i).unwrap() - manual_vwaps[i]).abs() < 1e-10);
        }

        // Test rolling VWAP with lookback 3
        let rolling_vwap = calculate_vwap(&df, 3).unwrap();

        // Manual check of last value (rolling 3 periods)
        let last_3_pv = typical_prices[2] * volumes[2]
            + typical_prices[3] * volumes[3]
            + typical_prices[4] * volumes[4];
        let last_3_vol = volumes[2] + volumes[3] + volumes[4];
        let expected = last_3_pv / last_3_vol;

        assert!((rolling_vwap.f64().unwrap().get(4).unwrap() - expected).abs() < 1e-10);
    }
}
