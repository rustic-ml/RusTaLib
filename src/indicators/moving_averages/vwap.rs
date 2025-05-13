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
    for (i, _) in typical_prices.iter().enumerate().take(df.height()) {
        price_volume.push(typical_prices[i] * volume.get(i).unwrap_or(0.0));
    }

    // For standard VWAP, calculate cumulative price*volume / cumulative volume
    let mut vwap_values = Vec::with_capacity(df.height());

    if lookback == 0 || lookback >= df.height() {
        // Calculate VWAP for the entire period
        let mut cumulative_pv = 0.0;
        let mut cumulative_volume = 0.0;

        for (i, &pv) in price_volume.iter().enumerate().take(df.height()) {
            cumulative_pv += pv;
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

            for (j, &pv) in price_volume.iter().enumerate().take(i + 1).skip(start_idx) {
                window_pv += pv;
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