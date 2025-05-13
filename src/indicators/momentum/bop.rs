use polars::prelude::*;

/// Calculates Balance of Power (BOP)
/// Formula: (Close - Open) / (High - Low)
///
/// This oscillator measures the strength of buyers vs. sellers in the market.
/// Values range typically between -1 and +1.
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLC data ("open", "high", "low", "close" columns)
///
/// # Returns
///
/// Returns a PolarsResult containing the BOP Series
pub fn calculate_bop(df: &DataFrame) -> PolarsResult<Series> {
    // Check that required columns exist
    if !df.schema().contains("open")
        || !df.schema().contains("high")
        || !df.schema().contains("low")
        || !df.schema().contains("close")
    {
        return Err(PolarsError::ShapeMismatch(
            "Missing required columns: open, high, low, close for BOP calculation".into(),
        ));
    }

    // Extract the required columns
    let open = df.column("open")?.f64()?;
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;

    // Calculate BOP: (Close - Open) / (High - Low)
    let mut bop_values = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        let open_val = open.get(i).unwrap_or(f64::NAN);
        let high_val = high.get(i).unwrap_or(f64::NAN);
        let low_val = low.get(i).unwrap_or(f64::NAN);
        let close_val = close.get(i).unwrap_or(f64::NAN);

        if !open_val.is_nan() && !high_val.is_nan() && !low_val.is_nan() && !close_val.is_nan() {
            let range = high_val - low_val;

            if range.abs() > 1e-10 {
                let bop = (close_val - open_val) / range;
                bop_values.push(bop);
            } else {
                // When the range is too small, we can consider it as zero, meaning no significant price movement
                bop_values.push(0.0);
            }
        } else {
            bop_values.push(f64::NAN);
        }
    }

    Ok(Series::new("bop".into(), bop_values))
}
