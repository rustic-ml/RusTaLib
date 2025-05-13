use polars::prelude::*;

/// Calculates Weighted Close Price
/// Formula: (High + Low + Close * 2) / 4
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data with high, low, and close columns
///
/// # Returns
///
/// Returns a PolarsResult containing the Weighted Close Price Series
pub fn calculate_wclprice(df: &DataFrame) -> PolarsResult<Series> {
    if !df.schema().contains("high")
        || !df.schema().contains("low")
        || !df.schema().contains("close")
    {
        return Err(PolarsError::ComputeError(
            "Weighted Close Price calculation requires high, low, and close columns".into(),
        ));
    }

    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;

    // Calculate (high + low + close * 2) / 4
    // Multiply close by 2
    let close_times_2 = close.clone() * 2.0;
    // Add high and low
    let high_plus_low = high + low;
    // Add the result to close_times_2
    let sum = high_plus_low + close_times_2;
    // Divide by 4
    let wcl_price = sum / 4.0;

    Ok(wcl_price.into_series().with_name("wclprice".into()))
}