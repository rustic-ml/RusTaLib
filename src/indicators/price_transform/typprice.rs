use polars::prelude::*;

/// Calculates Typical Price
/// Formula: (High + Low + Close) / 3
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data with high, low, and close columns
///
/// # Returns
///
/// Returns a PolarsResult containing the Typical Price Series
pub fn calculate_typprice(df: &DataFrame) -> PolarsResult<Series> {
    if !df.schema().contains("high")
        || !df.schema().contains("low")
        || !df.schema().contains("close")
    {
        return Err(PolarsError::ComputeError(
            "Typical Price calculation requires high, low, and close columns".into(),
        ));
    }

    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;

    // Calculate (high + low + close) / 3
    // First add high and low
    let high_plus_low = high + low;
    // Then add close
    let sum = high_plus_low + close.clone();
    // Finally divide by 3
    let typ_price = sum / 3.0;

    Ok(typ_price.into_series().with_name("typprice".into()))
}
