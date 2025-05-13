use polars::prelude::*;

/// Calculates Average Price
/// Formula: (High + Low) / 2
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data with high and low columns
///
/// # Returns
///
/// Returns a PolarsResult containing the Average Price Series
pub fn calculate_avgprice(df: &DataFrame) -> PolarsResult<Series> {
    if !df.schema().contains("high") || !df.schema().contains("low") {
        return Err(PolarsError::ComputeError(
            "Average Price calculation requires high and low columns".into(),
        ));
    }

    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;

    // Add high and low and divide by 2
    let avg_price = (high + low) / 2.0;

    Ok(avg_price.into_series().with_name("avgprice".into()))
}
