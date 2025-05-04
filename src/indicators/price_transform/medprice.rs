use polars::prelude::*;

/// Calculates Median Price
/// Formula: (High + Low) / 2
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data with high and low columns
///
/// # Returns
///
/// Returns a PolarsResult containing the Median Price Series
pub fn calculate_medprice(df: &DataFrame) -> PolarsResult<Series> {
    // Note: This is actually the same as avgprice but kept separate for naming consistency with TA-Lib
    if !df.schema().contains("high") || !df.schema().contains("low") {
        return Err(PolarsError::ComputeError(
            "Median Price calculation requires high and low columns".into(),
        ));
    }

    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;

    // Add high and low and divide by 2
    let med_price = (high + low) / 2.0;

    Ok(med_price.into_series().with_name("medprice".into()))
}
