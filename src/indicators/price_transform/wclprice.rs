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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wclprice() {
        // Create test OHLC DataFrame
        let high = Series::new("high".into(), &[12.0, 14.0, 16.0, 15.0, 17.0]);
        let low = Series::new("low".into(), &[8.0, 9.0, 10.0, 9.0, 11.0]);
        let close = Series::new("close".into(), &[10.0, 12.0, 14.0, 11.0, 15.0]);
        let df = DataFrame::new(vec![high.into(), low.into(), close.into()]).unwrap();

        let wcl = calculate_wclprice(&df).unwrap();

        // wclprice = (high + low + close * 2) / 4
        // For first row: (12 + 8 + 10 * 2) / 4 = 10
        assert_eq!(wcl.f64().unwrap().get(0).unwrap(), 10.0);

        // For fourth row: (15 + 9 + 11 * 2) / 4 = 11.5
        assert_eq!(wcl.f64().unwrap().get(3).unwrap(), 11.5);
    }
}
