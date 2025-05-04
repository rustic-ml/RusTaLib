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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_typprice() {
        // Create test OHLC DataFrame
        let high = Series::new("high".into(), &[12.0, 14.0, 16.0, 15.0, 17.0]);
        let low = Series::new("low".into(), &[8.0, 9.0, 10.0, 9.0, 11.0]);
        let close = Series::new("close".into(), &[10.0, 12.0, 14.0, 11.0, 15.0]);
        let df = DataFrame::new(vec![high.into(), low.into(), close.into()]).unwrap();

        let typ = calculate_typprice(&df).unwrap();

        // typprice = (high + low + close) / 3
        // For first row: (12 + 8 + 10) / 3 = 10
        assert_eq!(typ.f64().unwrap().get(0).unwrap(), 10.0);

        // For second row: (14 + 9 + 12) / 3 = 11.67
        assert!((typ.f64().unwrap().get(1).unwrap() - 11.67).abs() < 0.01);
    }
}
