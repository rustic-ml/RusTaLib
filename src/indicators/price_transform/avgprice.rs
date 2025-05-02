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
            "Average Price calculation requires high and low columns".into()
        ));
    }
    
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    
    // Add high and low and divide by 2
    let avg_price = (high + low) / 2.0;
    
    Ok(avg_price.into_series().with_name("avgprice".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_avgprice() {
        // Create test OHLC DataFrame
        let high = Series::new("high".into(), &[12.0, 14.0, 16.0, 15.0, 17.0]);
        let low = Series::new("low".into(), &[8.0, 9.0, 10.0, 9.0, 11.0]);
        let df = DataFrame::new(vec![high.into(), low.into()]).unwrap();
        
        let avg = calculate_avgprice(&df).unwrap();
        
        // avgprice = (high + low) / 2
        // For first row: (12 + 8) / 2 = 10
        assert_eq!(avg.f64().unwrap().get(0).unwrap(), 10.0);
        
        // For third row: (16 + 10) / 2 = 13
        assert_eq!(avg.f64().unwrap().get(2).unwrap(), 13.0);
    }
} 