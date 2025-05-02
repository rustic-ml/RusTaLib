use polars::prelude::*;

/// Calculates On-Balance Volume (OBV)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price and volume data
///
/// # Returns
///
/// Returns a PolarsResult containing the OBV Series
pub fn calculate_obv(df: &DataFrame) -> PolarsResult<Series> {
    // Check for required columns
    if !df.schema().contains("close") || !df.schema().contains("volume") {
        return Err(PolarsError::ComputeError(
            "OBV calculation requires both close and volume columns".into()
        ));
    }
    
    let close = df.column("close")?.f64()?;
    let prev_close = close.shift(1);
    let volume = df.column("volume")?.f64()?;
    
    let mut obv = Vec::with_capacity(df.height());
    let mut cumulative = 0.0;
    
    // First value
    cumulative = volume.get(0).unwrap_or(0.0);
    obv.push(cumulative);
    
    for i in 1..df.height() {
        let curr_close = close.get(i).unwrap_or(0.0);
        let prev_close_val = prev_close.get(i).unwrap_or(0.0);
        let curr_volume = volume.get(i).unwrap_or(0.0);
        
        if curr_close > prev_close_val {
            cumulative += curr_volume;
        } else if curr_close < prev_close_val {
            cumulative -= curr_volume;
        }
        // If equal, no change
        
        obv.push(cumulative);
    }
    
    Ok(Series::new("obv".into(), obv))
}

/// Placeholder for future implementation of Chaikin Money Flow
pub fn calculate_cmf(_df: &DataFrame, _window: usize) -> PolarsResult<Series> {
    unimplemented!("Chaikin Money Flow calculation not yet implemented")
} 