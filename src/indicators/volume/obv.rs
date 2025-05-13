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
            "OBV calculation requires both close and volume columns".into(),
        ));
    }

    let close = df.column("close")?.f64()?;
    let volume = df.column("volume")?.f64()?;

    let mut obv = Vec::with_capacity(df.height());

    // First value
    obv.push(volume.get(0).unwrap_or(0.0));

    for i in 1..df.height() {
        let curr_close = close.get(i).unwrap_or(0.0);
        let prev_close = close.get(i - 1).unwrap_or(0.0);
        let curr_volume = volume.get(i).unwrap_or(0.0);

        if curr_close > prev_close {
            obv.push(obv[i - 1] + curr_volume);
        } else if curr_close < prev_close {
            obv.push(obv[i - 1] - curr_volume);
        } else {
            // If prices are equal, OBV doesn't change
            obv.push(obv[i - 1]);
        }
    }

    Ok(Series::new("obv".into(), obv))
}
