use polars::prelude::*;

/// Calculate Accumulation/Distribution Line (ADL)
///
/// Returns a Series with ADL values
pub fn calculate_adl(
    df: &DataFrame,
    high_col: &str,
    low_col: &str,
    close_col: &str,
    volume_col: &str,
) -> PolarsResult<Series> {
    let high = df.column(high_col)?.f64()?;
    let low = df.column(low_col)?.f64()?;
    let close = df.column(close_col)?.f64()?;
    let volume = df.column(volume_col)?.f64()?;
    let len = df.height();
    let mut adl = vec![0.0; len];
    for i in 0..len {
        let high = high.get(i).unwrap_or(f64::NAN);
        let low = low.get(i).unwrap_or(f64::NAN);
        let close = close.get(i).unwrap_or(f64::NAN);
        let volume = volume.get(i).unwrap_or(f64::NAN);
        let mf_multiplier = if (high - low).abs() < f64::EPSILON {
            0.0
        } else {
            ((close - low) - (high - close)) / (high - low)
        };
        let mf_volume = mf_multiplier * volume;
        adl[i] = if i == 0 {
            mf_volume
        } else {
            adl[i - 1] + mf_volume
        };
    }
    Ok(Series::new("adl".into(), adl))
}
