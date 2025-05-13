use polars::prelude::*;

/// Calculate Price Volume Trend (PVT)
///
/// Returns a Series with PVT values
pub fn calculate_pvt(df: &DataFrame, close_col: &str, volume_col: &str) -> PolarsResult<Series> {
    let close = df.column(close_col)?.f64()?;
    let volume = df.column(volume_col)?.f64()?;
    let len = df.height();
    let mut pvt = vec![0.0; len];
    for i in 1..len {
        let prev_close = close.get(i - 1).unwrap_or(f64::NAN);
        let curr_close = close.get(i).unwrap_or(f64::NAN);
        let curr_vol = volume.get(i).unwrap_or(f64::NAN);
        if prev_close != 0.0 {
            pvt[i] = pvt[i - 1] + ((curr_close - prev_close) / prev_close) * curr_vol;
        } else {
            pvt[i] = pvt[i - 1];
        }
    }
    Ok(Series::new("pvt".into(), pvt))
}
