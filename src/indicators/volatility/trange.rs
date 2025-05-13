use polars::prelude::*;

/// Calculates True Range (TRANGE)
/// True Range is the greatest of the following:
/// 1. Current High - Current Low
/// 2. |Current High - Previous Close|
/// 3. |Current Low - Previous Close|
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
///
/// # Returns
///
/// Returns a PolarsResult containing the True Range Series
pub fn calculate_trange(df: &DataFrame) -> PolarsResult<Series> {
    // Check required columns
    if !df.schema().contains("high")
        || !df.schema().contains("low")
        || !df.schema().contains("close")
    {
        return Err(PolarsError::ShapeMismatch(
            "DataFrame must contain 'high', 'low', and 'close' columns for TRANGE calculation"
                .into(),
        ));
    }

    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    let prev_close = close.shift(1);

    let mut tr_values = Vec::with_capacity(df.height());

    // For the first value, True Range is simply High - Low
    // (since we don't have a previous close)
    let first_tr = {
        let h = high.get(0).unwrap_or(f64::NAN);
        let l = low.get(0).unwrap_or(f64::NAN);

        if h.is_nan() || l.is_nan() {
            f64::NAN
        } else {
            h - l
        }
    };
    tr_values.push(first_tr);

    // Calculate True Range for the rest of the data points
    for i in 1..df.height() {
        let h = high.get(i).unwrap_or(f64::NAN);
        let l = low.get(i).unwrap_or(f64::NAN);
        let pc = prev_close.get(i).unwrap_or(f64::NAN);

        if h.is_nan() || l.is_nan() || pc.is_nan() {
            tr_values.push(f64::NAN);
        } else {
            let range1 = h - l;
            let range2 = (h - pc).abs();
            let range3 = (l - pc).abs();

            let tr = range1.max(range2).max(range3);
            tr_values.push(tr);
        }
    }

    Ok(Series::new("trange".into(), tr_values))
}
