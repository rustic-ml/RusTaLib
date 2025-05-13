use polars::prelude::*;

/// Calculate Donchian Channels
///
/// Returns (upper_band, lower_band, middle_band)
pub fn calculate_donchian_channels(
    df: &DataFrame,
    high_col: &str,
    low_col: &str,
    window: usize,
) -> PolarsResult<(Series, Series, Series)> {
    let high = df.column(high_col)?.f64()?;
    let low = df.column(low_col)?.f64()?;
    let len = df.height();
    let mut upper = vec![f64::NAN; len];
    let mut lower = vec![f64::NAN; len];
    let mut middle = vec![f64::NAN; len];
    for i in 0..len {
        if i + 1 >= window {
            let h = high.slice((i + 1 - window) as i64, window);
            let l = low.slice((i + 1 - window) as i64, window);
            upper[i] = h.max().unwrap();
            lower[i] = l.min().unwrap();
            middle[i] = (upper[i] + lower[i]) / 2.0;
        }
    }
    Ok((
        Series::new("donchian_upper".into(), upper),
        Series::new("donchian_lower".into(), lower),
        Series::new("donchian_middle".into(), middle),
    ))
}
