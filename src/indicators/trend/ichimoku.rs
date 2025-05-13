use polars::prelude::*;

/// Calculate Ichimoku Cloud indicator
///
/// Returns (tenkan_sen, kijun_sen, senkou_span_a, senkou_span_b, chikou_span)
pub fn calculate_ichimoku_cloud(
    df: &DataFrame,
    high_col: &str,
    low_col: &str,
    close_col: &str,
    tenkan: usize,
    kijun: usize,
    senkou_b: usize,
) -> PolarsResult<(Series, Series, Series, Series, Series)> {
    let high = df.column(high_col)?.f64()?;
    let low = df.column(low_col)?.f64()?;
    let close = df.column(close_col)?.f64()?;
    let len = df.height();
    let mut tenkan_sen = vec![f64::NAN; len];
    let mut kijun_sen = vec![f64::NAN; len];
    let mut senkou_span_a = vec![f64::NAN; len];
    let mut senkou_span_b = vec![f64::NAN; len];
    let mut chikou_span = vec![f64::NAN; len];
    for i in 0..len {
        if i + 1 >= tenkan {
            let h = high.slice((i + 1 - tenkan) as i64, tenkan);
            let l = low.slice((i + 1 - tenkan) as i64, tenkan);
            tenkan_sen[i] = h.max().unwrap() + l.min().unwrap();
            tenkan_sen[i] /= 2.0;
        }
        if i + 1 >= kijun {
            let h = high.slice((i + 1 - kijun) as i64, kijun);
            let l = low.slice((i + 1 - kijun) as i64, kijun);
            kijun_sen[i] = h.max().unwrap() + l.min().unwrap();
            kijun_sen[i] /= 2.0;
        }
        if i + 1 >= kijun {
            senkou_span_a[i] = (tenkan_sen[i] + kijun_sen[i]) / 2.0;
        }
        if i + 1 >= senkou_b {
            let h = high.slice((i + 1 - senkou_b) as i64, senkou_b);
            let l = low.slice((i + 1 - senkou_b) as i64, senkou_b);
            senkou_span_b[i] = h.max().unwrap() + l.min().unwrap();
            senkou_span_b[i] /= 2.0;
        }
        if i + 26 < len {
            chikou_span[i] = close.get(i + 26).unwrap_or(f64::NAN);
        }
    }
    Ok((
        Series::new("tenkan_sen".into(), tenkan_sen),
        Series::new("kijun_sen".into(), kijun_sen),
        Series::new("senkou_span_a".into(), senkou_span_a),
        Series::new("senkou_span_b".into(), senkou_span_b),
        Series::new("chikou_span".into(), chikou_span),
    ))
}
