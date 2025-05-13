use polars::prelude::*;

/// Calculate Put/Call Open Interest Ratio
/// Expects columns: 'open_interest', 'is_call' (bool)
pub fn calculate_put_call_oi_ratio(df: &DataFrame, oi_col: &str, is_call_col: &str, window: usize) -> PolarsResult<Series> {
    let oi = df.column(oi_col)?.f64()?;
    let is_call = df.column(is_call_col)?.bool()?;
    let len = df.height();
    let mut put_oi = vec![0.0; len];
    let mut call_oi = vec![0.0; len];
    for i in 0..len {
        let val = oi.get(i).unwrap_or(f64::NAN);
        let call = is_call.get(i).unwrap_or(false);
        if call {
            call_oi[i] = val;
        } else {
            put_oi[i] = val;
        }
    }
    let mut ratio = vec![f64::NAN; len];
    for i in 0..len {
        if i+1 >= window {
            let put_sum: f64 = put_oi[(i+1-window)..=i].iter().sum();
            let call_sum: f64 = call_oi[(i+1-window)..=i].iter().sum();
            if call_sum > 0.0 {
                ratio[i] = put_sum / call_sum;
            }
        }
    }
    Ok(Series::new("put_call_oi_ratio".into(), ratio))
}

/// Calculate Skew-based Sentiment
/// Expects columns: 'iv_call', 'iv_put'
pub fn calculate_skew_sentiment(df: &DataFrame, iv_call_col: &str, iv_put_col: &str) -> PolarsResult<Series> {
    let iv_call = df.column(iv_call_col)?.f64()?;
    let iv_put = df.column(iv_put_col)?.f64()?;
    let len = df.height();
    let mut skew = vec![f64::NAN; len];
    for i in 0..len {
        let call_iv = iv_call.get(i).unwrap_or(f64::NAN);
        let put_iv = iv_put.get(i).unwrap_or(f64::NAN);
        if call_iv > 0.0 {
            skew[i] = (put_iv - call_iv) / call_iv;
        }
    }
    Ok(Series::new("skew_sentiment".into(), skew))
} 