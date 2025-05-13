use polars::prelude::*;

/// Calculate Ultimate Oscillator
///
/// Returns a Series with Ultimate Oscillator values
pub fn calculate_ultimate_oscillator(df: &DataFrame, high_col: &str, low_col: &str, close_col: &str, short: usize, medium: usize, long: usize) -> PolarsResult<Series> {
    let high = df.column(high_col)?.f64()?;
    let low = df.column(low_col)?.f64()?;
    let close = df.column(close_col)?.f64()?;
    let len = df.height();
    let mut bp = vec![f64::NAN; len];
    let mut tr = vec![f64::NAN; len];
    for i in 0..len {
        if i == 0 {
            bp[i] = close.get(i).unwrap_or(f64::NAN) - low.get(i).unwrap_or(f64::NAN);
            tr[i] = high.get(i).unwrap_or(f64::NAN) - low.get(i).unwrap_or(f64::NAN);
        } else {
            let prev_close = close.get(i-1).unwrap_or(f64::NAN);
            bp[i] = close.get(i).unwrap_or(f64::NAN) - low.get(i).unwrap_or(f64::NAN).min(prev_close);
            tr[i] = high.get(i).unwrap_or(f64::NAN).max(prev_close) - low.get(i).unwrap_or(f64::NAN).min(prev_close);
        }
    }
    let mut uo = vec![f64::NAN; len];
    for i in 0..len {
        if i+1 >= long {
            let sum = |v: &Vec<f64>, w: usize, idx: usize| v[(idx+1-w)..=idx].iter().filter(|x| !x.is_nan()).sum::<f64>();
            let avg = |w: usize, idx: usize| {
                let bp_sum = sum(&bp, w, idx);
                let tr_sum = sum(&tr, w, idx);
                if tr_sum == 0.0 { f64::NAN } else { bp_sum / tr_sum }
            };
            let s = avg(short, i);
            let m = avg(medium, i);
            let l = avg(long, i);
            if s.is_nan() || m.is_nan() || l.is_nan() { continue; }
            uo[i] = 100.0 * (4.0*s + 2.0*m + l) / 7.0;
        }
    }
    Ok(Series::new("ultimate_oscillator".into(), uo))
} 