use polars::prelude::*;

/// Calculate Vortex Indicator (VI+ and VI-)
///
/// Returns (vi_plus, vi_minus) as Series
pub fn calculate_vortex(
    df: &DataFrame,
    high_col: &str,
    low_col: &str,
    close_col: &str,
    period: usize,
) -> PolarsResult<(Series, Series)> {
    let high = df.column(high_col)?.f64()?;
    let low = df.column(low_col)?.f64()?;
    let close = df.column(close_col)?.f64()?;
    let len = df.height();
    let mut tr = vec![f64::NAN; len];
    let mut vm_plus = vec![f64::NAN; len];
    let mut vm_minus = vec![f64::NAN; len];
    for i in 1..len {
        let h = high.get(i).unwrap_or(f64::NAN);
        let l = low.get(i).unwrap_or(f64::NAN);
        let h_prev = high.get(i - 1).unwrap_or(f64::NAN);
        let l_prev = low.get(i - 1).unwrap_or(f64::NAN);
        let c_prev = close.get(i - 1).unwrap_or(f64::NAN);
        tr[i] = h.max(c_prev) - l.min(c_prev);
        vm_plus[i] = (h - l_prev).abs();
        vm_minus[i] = (l - h_prev).abs();
    }
    let mut vi_plus = vec![f64::NAN; len];
    let mut vi_minus = vec![f64::NAN; len];
    for i in 0..len {
        if i + 1 >= period {
            let sum_tr: f64 = tr[(i + 1 - period)..=i]
                .iter()
                .filter(|x| !x.is_nan())
                .sum();
            let sum_vm_plus: f64 = vm_plus[(i + 1 - period)..=i]
                .iter()
                .filter(|x| !x.is_nan())
                .sum();
            let sum_vm_minus: f64 = vm_minus[(i + 1 - period)..=i]
                .iter()
                .filter(|x| !x.is_nan())
                .sum();
            if sum_tr != 0.0 {
                vi_plus[i] = sum_vm_plus / sum_tr;
                vi_minus[i] = sum_vm_minus / sum_tr;
            }
        }
    }
    Ok((
        Series::new("vi_plus".into(), vi_plus),
        Series::new("vi_minus".into(), vi_minus),
    ))
}
