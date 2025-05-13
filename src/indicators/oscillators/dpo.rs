use polars::prelude::*;

/// Calculate Detrended Price Oscillator (DPO)
///
/// Returns a Series with DPO values
pub fn calculate_dpo(df: &DataFrame, close_col: &str, period: usize) -> PolarsResult<Series> {
    let close = df.column(close_col)?.f64()?;
    let len = df.height();
    let mut dpo = vec![f64::NAN; len];
    let shift = period / 2 + 1;
    for i in 0..len {
        if i+1 >= period {
            let sma: f64 = close.slice((i+1-period) as i64, period).mean().unwrap_or(f64::NAN);
            if i >= shift {
                dpo[i] = close.get(i-shift).unwrap_or(f64::NAN) - sma;
            }
        }
    }
    Ok(Series::new("dpo".into(), dpo))
} 