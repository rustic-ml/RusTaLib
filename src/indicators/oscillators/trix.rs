use polars::prelude::*;

/// Calculate TRIX (Triple Exponential Average)
///
/// Returns a Series with TRIX values
pub fn calculate_trix(df: &DataFrame, close_col: &str, period: usize) -> PolarsResult<Series> {
    let close = df.column(close_col)?.f64()?;
    let len = df.height();
    let mut ema1 = vec![f64::NAN; len];
    let mut ema2 = vec![f64::NAN; len];
    let mut ema3 = vec![f64::NAN; len];
    let alpha = 2.0 / (period as f64 + 1.0);
    // First EMA
    for i in 0..len {
        if i == 0 {
            ema1[i] = close.get(i).unwrap_or(f64::NAN);
        } else {
            ema1[i] = alpha * close.get(i).unwrap_or(f64::NAN) + (1.0 - alpha) * ema1[i-1];
        }
    }
    // Second EMA
    for i in 0..len {
        if i == 0 {
            ema2[i] = ema1[i];
        } else {
            ema2[i] = alpha * ema1[i] + (1.0 - alpha) * ema2[i-1];
        }
    }
    // Third EMA
    for i in 0..len {
        if i == 0 {
            ema3[i] = ema2[i];
        } else {
            ema3[i] = alpha * ema2[i] + (1.0 - alpha) * ema3[i-1];
        }
    }
    // TRIX: 1-period percent rate of change of triple EMA
    let mut trix = vec![f64::NAN; len];
    for i in 1..len {
        if ema3[i-1] != 0.0 {
            trix[i] = 100.0 * (ema3[i] - ema3[i-1]) / ema3[i-1];
        }
    }
    Ok(Series::new("trix".into(), trix))
} 