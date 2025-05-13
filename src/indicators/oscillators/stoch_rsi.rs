use polars::prelude::*;

/// Calculate Stochastic RSI
///
/// Returns a Series with StochRSI values
pub fn calculate_stoch_rsi(df: &DataFrame, close_col: &str, rsi_period: usize, stoch_period: usize) -> PolarsResult<Series> {
    let close = df.column(close_col)?.f64()?;
    let len = df.height();
    let mut rsi = vec![f64::NAN; len];
    // Calculate RSI
    let mut gain = vec![0.0; len];
    let mut loss = vec![0.0; len];
    for i in 1..len {
        let diff = close.get(i).unwrap_or(f64::NAN) - close.get(i-1).unwrap_or(f64::NAN);
        if diff > 0.0 {
            gain[i] = diff;
        } else {
            loss[i] = -diff;
        }
    }
    let mut avg_gain = vec![f64::NAN; len];
    let mut avg_loss = vec![f64::NAN; len];
    for i in 0..len {
        if i+1 >= rsi_period {
            let g: f64 = gain[(i+1-rsi_period)..=i].iter().sum::<f64>() / rsi_period as f64;
            let l: f64 = loss[(i+1-rsi_period)..=i].iter().sum::<f64>() / rsi_period as f64;
            avg_gain[i] = g;
            avg_loss[i] = l;
            let rs = if l == 0.0 { 100.0 } else { g / l };
            rsi[i] = 100.0 - (100.0 / (1.0 + rs));
        }
    }
    // Calculate StochRSI
    let mut stoch_rsi = vec![f64::NAN; len];
    for i in 0..len {
        if i+1 >= stoch_period {
            let min_rsi = rsi[(i+1-stoch_period)..=i].iter().cloned().fold(f64::INFINITY, f64::min);
            let max_rsi = rsi[(i+1-stoch_period)..=i].iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let denom = max_rsi - min_rsi;
            if denom.abs() > std::f64::EPSILON {
                stoch_rsi[i] = (rsi[i] - min_rsi) / denom;
            }
        }
    }
    Ok(Series::new("stoch_rsi".into(), stoch_rsi))
} 