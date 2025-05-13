use polars::prelude::*;

/// Calculate Percentage Price Oscillator (PPO)
///
/// Returns a Series with PPO values
pub fn calculate_ppo(df: &DataFrame, close_col: &str, fast_period: usize, slow_period: usize) -> PolarsResult<Series> {
    let close = df.column(close_col)?.f64()?;
    let len = df.height();
    let mut ema_fast = vec![f64::NAN; len];
    let mut ema_slow = vec![f64::NAN; len];
    let alpha_fast = 2.0 / (fast_period as f64 + 1.0);
    let alpha_slow = 2.0 / (slow_period as f64 + 1.0);
    for i in 0..len {
        if i == 0 {
            ema_fast[i] = close.get(i).unwrap_or(f64::NAN);
            ema_slow[i] = close.get(i).unwrap_or(f64::NAN);
        } else {
            ema_fast[i] = alpha_fast * close.get(i).unwrap_or(f64::NAN) + (1.0 - alpha_fast) * ema_fast[i-1];
            ema_slow[i] = alpha_slow * close.get(i).unwrap_or(f64::NAN) + (1.0 - alpha_slow) * ema_slow[i-1];
        }
    }
    let mut ppo = vec![f64::NAN; len];
    for i in 0..len {
        if ema_slow[i] != 0.0 {
            ppo[i] = 100.0 * (ema_fast[i] - ema_slow[i]) / ema_slow[i];
        }
    }
    Ok(Series::new("ppo".into(), ppo))
} 