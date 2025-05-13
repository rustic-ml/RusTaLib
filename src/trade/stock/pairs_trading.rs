use polars::prelude::*;

/// Calculate Pairs Trading Z-score
///
/// Returns a Series with z-score of the spread between two stocks
pub fn calculate_pairs_zscore(stock1_df: &DataFrame, stock1_col: &str, stock2_df: &DataFrame, stock2_col: &str, window: usize) -> PolarsResult<Series> {
    let s1 = stock1_df.column(stock1_col)?.f64()?;
    let s2 = stock2_df.column(stock2_col)?.f64()?;
    let len = s1.len().min(s2.len());
    let mut spread = vec![f64::NAN; len];
    for i in 0..len {
        spread[i] = s1.get(i).unwrap_or(f64::NAN) - s2.get(i).unwrap_or(f64::NAN);
    }
    let mut zscore = vec![f64::NAN; len];
    for i in 0..len {
        if i+1 >= window {
            let window_slice = &spread[(i+1-window)..=i];
            let mean = window_slice.iter().cloned().sum::<f64>() / window as f64;
            let std = (window_slice.iter().map(|x| (x-mean).powi(2)).sum::<f64>() / window as f64).sqrt();
            if std > 0.0 {
                zscore[i] = (spread[i] - mean) / std;
            }
        }
    }
    Ok(Series::new("pairs_zscore".into(), zscore))
} 