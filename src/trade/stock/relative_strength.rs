use polars::prelude::*;

/// Calculate Relative Strength vs. Index
///
/// Returns a Series with relative strength values (stock/benchmark ratio)
pub fn calculate_relative_strength(stock_df: &DataFrame, stock_col: &str, bench_df: &DataFrame, bench_col: &str) -> PolarsResult<Series> {
    let stock = stock_df.column(stock_col)?.f64()?;
    let bench = bench_df.column(bench_col)?.f64()?;
    let len = stock.len().min(bench.len());
    let mut rel_strength = vec![f64::NAN; len];
    for i in 0..len {
        let s = stock.get(i).unwrap_or(f64::NAN);
        let b = bench.get(i).unwrap_or(f64::NAN);
        if b != 0.0 {
            rel_strength[i] = s / b;
        }
    }
    Ok(Series::new("relative_strength".into(), rel_strength))
} 