use polars::prelude::*;

/// Calculate Gamma Exposure (GEX)
/// Expects columns: 'gamma', 'open_interest', 'contract_multiplier'
pub fn calculate_gamma_exposure(df: &DataFrame, gamma_col: &str, oi_col: &str, multiplier_col: &str) -> PolarsResult<Series> {
    let gamma = df.column(gamma_col)?.f64()?;
    let oi = df.column(oi_col)?.f64()?;
    let multiplier = df.column(multiplier_col)?.f64()?;
    let len = df.height();
    let mut gex = vec![f64::NAN; len];
    for i in 0..len {
        let g = gamma.get(i).unwrap_or(f64::NAN);
        let o = oi.get(i).unwrap_or(f64::NAN);
        let m = multiplier.get(i).unwrap_or(100.0);
        gex[i] = g * o * m;
    }
    Ok(Series::new("gamma_exposure".into(), gex))
} 