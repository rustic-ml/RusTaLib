use polars::prelude::*;
use crate::util::dataframe_utils::check_window_size;

/// Calculates Rate of Change (ROC)
/// Formula: ((price / prevPrice) - 1) * 100
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for ROC (typically 10)
/// * `column` - Column name to use for calculations (default "close")
///
/// # Returns
///
/// Returns a PolarsResult containing the ROC Series
pub fn calculate_roc(df: &DataFrame, window: usize, column: &str) -> PolarsResult<Series> {
    check_window_size(df, window, "ROC")?;
    
    let price = df.column(column)?.f64()?;
    let prev_price = price.shift(window as i64);
    
    let mut roc_values = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let current = price.get(i).unwrap_or(0.0);
        let prev = prev_price.get(i).unwrap_or(0.0);
        
        if prev != 0.0 {
            let roc = ((current / prev) - 1.0) * 100.0;
            roc_values.push(roc);
        } else {
            roc_values.push(f64::NAN);
        }
    }
    
    Ok(Series::new("roc".into(), roc_values))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_calculate_roc_basic() {
        // Create test DataFrame
        let price = Series::new("close".into(), &[10.0, 11.0, 12.0, 13.0, 14.0, 13.0, 12.0, 13.0, 14.0, 15.0]);
        let df = DataFrame::new(vec![price.into()]).unwrap();
        let window = 3;
        
        let roc = calculate_roc(&df, window, "close").unwrap();
        
        // First 'window' values should be NaN
        for i in 0..window {
            assert!(roc.f64().unwrap().get(i).unwrap().is_nan());
        }
        
        // Manual calculation for index 3:
        // ROC = ((price/prevPrice) - 1) * 100 = ((13/10) - 1) * 100 = 30%
        assert!((roc.f64().unwrap().get(3).unwrap() - 30.0).abs() < 1e-10);
        
        // Manual calculation for index 6:
        // ROC = ((price/prevPrice) - 1) * 100 = ((12/13) - 1) * 100 = -7.69%
        assert!((roc.f64().unwrap().get(6).unwrap() + 7.69).abs() < 0.01);
    }
} 