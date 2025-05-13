use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Standard Deviation (StdDev) of a series over a window
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for StdDev calculation (typically 14 or 20)
/// * `column` - Column to calculate StdDev on (usually "close")
///
/// # Returns
///
/// Returns a PolarsResult containing the StdDev Series
///
/// # Example
///
/// ```
/// use polars::prelude::*;
/// use ta_lib_in_rust::indicators::volatility::calculate_stddev;
///
/// let close = Series::new("close".into(), &[10.0, 11.0, 12.0, 9.0, 8.0, 10.0]);
/// let df = DataFrame::new(vec![close.into()]).unwrap();
///
/// let stddev = calculate_stddev(&df, 3, "close").unwrap();
/// ```
pub fn calculate_stddev(df: &DataFrame, window: usize, column: &str) -> PolarsResult<Series> {
    // Check window size
    check_window_size(df, window, "StdDev")?;

    // Check if the specified column exists
    if !df.schema().contains(column) {
        return Err(PolarsError::ShapeMismatch(
            format!("DataFrame must contain '{}' column for StdDev calculation", column).into(),
        ));
    }

    // Get the column to calculate StdDev on
    let col = df.column(column)?.f64()?;
    
    // Calculate rolling standard deviation
    let mut stddev_values = Vec::with_capacity(df.height());
    
    // Fill NaN for the first window-1 elements
    for _ in 0..(window - 1) {
        stddev_values.push(f64::NAN);
    }
    
    // Calculate StdDev for each window
    for i in (window - 1)..df.height() {
        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        let mut count = 0;
        
        for j in (i - window + 1)..=i {
            let val = col.get(j).unwrap_or(f64::NAN);
            if !val.is_nan() {
                sum += val;
                sum_sq += val * val;
                count += 1;
            }
        }
        
        if count > 1 {
            let mean = sum / count as f64;
            let variance = sum_sq / count as f64 - mean * mean;
            let stddev = if variance > 0.0 { variance.sqrt() } else { 0.0 };
            stddev_values.push(stddev);
        } else {
            stddev_values.push(f64::NAN);
        }
    }
    
    Ok(Series::new("stddev".into(), stddev_values))
} 