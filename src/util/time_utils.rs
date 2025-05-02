use polars::prelude::*;
use chrono::{NaiveDateTime, Timelike, Datelike};
use std::f64::consts::PI;

/// Create time-based cyclical features from a time column
///
/// # Arguments
///
/// * `df` - DataFrame containing a time column
/// * `time_column` - Name of the time column (default: "time")
/// * `time_format` - Format of the time strings (default: "%Y-%m-%d %H:%M:%S UTC")
///
/// # Returns
///
/// Returns a Result containing a vector of Series with cyclical time features
pub fn create_cyclical_time_features(
    df: &DataFrame, 
    time_column: &str,
    time_format: &str
) -> PolarsResult<Vec<Series>> {
    // Check if the time column exists
    if !df.schema().contains(time_column) {
        return Err(PolarsError::ComputeError(
            format!("Time column '{}' not found", time_column).into()
        ));
    }
    
    let time_col = df.column(time_column)?.str()?;
    let n_rows = df.height();
    
    // Create vectors for hour and day of week features
    let mut hour_sin = Vec::with_capacity(n_rows);
    let mut hour_cos = Vec::with_capacity(n_rows);
    let mut day_sin = Vec::with_capacity(n_rows);
    let mut day_cos = Vec::with_capacity(n_rows);
    
    for i in 0..n_rows {
        let time_str = time_col.get(i).unwrap_or("");
        let datetime = match NaiveDateTime::parse_from_str(time_str, time_format) {
            Ok(dt) => dt,
            Err(_) => {
                // Default values if parsing fails
                hour_sin.push(0.0);
                hour_cos.push(1.0);
                day_sin.push(0.0);
                day_cos.push(1.0);
                continue;
            }
        };
        
        // Extract hour (0-23) and day of week (0-6)
        let hour = datetime.hour() as f64;
        let day = datetime.weekday().num_days_from_monday() as f64;
        
        // Encode using sine and cosine to capture cyclical patterns
        hour_sin.push((2.0 * PI * hour / 24.0).sin());
        hour_cos.push((2.0 * PI * hour / 24.0).cos());
        day_sin.push((2.0 * PI * day / 7.0).sin());
        day_cos.push((2.0 * PI * day / 7.0).cos());
    }
    
    // Create series
    let result = vec![
        Series::new("hour_sin".into(), hour_sin),
        Series::new("hour_cos".into(), hour_cos),
        Series::new("day_of_week_sin".into(), day_sin),
        Series::new("day_of_week_cos".into(), day_cos),
    ];
    
    Ok(result)
} 