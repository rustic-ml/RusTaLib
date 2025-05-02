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

#[cfg(test)]
mod tests {
    use super::*;
    use std::f64::consts::PI;
    use approx::assert_relative_eq;
    
    #[test]
    fn test_create_cyclical_time_features() {
        // Create test DataFrame with time column
        let time_data = vec![
            "2023-01-01 00:00:00 UTC", // Sunday midnight
            "2023-01-01 06:00:00 UTC", // Sunday 6 AM
            "2023-01-01 12:00:00 UTC", // Sunday noon
            "2023-01-01 18:00:00 UTC", // Sunday 6 PM
            "2023-01-02 12:00:00 UTC", // Monday noon
            "2023-01-03 12:00:00 UTC", // Tuesday noon
            "2023-01-04 12:00:00 UTC", // Wednesday noon
        ];
        
        let time_series = Series::new("timestamp".into(), time_data);
        let df = DataFrame::new(vec![time_series.into()]).unwrap();
        
        // Get cyclical features
        let features = create_cyclical_time_features(&df, "timestamp", "%Y-%m-%d %H:%M:%S UTC").unwrap();
        
        // We should have 4 feature series
        assert_eq!(features.len(), 4);
        
        // Check naming
        assert_eq!(features[0].name(), "hour_sin");
        assert_eq!(features[1].name(), "hour_cos");
        assert_eq!(features[2].name(), "day_of_week_sin");
        assert_eq!(features[3].name(), "day_of_week_cos");
        
        // Verify values
        
        // Midnight should have hour_sin = 0, hour_cos = 1
        assert_relative_eq!(features[0].f64().unwrap().get(0).unwrap(), 0.0, epsilon = 1e-10);
        assert_relative_eq!(features[1].f64().unwrap().get(0).unwrap(), 1.0, epsilon = 1e-10);
        
        // 6 AM should have hour_sin = 0.5, hour_cos = 0.866... (30 degrees)
        assert_relative_eq!(features[0].f64().unwrap().get(1).unwrap(), (PI/2.0).sin(), epsilon = 1e-10);
        assert_relative_eq!(features[1].f64().unwrap().get(1).unwrap(), (PI/2.0).cos(), epsilon = 1e-10);
        
        // Noon should have hour_sin = 0, hour_cos = -1 (180 degrees)
        assert_relative_eq!(features[0].f64().unwrap().get(2).unwrap(), (PI).sin(), epsilon = 1e-10);
        assert_relative_eq!(features[1].f64().unwrap().get(2).unwrap(), (PI).cos(), epsilon = 1e-10);
        
        // 6 PM should have hour_sin = -0.5, hour_cos = 0.866... (270 degrees)
        assert_relative_eq!(features[0].f64().unwrap().get(3).unwrap(), (3.0*PI/2.0).sin(), epsilon = 1e-10);
        assert_relative_eq!(features[1].f64().unwrap().get(3).unwrap(), (3.0*PI/2.0).cos(), epsilon = 1e-10);
        
        // Sunday should have day_of_week_sin = 0, day_of_week_cos = 1
        assert_relative_eq!(features[2].f64().unwrap().get(0).unwrap(), (2.0*PI*6.0/7.0).sin(), epsilon = 1e-10);
        assert_relative_eq!(features[3].f64().unwrap().get(0).unwrap(), (2.0*PI*6.0/7.0).cos(), epsilon = 1e-10);
        
        // Monday through Wednesday should have incremental values
        for i in 4..7 {
            let day = i - 4;
            let expected_sin = (2.0 * PI * day as f64 / 7.0).sin();
            let expected_cos = (2.0 * PI * day as f64 / 7.0).cos();
            assert_relative_eq!(features[2].f64().unwrap().get(i).unwrap(), expected_sin, epsilon = 1e-10);
            assert_relative_eq!(features[3].f64().unwrap().get(i).unwrap(), expected_cos, epsilon = 1e-10);
        }
    }
    
    #[test]
    fn test_create_cyclical_time_features_invalid_format() {
        // Create test DataFrame with improperly formatted time
        let time_data = vec![
            "2023-01-01 00:00:00 UTC", // Correct format
            "2023/01/01 06:00:00",     // Incorrect format
        ];
        
        let time_series = Series::new("timestamp".into(), time_data);
        let df = DataFrame::new(vec![time_series.into()]).unwrap();
        
        // Get cyclical features
        let features = create_cyclical_time_features(&df, "timestamp", "%Y-%m-%d %H:%M:%S UTC").unwrap();
        
        // First row should have valid values
        assert_relative_eq!(features[0].f64().unwrap().get(0).unwrap(), 0.0, epsilon = 1e-10);
        assert_relative_eq!(features[1].f64().unwrap().get(0).unwrap(), 1.0, epsilon = 1e-10);
        
        // Second row should have default values due to parsing error
        assert_relative_eq!(features[0].f64().unwrap().get(1).unwrap(), 0.0, epsilon = 1e-10);
        assert_relative_eq!(features[1].f64().unwrap().get(1).unwrap(), 1.0, epsilon = 1e-10);
    }
    
    #[test]
    #[should_panic(expected = "not found")]
    fn test_create_cyclical_time_features_missing_column() {
        // Create test DataFrame with no time column
        let dummy_series = Series::new("dummy".into(), &[1, 2, 3]);
        let df = DataFrame::new(vec![dummy_series.into()]).unwrap();
        
        // This should panic as we're requesting a non-existent column
        let _ = create_cyclical_time_features(&df, "timestamp", "%Y-%m-%d %H:%M:%S UTC").unwrap();
    }
} 