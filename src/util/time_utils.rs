use chrono::{Datelike, NaiveDateTime, Timelike, NaiveDate};
use polars::prelude::*;
use std::f64::consts::PI;

/// Parse a date string into a NaiveDate object
///
/// # Arguments
///
/// * `date_str` - Date string in YYYY-MM-DD format
///
/// # Returns
///
/// Returns a Result with NaiveDate on success, or error on failure
pub fn parse_date(date_str: &str) -> Result<NaiveDate, chrono::ParseError> {
    NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
}

/// Format a NaiveDate into a string
///
/// # Arguments
///
/// * `date` - NaiveDate object to format
///
/// # Returns
///
/// Returns a formatted date string in YYYY-MM-DD format
pub fn format_date(date: &NaiveDate) -> String {
    date.format("%Y-%m-%d").to_string()
}

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
    time_format: &str,
) -> PolarsResult<Vec<Series>> {
    // Extract and validate time column
    let time_series = df.column(time_column)?;
    let time_strs = time_series.str()?;

    // Initialize vectors for storing sine and cosine features
    let mut hour_sin = Vec::with_capacity(df.height());
    let mut hour_cos = Vec::with_capacity(df.height());
    let mut day_sin = Vec::with_capacity(df.height());
    let mut day_cos = Vec::with_capacity(df.height());

    // Create Timezone-naïve chrono format
    let format_str = time_format.replace(" UTC", "");

    for i in 0..df.height() {
        let time_str = time_strs.get(i).unwrap_or("");
        let datetime = match NaiveDateTime::parse_from_str(time_str, &format_str) {
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
