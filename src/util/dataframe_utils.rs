use polars::frame::column::Column;
use polars::prelude::*;

/// Ensure a column in a DataFrame is of Float64 type
///
/// # Arguments
///
/// * `df` - DataFrame to modify
/// * `column_name` - Name of the column to convert
///
/// # Returns
///
/// Returns a PolarsResult indicating success or failure
pub fn ensure_f64_column(df: &mut DataFrame, column_name: &str) -> PolarsResult<()> {
    // 1) Wrap the existing Series in a Column for in-place mutation
    let s: Series = df.column(column_name)?.as_materialized_series().clone();
    let mut col: Column = s.into_column();

    // 2) Materialize and get a &mut Series to cast in place
    let series_mut: &mut Series = col.into_materialized_series();
    *series_mut = series_mut.cast(&DataType::Float64)?;

    // 3) Convert the Column back into a Series and replace it in the DataFrame
    let series: Series = col.take_materialized_series();
    df.replace(column_name, series)?;

    Ok(())
}

/// Check if a DataFrame has enough rows for a given window size
///
/// # Arguments
///
/// * `df` - The DataFrame to check
/// * `window` - The window size required
/// * `indicator_name` - Name of the indicator (for error message)
///
/// # Returns
///
/// Returns a PolarsResult<()> or an error if there are not enough rows
pub fn check_window_size(df: &DataFrame, window: usize, indicator_name: &str) -> PolarsResult<()> {
    if df.height() < window {
        return Err(PolarsError::ComputeError(
            format!(
                "Not enough data points ({}) for {} window ({})",
                df.height(),
                indicator_name,
                window
            )
            .into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    #[test]
    fn test_ensure_f64_column() {
        // Test with integer column
        let int_series = Series::new("values".into(), &[1, 2, 3, 4, 5]);
        let mut df = DataFrame::new(vec![int_series.into()]).unwrap();

        // Ensure the column is converted to f64
        ensure_f64_column(&mut df, "values").unwrap();

        // Verify the column type is now Float64
        assert_eq!(df.column("values").unwrap().dtype(), &DataType::Float64);

        // Verify values were properly converted
        let expected = &[1.0, 2.0, 3.0, 4.0, 5.0];
        for (i, val) in expected.iter().enumerate() {
            assert_eq!(
                df.column("values").unwrap().f64().unwrap().get(i).unwrap(),
                *val
            );
        }
    }

    #[test]
    fn test_ensure_f64_column_already_f64() {
        // Test with already f64 column
        let f64_series = Series::new("values".into(), &[1.0, 2.0, 3.0, 4.0, 5.0]);
        let mut df = DataFrame::new(vec![f64_series.into()]).unwrap();

        // Ensure the column is f64 (should be a no-op)
        ensure_f64_column(&mut df, "values").unwrap();

        // Verify the column type is still Float64
        assert_eq!(df.column("values").unwrap().dtype(), &DataType::Float64);

        // Verify values are unchanged
        let expected = &[1.0, 2.0, 3.0, 4.0, 5.0];
        for (i, val) in expected.iter().enumerate() {
            assert_eq!(
                df.column("values").unwrap().f64().unwrap().get(i).unwrap(),
                *val
            );
        }
    }

    #[test]
    #[should_panic(expected = "not found")]
    fn test_ensure_f64_column_nonexistent() {
        // Test with nonexistent column
        let series = Series::new("values".into(), &[1, 2, 3, 4, 5]);
        let mut df = DataFrame::new(vec![series.into()]).unwrap();

        // Try to ensure a nonexistent column is f64
        ensure_f64_column(&mut df, "nonexistent").unwrap();
    }

    #[test]
    fn test_check_window_size_sufficient() {
        // Test with sufficient data points
        let series = Series::new("values".into(), &[1.0, 2.0, 3.0, 4.0, 5.0]);
        let df = DataFrame::new(vec![series.into()]).unwrap();

        // Window size is less than number of rows
        let result = check_window_size(&df, 3, "Test");
        assert!(result.is_ok());

        // Window size equals number of rows
        let result = check_window_size(&df, 5, "Test");
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_window_size_insufficient() {
        // Test with insufficient data points
        let series = Series::new("values".into(), &[1.0, 2.0, 3.0]);
        let df = DataFrame::new(vec![series.into()]).unwrap();

        // Window size is greater than number of rows
        let result = check_window_size(&df, 4, "Test");
        assert!(result.is_err());

        // Check error message
        let err = result.unwrap_err();
        match err {
            PolarsError::ComputeError(msg) => {
                assert!(msg.contains("Not enough data points"));
                assert!(msg.contains("Test"));
            }
            _ => panic!("Expected ComputeError, got {:?}", err),
        }
    }
}
