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
///
/// # Example
///
/// ```
/// use polars::prelude::*;
/// use ta_lib_in_rust::util::dataframe_utils::ensure_f64_column;
///
/// let mut df = DataFrame::new(vec![
///     Series::new("price", &[1, 2, 3]),
/// ]).unwrap();
/// ensure_f64_column(&mut df, "price").unwrap();
/// assert_eq!(df.column("price").unwrap().dtype(), &DataType::Float64);
/// ```
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
///
/// # Example
///
/// ```
/// use polars::prelude::*;
/// use ta_lib_in_rust::util::dataframe_utils::check_window_size;
///
/// let df = DataFrame::new(vec![Series::new("close", &[1.0, 2.0, 3.0, 4.0])]).unwrap();
/// assert!(check_window_size(&df, 3, "test").is_ok());
/// assert!(check_window_size(&df, 5, "test").is_err());
/// ```
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
