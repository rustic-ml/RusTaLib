use polars::prelude::*;

/// Vector arithmetic addition
///
/// # Arguments
///
/// * `df` - DataFrame containing the data
/// * `col1` - First column name
/// * `col2` - Second column name
///
/// # Returns
///
/// Returns a PolarsResult containing the addition Series
pub fn calculate_add(df: &DataFrame, col1: &str, col2: &str) -> PolarsResult<Series> {
    if !df.schema().contains(col1) || !df.schema().contains(col2) {
        return Err(PolarsError::ComputeError(
            format!("Addition requires both {col1} and {col2} columns").into(),
        ));
    }

    let series1 = df.column(col1)?.f64()?;
    let series2 = df.column(col2)?.f64()?;

    let result = series1 + series2;

    Ok(result.with_name(format!("{col1}_add_{col2}").into()).into())
}

/// Vector arithmetic subtraction
///
/// # Arguments
///
/// * `df` - DataFrame containing the data
/// * `col1` - First column name (minuend)
/// * `col2` - Second column name (subtrahend)
///
/// # Returns
///
/// Returns a PolarsResult containing the subtraction Series
pub fn calculate_sub(df: &DataFrame, col1: &str, col2: &str) -> PolarsResult<Series> {
    if !df.schema().contains(col1) || !df.schema().contains(col2) {
        return Err(PolarsError::ComputeError(
            format!("Subtraction requires both {col1} and {col2} columns").into(),
        ));
    }

    let series1 = df.column(col1)?.f64()?;
    let series2 = df.column(col2)?.f64()?;

    let result = series1 - series2;

    Ok(result.with_name(format!("{col1}_sub_{col2}").into()).into())
}

/// Vector arithmetic multiplication
///
/// # Arguments
///
/// * `df` - DataFrame containing the data
/// * `col1` - First column name
/// * `col2` - Second column name
///
/// # Returns
///
/// Returns a PolarsResult containing the multiplication Series
pub fn calculate_mult(df: &DataFrame, col1: &str, col2: &str) -> PolarsResult<Series> {
    if !df.schema().contains(col1) || !df.schema().contains(col2) {
        return Err(PolarsError::ComputeError(
            format!("Multiplication requires both {col1} and {col2} columns").into(),
        ));
    }

    let series1 = df.column(col1)?.f64()?;
    let series2 = df.column(col2)?.f64()?;

    let result = series1 * series2;

    Ok(result
        .with_name(format!("{col1}_mult_{col2}").into())
        .into())
}

/// Vector arithmetic division
///
/// # Arguments
///
/// * `df` - DataFrame containing the data
/// * `col1` - First column name (numerator)
/// * `col2` - Second column name (denominator)
///
/// # Returns
///
/// Returns a PolarsResult containing the division Series
pub fn calculate_div(df: &DataFrame, col1: &str, col2: &str) -> PolarsResult<Series> {
    if !df.schema().contains(col1) || !df.schema().contains(col2) {
        return Err(PolarsError::ComputeError(
            format!("Division requires both {col1} and {col2} columns").into(),
        ));
    }

    let series1 = df.column(col1)?.f64()?;
    let series2 = df.column(col2)?.f64()?;

    // Replace zeros with NaN to avoid division by zero
    let mut div_values = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        let num = series1.get(i).unwrap_or(f64::NAN);
        let denom = series2.get(i).unwrap_or(f64::NAN);

        if denom != 0.0 && !denom.is_nan() && !num.is_nan() {
            div_values.push(num / denom);
        } else {
            div_values.push(f64::NAN);
        }
    }

    Ok(Series::new(format!("{col1}_div_{col2}").into(), div_values))
}

/// Find maximum value over a specified window
///
/// # Arguments
///
/// * `df` - DataFrame containing the data
/// * `column` - Column name to calculate on
/// * `window` - Window size for the calculation
///
/// # Returns
///
/// Returns a PolarsResult containing the MAX Series
pub fn calculate_max(df: &DataFrame, column: &str, window: usize) -> PolarsResult<Series> {
    if !df.schema().contains(column) {
        return Err(PolarsError::ComputeError(
            format!("MAX calculation requires {column} column").into(),
        ));
    }

    let series = df.column(column)?.f64()?;

    let mut max_values = Vec::with_capacity(df.height());

    // Fill initial values with NaN
    for _i in 0..window - 1 {
        max_values.push(f64::NAN);
    }

    // Calculate max for each window
    for i in window - 1..df.height() {
        let mut max_val = f64::NEG_INFINITY;
        let mut all_nan = true;

        for j in 0..window {
            let val = series.get(i - j).unwrap_or(f64::NAN);
            if !val.is_nan() {
                max_val = max_val.max(val);
                all_nan = false;
            }
        }

        if all_nan {
            max_values.push(f64::NAN);
        } else {
            max_values.push(max_val);
        }
    }

    Ok(Series::new(
        format!("{column}_max_{window}").into(),
        max_values,
    ))
}

/// Find minimum value over a specified window
///
/// # Arguments
///
/// * `df` - DataFrame containing the data
/// * `column` - Column name to calculate on
/// * `window` - Window size for the calculation
///
/// # Returns
///
/// Returns a PolarsResult containing the MIN Series
pub fn calculate_min(df: &DataFrame, column: &str, window: usize) -> PolarsResult<Series> {
    if !df.schema().contains(column) {
        return Err(PolarsError::ComputeError(
            format!("MIN calculation requires {column} column").into(),
        ));
    }

    let series = df.column(column)?.f64()?;

    let mut min_values = Vec::with_capacity(df.height());

    // Fill initial values with NaN
    for _i in 0..window - 1 {
        min_values.push(f64::NAN);
    }

    // Calculate min for each window
    for i in window - 1..df.height() {
        let mut min_val = f64::INFINITY;
        let mut all_nan = true;

        for j in 0..window {
            let val = series.get(i - j).unwrap_or(f64::NAN);
            if !val.is_nan() {
                min_val = min_val.min(val);
                all_nan = false;
            }
        }

        if all_nan {
            min_values.push(f64::NAN);
        } else {
            min_values.push(min_val);
        }
    }

    Ok(Series::new(
        format!("{column}_min_{window}").into(),
        min_values,
    ))
}

/// Calculate sum over a specified window
///
/// # Arguments
///
/// * `df` - DataFrame containing the data
/// * `column` - Column name to calculate on
/// * `window` - Window size for the calculation
///
/// # Returns
///
/// Returns a PolarsResult containing the SUM Series
pub fn calculate_sum(df: &DataFrame, column: &str, window: usize) -> PolarsResult<Series> {
    if !df.schema().contains(column) {
        return Err(PolarsError::ComputeError(
            format!("SUM calculation requires {column} column").into(),
        ));
    }

    let series = df.column(column)?.f64()?;

    let mut sum_values = Vec::with_capacity(df.height());

    // Fill initial values with NaN
    for _i in 0..window - 1 {
        sum_values.push(f64::NAN);
    }

    // Calculate sum for each window
    for i in window - 1..df.height() {
        let mut sum = 0.0;
        let mut all_nan = true;

        for j in 0..window {
            let val = series.get(i - j).unwrap_or(f64::NAN);
            if !val.is_nan() {
                sum += val;
                all_nan = false;
            }
        }

        if all_nan {
            sum_values.push(f64::NAN);
        } else {
            sum_values.push(sum);
        }
    }

    Ok(Series::new(
        format!("{column}_sum_{window}").into(),
        sum_values,
    ))
}

/// Calculate the rolling sum of a column in a DataFrame
///
/// # Arguments
///
/// * `df` - DataFrame containing the column
/// * `column_name` - Name of the column to sum
/// * `window` - Window size for the rolling sum
///
/// # Returns
///
/// Returns a PolarsResult containing the rolling sum Series
pub fn calculate_rolling_sum(
    df: &DataFrame,
    column_name: &str,
    window: usize,
) -> PolarsResult<Series> {
    // Get the column
    let column = df.column(column_name)?.f64()?;
    let n = column.len();

    // Initialize a new vector for the results
    let mut result = Vec::with_capacity(n);

    // Calculate the first window-1 values which are null
    for _i in 0..window - 1 {
        result.push(f64::NAN);
    }

    // Calculate the remaining values
    for i in window - 1..n {
        let mut sum = 0.0;
        for j in 0..window {
            sum += column.get(i - j).unwrap_or(0.0);
        }
        result.push(sum);
    }

    // Return the result as a Series
    Ok(Series::new(
        format!("{}_sum{}", column_name, window).into(),
        result,
    ))
}

/// Calculate the rolling average of a column in a DataFrame
///
/// # Arguments
///
/// * `df` - DataFrame containing the column
/// * `column_name` - Name of the column to average
/// * `window` - Window size for the rolling average
///
/// # Returns
///
/// Returns a PolarsResult containing the rolling average Series
pub fn calculate_rolling_avg(
    df: &DataFrame,
    column_name: &str,
    window: usize,
) -> PolarsResult<Series> {
    // Get the column
    let column = df.column(column_name)?.f64()?;
    let n = column.len();

    // Initialize a new vector for the results
    let mut result = Vec::with_capacity(n);

    // Calculate the first window-1 values which are null
    for _i in 0..window - 1 {
        result.push(f64::NAN);
    }

    // Calculate the remaining values
    for i in window - 1..n {
        let mut sum = 0.0;
        for j in 0..window {
            sum += column.get(i - j).unwrap_or(0.0);
        }
        result.push(sum / window as f64);
    }

    // Return the result as a Series
    Ok(Series::new(
        format!("{}_avg{}", column_name, window).into(),
        result,
    ))
}

/// Calculate the rolling standard deviation of a column in a DataFrame
///
/// # Arguments
///
/// * `df` - DataFrame containing the column
/// * `column_name` - Name of the column to calculate the standard deviation for
/// * `window` - Window size for the rolling standard deviation
///
/// # Returns
///
/// Returns a PolarsResult containing the rolling standard deviation Series
pub fn calculate_rolling_std(
    df: &DataFrame,
    column_name: &str,
    window: usize,
) -> PolarsResult<Series> {
    // Get the column
    let column = df.column(column_name)?.f64()?;
    let n = column.len();

    // Initialize a new vector for the results
    let mut result = Vec::with_capacity(n);

    // Calculate the first window-1 values which are null
    for _i in 0..window - 1 {
        result.push(f64::NAN);
    }

    // Calculate the remaining values
    for i in window - 1..n {
        let mut sum = 0.0;
        let mut sum_sq = 0.0;

        for j in 0..window {
            let value = column.get(i - j).unwrap_or(0.0);
            sum += value;
            sum_sq += value * value;
        }

        let avg = sum / window as f64;
        let variance = if window > 1 {
            (sum_sq - sum * avg) / (window as f64 - 1.0)
        } else {
            0.0
        };

        if variance < 0.0 {
            // Due to floating point errors, variance can be slightly negative
            // when it should be zero. In this case, just return 0.0.
            result.push(0.0);
        } else {
            result.push(variance.sqrt());
        }
    }

    // Return the result as a Series
    Ok(Series::new(
        format!("{}_std{}", column_name, window).into(),
        result,
    ))
}

/// Calculate the rate of change (ROC) of a column in a DataFrame
///
/// # Arguments
///
/// * `df` - DataFrame containing the column
/// * `column_name` - Name of the column to calculate ROC for
/// * `period` - Period for the ROC calculation
///
/// # Returns
///
/// Returns a PolarsResult containing the ROC Series
pub fn calculate_rate_of_change(
    df: &DataFrame,
    column_name: &str,
    period: usize,
) -> PolarsResult<Series> {
    // Get the column
    let column = df.column(column_name)?.f64()?;
    let n = column.len();

    // Initialize a new vector for the results
    let mut result = Vec::with_capacity(n);

    // Calculate the first period values which are null
    for _ in 0..period {
        result.push(f64::NAN);
    }

    // Calculate the remaining values
    for i in period..n {
        let current_value = column.get(i).unwrap_or(0.0);
        let previous_value = column.get(i - period).unwrap_or(1.0); // Avoid division by zero

        if previous_value == 0.0 {
            result.push(0.0); // Handle division by zero
        } else {
            let roc = ((current_value - previous_value) / previous_value) * 100.0;
            result.push(roc);
        }
    }

    // Return the result as a Series
    Ok(Series::new(
        format!("{}_roc{}", column_name, period).into(),
        result,
    ))
}