use polars::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

/// Structure to hold standardized financial data column names
#[derive(Debug, Clone)]
pub struct FinancialColumns {
    pub date: Option<String>,
    pub open: Option<String>,
    pub high: Option<String>,
    pub low: Option<String>,
    pub close: Option<String>,
    pub volume: Option<String>,
}

/// Read a CSV file into a DataFrame
///
/// # Arguments
///
/// * `file_path` - Path to the CSV file
/// * `has_header` - Whether the CSV file has a header row
/// * `delimiter` - The delimiter character (default: ',')
///
/// # Returns
///
/// Returns a PolarsResult<DataFrame> containing the data if successful
///
/// # Example
///
/// ```
/// use ta_lib_in_rust::util::file_utils::read_csv;
///
/// let df = read_csv("data/prices.csv", true, ',').unwrap();
/// println!("{:?}", df);
/// ```
pub fn read_csv<P: AsRef<Path>>(
    file_path: P,
    has_header: bool,
    delimiter: char,
) -> PolarsResult<DataFrame> {
    let file = File::open(file_path)?;

    // Create CSV reader with options
    let csv_options = CsvReadOptions::default()
        .with_has_header(has_header)
        .map_parse_options(|opts| opts.with_separator(delimiter as u8));

    CsvReader::new(file).with_options(csv_options).finish()
}

/// Read a CSV file into a DataFrame with default settings (has header and comma delimiter)
///
/// # Arguments
///
/// * `file_path` - Path to the CSV file
///
/// # Returns
///
/// Returns a PolarsResult<DataFrame> containing the data if successful
pub fn read_csv_default<P: AsRef<Path>>(file_path: P) -> PolarsResult<DataFrame> {
    read_csv(file_path, true, ',')
}

/// Read a Parquet file into a DataFrame
///
/// # Arguments
///
/// * `file_path` - Path to the Parquet file
///
/// # Returns
///
/// Returns a PolarsResult<DataFrame> containing the data if successful
///
/// # Example
///
/// ```
/// use ta_lib_in_rust::util::file_utils::read_parquet;
///
/// let df = read_parquet("data/prices.parquet").unwrap();
/// println!("{:?}", df);
/// ```
pub fn read_parquet<P: AsRef<Path>>(file_path: P) -> PolarsResult<DataFrame> {
    let file = File::open(file_path)?;
    ParquetReader::new(file).finish()
}

/// Read a financial data file (CSV or Parquet) and standardize column names
///
/// This function attempts to identify and standardize OHLCV columns whether or not
/// the file has headers.
///
/// # Arguments
///
/// * `file_path` - Path to the file
/// * `has_header` - Whether the file has headers
/// * `file_type` - "csv" or "parquet"
/// * `delimiter` - Delimiter for CSV files (default: ',')
///
/// # Returns
///
/// A tuple with (DataFrame, FinancialColumns) where FinancialColumns contains the
/// standardized column name mapping
///
/// # Example
///
/// ```
/// use ta_lib_in_rust::util::file_utils::read_financial_data;
///
/// let (df, columns) = read_financial_data("data/prices.csv", true, "csv", ',').unwrap();
/// println!("Close column: {:?}", columns.close);
/// ```
pub fn read_financial_data<P: AsRef<Path>>(
    file_path: P,
    has_header: bool,
    file_type: &str,
    delimiter: char,
) -> PolarsResult<(DataFrame, FinancialColumns)> {
    // Read the data file
    let mut df = match file_type.to_lowercase().as_str() {
        "csv" => read_csv(file_path, has_header, delimiter)?,
        "parquet" => read_parquet(file_path)?,
        _ => return Err(PolarsError::ComputeError("Unsupported file type".into())),
    };

    // Map the columns
    let columns = if has_header {
        map_columns_with_headers(&df)?
    } else {
        // For files without headers, generate column names and then map them
        rename_columns_without_headers(&mut df)?
    };

    Ok((df, columns))
}

/// Maps column names for files with headers
fn map_columns_with_headers(df: &DataFrame) -> PolarsResult<FinancialColumns> {
    let column_names: Vec<String> = df
        .get_column_names()
        .into_iter()
        .map(|s| s.to_string())
        .collect();

    // Create mappings of common financial column names
    let mut financial_columns = FinancialColumns {
        date: None,
        open: None,
        high: None,
        low: None,
        close: None,
        volume: None,
    };

    // Common variations of column names
    let date_variations = ["date", "time", "datetime", "timestamp"];
    let open_variations = ["open", "o", "opening"];
    let high_variations = ["high", "h", "highest"];
    let low_variations = ["low", "l", "lowest"];
    let close_variations = ["close", "c", "closing"];
    let volume_variations = ["volume", "vol", "v"];

    for col in column_names {
        let lower_col = col.to_lowercase();

        if financial_columns.date.is_none()
            && date_variations.iter().any(|&v| lower_col.contains(v))
        {
            financial_columns.date = Some(col.clone());
        } else if financial_columns.open.is_none()
            && open_variations.iter().any(|&v| lower_col.contains(v))
        {
            financial_columns.open = Some(col.clone());
        } else if financial_columns.high.is_none()
            && high_variations.iter().any(|&v| lower_col.contains(v))
        {
            financial_columns.high = Some(col.clone());
        } else if financial_columns.low.is_none()
            && low_variations.iter().any(|&v| lower_col.contains(v))
        {
            financial_columns.low = Some(col.clone());
        } else if financial_columns.close.is_none()
            && close_variations.iter().any(|&v| lower_col.contains(v))
        {
            financial_columns.close = Some(col.clone());
        } else if financial_columns.volume.is_none()
            && volume_variations.iter().any(|&v| lower_col.contains(v))
        {
            financial_columns.volume = Some(col.clone());
        }
    }

    Ok(financial_columns)
}

/// For files without headers, rename columns and identify OHLCV columns
fn rename_columns_without_headers(df: &mut DataFrame) -> PolarsResult<FinancialColumns> {
    let n_cols = df.width();
    let mut col_names = Vec::with_capacity(n_cols);

    // Basic column renaming
    for i in 0..n_cols {
        col_names.push(format!("col_{}", i));
    }

    // Rename the columns from col_0, col_1, etc.
    df.set_column_names(&col_names)?;

    // Try to identify financial columns through data patterns
    let mut financial_columns = FinancialColumns {
        date: None,
        open: None,
        high: None,
        low: None,
        close: None,
        volume: None,
    };

    // If 5+ columns, assume typical OHLCV structure (date, open, high, low, close, volume)
    if n_cols >= 5 {
        // Check each column to see if it might contain date information
        for (i, name) in col_names.iter().enumerate() {
            if let Ok(series) = df.column(name) {
                if i == 0
                    && (series.dtype() == &DataType::String
                        || series.dtype() == &DataType::Date
                        || matches!(series.dtype(), DataType::Datetime(_, _)))
                {
                    financial_columns.date = Some(name.clone());
                    continue;
                }

                // Skip if we've identified this as a date column
                if Some(name.clone()) == financial_columns.date {
                    continue;
                }

                // Now analyze numerical columns
                if series.dtype().is_primitive_numeric() {
                    // Get basic stats for this column
                    if let Some(stats) = series.clone().cast(&DataType::Float64)?.f64()?.mean() {
                        // Volume is typically much larger than price and often close to integers
                        if financial_columns.volume.is_none()
                            && (stats > 1000.0
                                || series.dtype() == &DataType::Int64
                                || series.dtype() == &DataType::UInt64)
                        {
                            financial_columns.volume = Some(name.clone());
                            continue;
                        }
                    }
                }
            }
        }

        // Now identify remaining columns if we have at least 4 price columns
        let price_cols: Vec<String> = col_names
            .iter()
            .filter(|&name| {
                Some(name.clone()) != financial_columns.date
                    && Some(name.clone()) != financial_columns.volume
            })
            .cloned()
            .collect();

        // Simple heuristic mapping
        if price_cols.len() >= 4 {
            financial_columns.open = Some(price_cols[0].clone());
            financial_columns.high = Some(price_cols[1].clone());
            financial_columns.low = Some(price_cols[2].clone());
            financial_columns.close = Some(price_cols[3].clone());
        }

        // For typical 6-column format (date, open, high, low, close, volume)
        if n_cols == 6 && financial_columns.date.is_some() && financial_columns.volume.is_none() {
            // Last column is likely volume if not identified
            financial_columns.volume = Some(col_names[5].clone());
        }

        // If we still haven't identified the price columns, try to use statistics
        if financial_columns.high.is_none() || financial_columns.low.is_none() {
            identify_price_columns_by_statistics(df, &mut financial_columns, &price_cols)?;
        }
    }

    Ok(financial_columns)
}

/// Use statistical properties to identify high, low, open, close columns
fn identify_price_columns_by_statistics(
    df: &DataFrame,
    financial_columns: &mut FinancialColumns,
    price_cols: &[String],
) -> PolarsResult<()> {
    // Track min and max values by column
    let mut col_stats: HashMap<String, (f64, f64)> = HashMap::new(); // (min, max)

    for col_name in price_cols {
        if let Ok(series) = df.column(col_name) {
            if series.dtype().is_primitive_numeric() {
                let f64_series = series.clone().cast(&DataType::Float64)?;

                // Use proper Series methods with f64() to get ChunkedArray<Float64Type>
                if let Ok(f64_chunked) = f64_series.f64() {
                    let min_val = f64_chunked.min();
                    let max_val = f64_chunked.max();

                    if let (Some(min), Some(max)) = (min_val, max_val) {
                        col_stats.insert(col_name.clone(), (min, max));
                    }
                }
            }
        }
    }

    // Find column with highest max values (likely high)
    let mut high_col = None;
    let mut high_val = f64::MIN;
    for (col, (_, max)) in &col_stats {
        if *max > high_val {
            high_val = *max;
            high_col = Some(col.clone());
        }
    }

    // Find column with lowest min values (likely low)
    let mut low_col = None;
    let mut low_val = f64::MAX;
    for (col, (min, _)) in &col_stats {
        if *min < low_val {
            low_val = *min;
            low_col = Some(col.clone());
        }
    }

    // Assign remaining columns to open and close if they haven't been assigned
    if price_cols.len() >= 4 {
        let remaining_cols: Vec<String> = price_cols
            .iter()
            .filter(|&col| Some(col.clone()) != high_col && Some(col.clone()) != low_col)
            .cloned()
            .collect();

        if remaining_cols.len() >= 2 {
            if financial_columns.open.is_none() {
                financial_columns.open = Some(remaining_cols[0].clone());
            }
            if financial_columns.close.is_none() {
                financial_columns.close = Some(remaining_cols[1].clone());
            }
        }
    }

    // Set high and low if they were identified and not already set
    if financial_columns.high.is_none() && high_col.is_some() {
        financial_columns.high = high_col;
    }
    if financial_columns.low.is_none() && low_col.is_some() {
        financial_columns.low = low_col;
    }

    Ok(())
}
