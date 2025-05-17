use polars::prelude::*;
use std::fs::File;
use std::io::BufRead;
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
/// This function automatically detects and handles various aspects of financial data files:
/// - File type (CSV or Parquet) is detected from the file extension
/// - For CSV files:
///   - Automatically detects if the file has headers by checking for common financial column names
///   - Tries multiple common delimiters (comma, semicolon, tab, pipe) until successful
/// - For Parquet files:
///   - Directly reads the file as Parquet format is self-describing
///
/// The function attempts to identify and standardize OHLCV (Open, High, Low, Close, Volume) columns
/// whether or not the file has headers. It handles various common column name variations and
/// automatically maps them to standardized names.
///
/// # Column Name Handling
///
/// - **Case-insensitive**: Column names are handled in a case-insensitive manner (DATE, Date, date all match)
/// - **Abbreviations**: Common abbreviations are supported:
///   - Date: "date", "dt", "time", "datetime", "timestamp"
///   - Open: "open", "o", "opening"
///   - High: "high", "h", "highest"
///   - Low: "low", "l", "lowest"
///   - Close: "close", "c", "closing"
///   - Volume: "volume", "vol", "v"
///
/// # Arguments
///
/// * `file_path` - Path to the file (must have .csv or .parquet extension)
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
/// // Read a CSV file - automatically detects headers and delimiter
/// let (df, columns) = read_financial_data("data/prices.csv").unwrap();
/// println!("Close column: {:?}", columns.close);
///
/// // Read a Parquet file
/// let (df, columns) = read_financial_data("data/prices.parquet").unwrap();
/// println!("Volume column: {:?}", columns.volume);
/// ```
///
/// # Supported File Types
///
/// - CSV files (`.csv` extension)
///   - Automatically detects headers
///   - Supports multiple delimiters: comma (,), semicolon (;), tab (\t), pipe (|)
/// - Parquet files (`.parquet` extension)
///
/// # Error Handling
///
/// The function will return an error if:
/// - The file extension is not supported
/// - The file cannot be read
/// - No valid delimiter is found for CSV files
/// - The file format is invalid
pub fn read_financial_data<P: AsRef<Path>>(
    file_path: P,
) -> PolarsResult<(DataFrame, FinancialColumns)> {
    let path = file_path.as_ref();

    // Detect file type from extension
    let file_type = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_lowercase())
        .ok_or_else(|| {
            PolarsError::ComputeError("Could not determine file type from extension".into())
        })?;

    // Read the data file
    let df = match file_type.as_str() {
        "csv" => {
            // Try to detect if file has headers by reading first line
            let file = File::open(path)?;
            let mut reader = std::io::BufReader::new(file);
            let mut first_line = String::new();
            reader.read_line(&mut first_line)?;

            // Check if first line looks like headers (contains common column names)
            let has_header = ["date", "time", "open", "high", "low", "close", "volume"]
                .iter()
                .any(|&name| first_line.to_lowercase().contains(name));

            // Try different delimiters
            let delimiters = [',', ';', '\t', '|'];
            let mut last_error = None;

            for &delimiter in &delimiters {
                match read_csv(path, has_header, delimiter) {
                    Ok(df) => return process_dataframe(df, has_header),
                    Err(e) => last_error = Some(e),
                }
            }

            // If all delimiters failed, return the last error
            Err(last_error.unwrap_or_else(|| {
                PolarsError::ComputeError("Failed to read CSV with any common delimiter".into())
            }))?
        }
        "parquet" => read_parquet(path)?,
        _ => {
            return Err(PolarsError::ComputeError(
                format!("Unsupported file type: {}", file_type).into(),
            ))
        }
    };

    // Map the columns
    let columns = map_columns_with_headers(&df)?;
    Ok((df, columns))
}

/// Helper function to process the DataFrame and map columns
fn process_dataframe(
    mut df: DataFrame,
    has_header: bool,
) -> PolarsResult<(DataFrame, FinancialColumns)> {
    let columns = if has_header {
        map_columns_with_headers(&df)?
    } else {
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
    let date_variations = ["date", "time", "datetime", "timestamp", "dt"];
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
    let mut col_names = vec![String::new(); n_cols];
    let mut identified_cols = vec![false; n_cols];

    // Initialize financial columns structure
    let mut financial_columns = FinancialColumns {
        date: None,
        open: None,
        high: None,
        low: None,
        close: None,
        volume: None,
    };

    // First pass: identify date column (usually first column)
    for i in 0..n_cols {
        if let Some(series) = df.select_at_idx(i) {
            if !identified_cols[i]
                && (series.dtype() == &DataType::String
                    || series.dtype() == &DataType::Date
                    || matches!(series.dtype(), DataType::Datetime(_, _)))
            {
                col_names[i] = "date".to_string();
                financial_columns.date = Some("date".to_string());
                identified_cols[i] = true;
                break; // Only identify one date column
            }
        }
    }

    // Second pass: identify volume column
    // Look for integer columns or columns with significantly larger values
    for (i, &identified) in identified_cols.iter().enumerate().take(n_cols) {
        if identified {
            continue;
        }

        if let Some(series) = df.select_at_idx(i) {
            if series.dtype().is_primitive_numeric() {
                if let Ok(f64_series) = series.cast(&DataType::Float64) {
                    if let Ok(nums) = f64_series.f64() {
                        // Check if the column contains mostly large integers
                        let is_volume =
                            if let (Some(mean), Some(std_dev)) = (nums.mean(), nums.std(0)) {
                                // Volume typically has:
                                // 1. Much larger values than prices
                                // 2. Higher variance
                                // 3. Often contains round numbers
                                let other_cols_mean = get_numeric_columns_mean(df, i)?;
                                mean > other_cols_mean * 100.0 && std_dev > mean * 0.1
                            } else {
                                false
                            };

                        if is_volume {
                            col_names[i] = "volume".to_string();
                            financial_columns.volume = Some("volume".to_string());
                            identified_cols[i] = true;
                            break; // Only identify one volume column
                        }
                    }
                }
            }
        }
    }

    // Third pass: identify OHLC columns based on their statistical properties
    let mut price_stats: Vec<(usize, f64, f64, f64)> = Vec::new(); // (index, min, max, std_dev)

    for (i, &identified) in identified_cols.iter().enumerate().take(n_cols) {
        if identified {
            continue;
        }

        if let Some(series) = df.select_at_idx(i) {
            if series.dtype().is_primitive_numeric() {
                if let Ok(f64_series) = series.cast(&DataType::Float64) {
                    if let Ok(nums) = f64_series.f64() {
                        if let (Some(min), Some(max), Some(std)) =
                            (nums.min(), nums.max(), nums.std(0))
                        {
                            price_stats.push((i, min, max, std));
                        }
                    }
                }
            }
        }
    }

    // Sort by max values and standard deviation to identify columns
    price_stats.sort_by(|a, b| {
        let a_range = a.2 - a.1;
        let b_range = b.2 - b.1;
        b_range
            .partial_cmp(&a_range)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal))
    });

    // Assign OHLC names based on statistical properties
    for (idx, stat) in price_stats.iter().enumerate() {
        let i = stat.0;
        if !identified_cols[i] {
            let col_name = match idx {
                0 => {
                    financial_columns.high = Some("high".to_string());
                    "high"
                }
                1 => {
                    financial_columns.low = Some("low".to_string());
                    "low"
                }
                2 => {
                    financial_columns.close = Some("close".to_string());
                    "close"
                }
                3 => {
                    financial_columns.open = Some("open".to_string());
                    "open"
                }
                _ => "unknown",
            };
            col_names[i] = col_name.to_string();
            identified_cols[i] = true;
        }
    }

    // Fill in any remaining unidentified columns
    for (i, name) in col_names.iter_mut().enumerate().take(n_cols) {
        if name.is_empty() {
            *name = format!("unknown_{}", i);
        }
    }

    // Rename the columns
    df.set_column_names(&col_names)?;

    Ok(financial_columns)
}

/// Helper function to calculate mean of numeric columns excluding the specified column
fn get_numeric_columns_mean(df: &DataFrame, exclude_idx: usize) -> PolarsResult<f64> {
    let mut sum = 0.0;
    let mut count = 0;

    for i in 0..df.width() {
        if i == exclude_idx {
            continue;
        }

        if let Some(series) = df.select_at_idx(i) {
            if series.dtype().is_primitive_numeric() {
                if let Ok(f64_series) = series.cast(&DataType::Float64) {
                    if let Ok(nums) = f64_series.f64() {
                        if let Some(mean) = nums.mean() {
                            sum += mean;
                            count += 1;
                        }
                    }
                }
            }
        }
    }

    if count > 0 {
        Ok(sum / count as f64)
    } else {
        Ok(0.0)
    }
}
