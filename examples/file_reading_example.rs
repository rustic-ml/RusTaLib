use polars::prelude::*;
use rusttalib::util::file_utils::{read_csv, read_csv_default, read_financial_data, read_parquet};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("======= Reading Financial Data Files Example =======\n");

    // File paths
    let csv_with_headers = "examples/csv/AAPL_daily_ohlcv.csv";
    let parquet_with_headers = "examples/csv/AAPL_daily_ohlcv.parquet";

    // ===== EXAMPLE 1: Basic CSV Reading =====
    println!("EXAMPLE 1: Basic CSV Reading");
    println!("-------------------------------");

    // Read CSV with headers (explicitly specifying parameters)
    let df1 = read_csv(csv_with_headers, true, ',')?;
    println!("CSV with headers - Shape: {:?}", df1.shape());
    println!("Column names: {:?}", df1.get_column_names());

    // Print first 5 rows (simplified to avoid lazy operations)
    println!("\nFirst few rows summary:");
    let height = df1.height();
    println!("DataFrame has {} rows and {} columns", height, df1.width());

    // Read CSV with default parameters (has headers, comma delimiter)
    let df2 = read_csv_default(csv_with_headers)?;
    println!("\nCSV with default parameters - Shape: {:?}", df2.shape());

    // ===== EXAMPLE 2: Basic Parquet Reading =====
    println!("\nEXAMPLE 2: Basic Parquet Reading");
    println!("--------------------------------");

    // Read Parquet file
    let df3 = read_parquet(parquet_with_headers)?;
    println!("Parquet file - Shape: {:?}", df3.shape());
    println!("Column names: {:?}", df3.get_column_names());

    // Print simple summary
    println!("\nBasic summary:");
    println!(
        "DataFrame has {} rows and {} columns",
        df3.height(),
        df3.width()
    );

    // ===== EXAMPLE 3: Reading Financial Data with Column Mapping =====
    println!("\nEXAMPLE 3: Financial Data Reading with Column Mapping");
    println!("---------------------------------------------------");

    // Read CSV file with financial data column mapping
    let (df4, columns) = read_financial_data(csv_with_headers, true, "csv", ',')?;

    println!("Financial data from CSV - Shape: {:?}", df4.shape());
    println!("\nIdentified financial columns:");
    println!("Date column: {:?}", columns.date);
    println!("Open column: {:?}", columns.open);
    println!("High column: {:?}", columns.high);
    println!("Low column: {:?}", columns.low);
    println!("Close column: {:?}", columns.close);
    println!("Volume column: {:?}", columns.volume);

    // ===== EXAMPLE 4: Reading Parquet with Financial Column Mapping =====
    println!("\nEXAMPLE 4: Parquet Financial Data Reading");
    println!("---------------------------------------");

    // Read Parquet file with financial data column mapping
    let (df5, columns) = read_financial_data(parquet_with_headers, true, "parquet", ',')?;

    println!("Financial data from Parquet - Shape: {:?}", df5.shape());
    println!("\nIdentified financial columns:");
    println!("Date column: {:?}", columns.date);
    println!("Open column: {:?}", columns.open);
    println!("High column: {:?}", columns.high);
    println!("Low column: {:?}", columns.low);
    println!("Close column: {:?}", columns.close);
    println!("Volume column: {:?}", columns.volume);

    // ===== EXAMPLE 5: Simple Data Access =====
    println!("\nEXAMPLE 5: Simple Data Access");
    println!("--------------------------------");

    if let (Some(close), Some(date)) = (&columns.close, &columns.date) {
        // Extract first and last row's close prices and dates
        if df5.height() > 0 {
            let close_series = df5.column(close)?;
            let date_series = df5.column(date)?;

            println!("First date: {}", date_series.get(0).unwrap());
            println!("First close price: {}", close_series.get(0).unwrap());

            let last_idx = df5.height() - 1;
            println!("Last date: {}", date_series.get(last_idx).unwrap());
            println!("Last close price: {}", close_series.get(last_idx).unwrap());

            // Calculate basic statistics manually without using lazy operations
            let close_f64 = close_series.f64()?;
            let mut sum = 0.0;
            let mut count = 0;
            let mut min = f64::MAX;
            let mut max = f64::MIN;

            for i in 0..close_f64.len() {
                if let Some(val) = close_f64.get(i) {
                    sum += val;
                    count += 1;
                    min = min.min(val);
                    max = max.max(val);
                }
            }

            println!("\nBasic statistics for close prices:");
            println!("Count: {}", count);
            println!("Min: {:.2}", min);
            println!("Max: {:.2}", max);
            println!("Mean: {:.2}", sum / count as f64);
        }
    }

    // ===== EXAMPLE 6: Reading Without Headers =====
    println!("\nEXAMPLE 6: Reading Files Without Headers");
    println!("--------------------------------------");
    println!("(Simulating a file without headers by setting has_header=false)");

    // Read CSV file pretending it has no headers
    let (df6, columns) = read_financial_data(csv_with_headers, false, "csv", ',')?;

    println!(
        "Financial data from CSV (no headers) - Shape: {:?}",
        df6.shape()
    );
    println!("\nAutomatically mapped columns:");
    println!("Date column: {:?}", columns.date);
    println!("Open column: {:?}", columns.open);
    println!("High column: {:?}", columns.high);
    println!("Low column: {:?}", columns.low);
    println!("Close column: {:?}", columns.close);
    println!("Volume column: {:?}", columns.volume);

    println!("\nDone! Successfully demonstrated reading CSV and Parquet files.");

    Ok(())
}
