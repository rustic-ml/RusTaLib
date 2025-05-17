use polars::prelude::*;
use rustalib::util::file_utils::read_financial_data;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("Testing case sensitive headers reading");
    println!("=====================================\n");

    // Create a sample DataFrame with financial data with differently cased headers
    let df = DataFrame::new(vec![
        Series::new("DATE".into(), &["2024-01-01", "2024-01-02", "2024-01-03"]).into(),
        Series::new("Open".into(), &[150.0, 152.0, 151.0]).into(),
        Series::new("HIGH".into(), &[155.0, 156.0, 154.0]).into(),
        Series::new("low".into(), &[149.0, 151.0, 150.0]).into(),
        Series::new("Close".into(), &[152.0, 153.0, 152.0]).into(),
        Series::new("VOLUME".into(), &[1500000.0, 1600000.0, 1400000.0]).into(),
    ])?;

    println!("Original DataFrame with case-varied headers:");
    println!("{}\n", df);

    // Save DataFrame to a temporary CSV file
    let temp_file = "temp_case_test.csv";
    let mut file = std::fs::File::create(temp_file)?;
    CsvWriter::new(&mut file).finish(&mut df.clone())?;
    drop(file);

    // Process the DataFrame using read_financial_data
    let (processed_df, columns) = read_financial_data(temp_file)?;

    println!("Processed DataFrame with standardized columns:");
    println!("{}\n", processed_df);

    println!("Identified column mappings:");
    println!("Date column: {:?}", columns.date);
    println!("Open column: {:?}", columns.open);
    println!("High column: {:?}", columns.high);
    println!("Low column: {:?}", columns.low);
    println!("Close column: {:?}", columns.close);
    println!("Volume column: {:?}", columns.volume);

    // Clean up temporary file
    std::fs::remove_file(temp_file)?;

    Ok(())
}
