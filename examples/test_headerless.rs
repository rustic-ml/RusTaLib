use polars::prelude::*;
use rustalib::util::file_utils::read_financial_data;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("Testing headerless file reading");
    println!("==============================\n");

    // Create a sample DataFrame with financial data but no headers
    let dates = Series::new(
        "col_0".into(),
        &[
            "2024-01-01",
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
        ],
    );

    let opens = Series::new("col_1".into(), &[150.0, 152.0, 151.0, 153.0, 154.0]);
    let highs = Series::new("col_2".into(), &[155.0, 156.0, 154.0, 157.0, 158.0]);
    let lows = Series::new("col_3".into(), &[149.0, 151.0, 150.0, 152.0, 153.0]);
    let closes = Series::new("col_4".into(), &[152.0, 153.0, 152.0, 155.0, 156.0]);
    let volumes = Series::new(
        "col_5".into(),
        &[1500000.0, 1600000.0, 1400000.0, 1700000.0, 1800000.0],
    );

    // Create DataFrame
    let mut df = DataFrame::new(vec![
        dates.into(),
        opens.into(),
        highs.into(),
        lows.into(),
        closes.into(),
        volumes.into(),
    ])?;

    println!("Original DataFrame with generic column names:");
    println!("{}\n", df);

    // Save DataFrame to a temporary CSV file without headers
    let temp_file = "temp_test.csv";
    let mut file = std::fs::File::create(temp_file)?;
    CsvWriter::new(&mut file)
        .include_header(false)
        .finish(&mut df)?;
    drop(file);

    // Process the DataFrame using read_financial_data
    let (processed_df, columns) = read_financial_data(temp_file)?;

    println!("Processed DataFrame with identified columns:");
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
