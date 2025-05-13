// Example: Debug Array Lengths
// This program will print all array lengths to identify the mismatch

use polars::prelude::*;

fn main() -> Result<(), PolarsError> {
    println!("Debugging Array Lengths");
    println!("=====================\n");

    let dates_arr = [
        "2023-01-01",
        "2023-01-02",
        "2023-01-03",
        "2023-01-04",
        "2023-01-05",
        "2023-01-06",
        "2023-01-07",
        "2023-01-08",
        "2023-01-09",
        "2023-01-10",
        "2023-01-11",
        "2023-01-12",
        "2023-01-13",
        "2023-01-14",
        "2023-01-15",
        "2023-01-16",
        "2023-01-17",
        "2023-01-18",
        "2023-01-19",
        "2023-01-20",
        "2023-01-21",
        "2023-01-22",
        "2023-01-23",
        "2023-01-24",
        "2023-01-25",
        "2023-01-26",
        "2023-01-27",
        "2023-01-28",
        "2023-01-29",
        "2023-01-30",
        "2023-01-31",
        "2023-02-01",
        "2023-02-02",
        "2023-02-03",
        "2023-02-04",
        "2023-02-05",
        "2023-02-06",
        "2023-02-07",
        "2023-02-08",
        "2023-02-09",
        "2023-02-10",
        "2023-02-11",
        "2023-02-12",
        "2023-02-13",
        "2023-02-14",
        "2023-02-15",
        "2023-02-16",
        "2023-02-17",
        "2023-02-18",
        "2023-02-19",
        "2023-02-20",
        "2023-02-21",
        "2023-02-22",
        "2023-02-23",
        "2023-02-24",
        "2023-02-25",
        "2023-02-26",
        "2023-02-27",
        "2023-02-28",
        "2023-03-01",
        "2023-03-02",
        "2023-03-03",
        "2023-03-04",
        "2023-03-05",
        "2023-03-06",
        "2023-03-07",
        "2023-03-08",
        "2023-03-09",
        "2023-03-10",
    ];

    let closes_arr = [
        101.0, 102.0, 103.0, 102.5, 101.8, 102.3, 103.1, 104.2, 103.8, 103.5, 104.1, 104.8, 105.2,
        105.8, 106.3, 107.0, 106.5, 107.2, 107.8, 108.3, 109.0, 109.5, 110.2, 110.8, 111.5, 112.0,
        112.8, 113.5, 114.0, 114.5, 115.0, 115.5, 116.0, 116.5, 117.0, 117.5, 118.0, 119.0, 121.0,
        123.0, 126.0, 129.0, 132.0, 131.0, 129.0, 127.0, 125.0, 123.0, 122.0, 121.0, 120.0, 118.5,
        117.0, 116.0, 115.5, 115.0, 114.5, 114.0, 113.5, 113.0, 112.5, 112.0, 111.5, 111.0, 110.5,
        110.0, 109.5, 109.0, 108.5,
    ];

    println!("Array lengths:");
    println!("  dates_arr: {}", dates_arr.len());
    println!("  closes_arr: {}", closes_arr.len());

    // Count elements explicitly
    let mut dates_count = 0;
    for _ in dates_arr.iter() {
        dates_count += 1;
    }

    let mut closes_count = 0;
    for _ in closes_arr.iter() {
        closes_count += 1;
    }

    println!("\nManual counting:");
    println!("  dates_count: {}", dates_count);
    println!("  closes_count: {}", closes_count);

    // List all elements with indices
    println!("\nDates array elements:");
    for (i, date) in dates_arr.iter().enumerate() {
        println!("  [{}]: {}", i, date);
    }

    println!("\nCloses array elements:");
    for (i, close) in closes_arr.iter().enumerate() {
        println!("  [{}]: {}", i, close);
    }

    // Create Series and check lengths
    let dates_series = Series::new("date".into(), &dates_arr);
    let closes_series = Series::new("close".into(), &closes_arr);

    println!("\nSeries lengths:");
    println!("  dates_series: {}", dates_series.len());
    println!("  closes_series: {}", closes_series.len());

    // Create DataFrame
    let mut df = DataFrame::new(vec![
        dates_series.clone().into(),
        closes_series.clone().into(),
    ])?;

    println!("\nDataFrame height: {}", df.height());

    // Create a test vector with exactly df.height() elements
    let mut test_vec = Vec::with_capacity(df.height() as usize);
    for i in 0..df.height() {
        test_vec.push(i as f64);
    }

    println!("Test vector length: {}", test_vec.len());
    println!("DataFrame height: {}", df.height());

    // Try to add the test vector to the DataFrame
    let test_series = Series::new("test".into(), test_vec);
    match df.with_column(test_series) {
        Ok(new_df) => {
            println!("Success! New DataFrame height: {}", new_df.height());
        }
        Err(e) => {
            println!("Error: {}", e);
        }
    }

    Ok(())
}
