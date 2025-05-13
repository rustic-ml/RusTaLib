// Simple DataFrame example
use polars::prelude::*;

fn main() -> Result<(), PolarsError> {
    // Create simple data
    let dates = Series::new("date".into(), &["2023-01-01", "2023-01-02", "2023-01-03"]);
    let values = Series::new("value".into(), &[10.0, 11.0, 12.0]);

    // Create DataFrame
    let df = DataFrame::new(vec![dates.clone().into(), values.clone().into()])?;

    // Print information
    println!("DataFrame created with {} rows and {} columns", df.height(), df.width());
    println!("{}", df);

    Ok(())
} 