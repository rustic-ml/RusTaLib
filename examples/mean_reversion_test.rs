// Simple DataFrame example

use polars::prelude::*;

fn main() -> Result<(), PolarsError> {
    println!("DataFrame Example");
    println!("==================\n");

    // Create simple data
    let dates = Series::new(
        "date".into(),
        &[
            "2023-01-01",
            "2023-01-02",
            "2023-01-03",
            "2023-01-04",
            "2023-01-05",
        ],
    );

    let values = Series::new("value".into(), &[10.0, 11.0, 12.0, 13.0, 14.0]);

    // Create DataFrame
    let mut df = DataFrame::new(vec![dates.clone().into(), values.clone().into()])?;

    // Print information
    println!("Original DataFrame:");
    println!("  Height: {}", df.height());
    println!("  Width: {}", df.width());
    println!("\nDataFrame content:");
    println!("{}", df);

    // Create a new column by doubling the values
    let values_f64 = values.f64()?;
    let mut doubles = Vec::with_capacity(values_f64.len());

    for i in 0..values_f64.len() {
        if let Some(val) = values_f64.get(i) {
            doubles.push(val * 2.0);
        } else {
            doubles.push(f64::NAN);
        }
    }

    let doubles_series = Series::new("doubles".into(), doubles);

    // Add the new column
    df.with_column(doubles_series)?;
    println!("\nDataFrame with new column:");
    println!("{}", df);

    Ok(())
}
