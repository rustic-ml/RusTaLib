// Test DataFrame

use polars::prelude::*;

fn main() -> Result<(), PolarsError> {
    // Create simple Series
    let s1 = Series::new("a".into(), &[1, 2, 3]);
    let s2 = Series::new("b".into(), &[4, 5, 6]);

    // Create DataFrame
    let df = DataFrame::new(vec![s1.into(), s2.into()])?;

    // Print the DataFrame
    println!("Test DataFrame");
    println!("{}", df);

    Ok(())
}
