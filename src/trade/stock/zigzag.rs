use polars::prelude::*;

/// Calculate Zig Zag indicator
///
/// Returns a Series with Zig Zag points (NaN for non-pivot, price for pivot)
pub fn calculate_zigzag(df: &DataFrame, price_col: &str, percent: f64) -> PolarsResult<Series> {
    let price = df.column(price_col)?.f64()?;
    let len = df.height();
    let mut zigzag = vec![f64::NAN; len];
    if len == 0 { return Ok(Series::new("zigzag".into(), zigzag)); }
    let mut last_pivot = 0;
    let mut last_pivot_price = price.get(0).unwrap_or(f64::NAN);
    zigzag[0] = last_pivot_price;
    let mut trend = 0; // 1 = up, -1 = down, 0 = unknown
    for i in 1..len {
        let curr_price = price.get(i).unwrap_or(f64::NAN);
        if trend == 0 {
            if curr_price > last_pivot_price * (1.0 + percent) {
                trend = 1;
                last_pivot = i;
                last_pivot_price = curr_price;
                zigzag[i] = curr_price;
            } else if curr_price < last_pivot_price * (1.0 - percent) {
                trend = -1;
                last_pivot = i;
                last_pivot_price = curr_price;
                zigzag[i] = curr_price;
            }
        } else if trend == 1 {
            if curr_price < last_pivot_price * (1.0 - percent) {
                trend = -1;
                last_pivot = i;
                last_pivot_price = curr_price;
                zigzag[i] = curr_price;
            } else if curr_price > last_pivot_price {
                last_pivot = i;
                last_pivot_price = curr_price;
                zigzag[i] = curr_price;
            }
        } else if trend == -1 {
            if curr_price > last_pivot_price * (1.0 + percent) {
                trend = 1;
                last_pivot = i;
                last_pivot_price = curr_price;
                zigzag[i] = curr_price;
            } else if curr_price < last_pivot_price {
                last_pivot = i;
                last_pivot_price = curr_price;
                zigzag[i] = curr_price;
            }
        }
    }
    Ok(Series::new("zigzag".into(), zigzag))
} 