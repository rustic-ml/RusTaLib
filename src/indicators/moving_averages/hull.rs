use polars::prelude::*;

/// Calculate Hull Moving Average
///
/// HMA = WMA(2*WMA(n/2) - WMA(n)), sqrt(n)
pub fn calculate_hma(df: &DataFrame, column: &str, period: usize) -> PolarsResult<Series> {
    // Calculate half period
    let half_period = period / 2;

    // Calculate WMA with period and half period
    let wma_full = super::wma::calculate_wma(df, column, period)?;
    let wma_half = super::wma::calculate_wma(df, column, half_period)?;

    // Calculate 2 * WMA(half period) - WMA(full period)
    let raw_hma = wma_half
        .multiply(&Series::new("mult".into(), &[2.0f64]))?
        .subtract(&wma_full)?;

    // Create a temporary dataframe with the raw_hma
    let temp_df = DataFrame::new(vec![raw_hma.clone().into()])?;

    // Calculate WMA of the result with period = sqrt(n)
    let sqrt_period = (period as f64).sqrt().round() as usize;
    super::wma::calculate_wma(&temp_df, raw_hma.name(), sqrt_period)
}
