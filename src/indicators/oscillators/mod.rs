// Oscillators module

use polars::prelude::*;

// Module declarations
mod macd;
mod rsi;
mod stochastic;
mod williams_r;

// Re-export functions
pub use macd::calculate_macd;
pub use rsi::calculate_rsi;
pub use stochastic::calculate_stochastic;
pub use williams_r::calculate_williams_r;

/// Add oscillator indicators to a DataFrame
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLCV data
///
/// # Returns
///
/// * `PolarsResult<DataFrame>` - DataFrame with added oscillator indicators
///
/// # Example
///
/// ```
/// use polars::prelude::*;
/// use ta_lib_in_rust::indicators::oscillators::add_oscillator_indicators;
///
/// // Create or load a DataFrame with OHLCV data
/// let df = DataFrame::default(); // Replace with actual data
///
/// // Add oscillator indicators
/// let df_with_indicators = add_oscillator_indicators(&df).unwrap();
/// ```
pub fn add_oscillator_indicators(df: &DataFrame) -> PolarsResult<DataFrame> {
    let mut result_df = df.clone();

    // RSI
    let rsi_14 = calculate_rsi(df, 14, "close")?;
    result_df.with_column(rsi_14)?;

    // MACD
    let (macd, macd_signal) = calculate_macd(df, 12, 26, 9, "close")?;
    result_df.with_column(macd)?;
    result_df.with_column(macd_signal)?;

    // Williams %R
    let williams_r_14 = calculate_williams_r(df, 14)?;
    result_df.with_column(williams_r_14)?;

    // Stochastic Oscillator
    let (stoch_k, stoch_d) = calculate_stochastic(df, 14, 3, 3)?;
    result_df.with_column(stoch_k)?;
    result_df.with_column(stoch_d)?;

    Ok(result_df)
}
