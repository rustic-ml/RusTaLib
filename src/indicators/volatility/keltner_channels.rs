use crate::indicators::volatility::calculate_atr;
use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Keltner Channels
///
/// Keltner Channels are volatility-based bands that surround the price of an asset.
/// They consist of:
/// - Middle band: An Exponential Moving Average (EMA) of the closing price
/// - Upper band: Middle band + (multiplier * Average True Range)
/// - Lower band: Middle band - (multiplier * Average True Range)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data (must include 'high', 'low', 'close' columns)
/// * `window` - Window size for EMA and ATR calculations (typically 20)
/// * `multiplier` - Multiplier for ATR (typically 2.0)
///
/// # Returns
///
/// Returns a PolarsResult containing a DataFrame with:
/// - keltner_upper: Upper Keltner Channel
/// - keltner_middle: Middle Keltner Channel (EMA)
/// - keltner_lower: Lower Keltner Channel
///
/// # Example
///
/// ```
/// use polars::prelude::*;
/// use ta_lib_in_rust::indicators::volatility::calculate_keltner_channels;
///
/// // Create example data
/// let open = Series::new("open".into(), &[10.0, 10.5, 11.0, 11.5, 12.0]);
/// let high = Series::new("high".into(), &[12.0, 13.0, 13.5, 14.0, 14.5]);
/// let low = Series::new("low".into(), &[9.5, 10.5, 11.0, 11.5, 12.0]);
/// let close = Series::new("close".into(), &[11.0, 12.0, 12.5, 13.0, 13.5]);
/// 
/// let df = DataFrame::new(vec![
///     open.into(), high.into(), low.into(), close.into()
/// ]).unwrap();
///
/// let channels = calculate_keltner_channels(&df, 2, 2.0).unwrap();
/// ```
pub fn calculate_keltner_channels(
    df: &DataFrame,
    window: usize,
    multiplier: f64,
) -> PolarsResult<DataFrame> {
    // Check window size
    check_window_size(df, window, "Keltner Channels")?;
    
    // Check required columns
    if !df.schema().contains("high") || !df.schema().contains("low") || !df.schema().contains("close") {
        return Err(PolarsError::ShapeMismatch(
            "DataFrame must contain 'high', 'low', and 'close' columns for Keltner Channels calculation".into(),
        ));
    }

    // Calculate the middle band (EMA of close)
    let close = df.column("close")?.f64()?;
    
    // Calculate EMA
    let mut middle_band = Vec::with_capacity(df.height());
    let smoothing_factor = 2.0 / (window as f64 + 1.0);
    
    // Initialize with SMA for first window elements
    let mut sum = 0.0;
    let mut count = 0;
    for i in 0..window.min(df.height()) {
        let val = close.get(i).unwrap_or(f64::NAN);
        if !val.is_nan() {
            sum += val;
            count += 1;
        }
    }
    
    let first_ema = if count > 0 { sum / count as f64 } else { f64::NAN };
    
    // Fill NaN for the first window-1 elements
    for _ in 0..(window - 1) {
        middle_band.push(f64::NAN);
    }
    
    middle_band.push(first_ema);
    
    // Calculate EMA for the rest of the data
    let mut prev_ema = first_ema;
    for i in window..df.height() {
        let close_val = close.get(i).unwrap_or(f64::NAN);
        
        if !close_val.is_nan() && !prev_ema.is_nan() {
            let ema = close_val * smoothing_factor + prev_ema * (1.0 - smoothing_factor);
            middle_band.push(ema);
            prev_ema = ema;
        } else {
            middle_band.push(f64::NAN);
        }
    }
    
    // Calculate ATR
    let atr = calculate_atr(df, window)?;
    
    // Calculate upper and lower bands
    let mut upper_band = Vec::with_capacity(df.height());
    let mut lower_band = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let mid = middle_band[i];
        let atr_val = atr.f64()?.get(i).unwrap_or(f64::NAN);
        
        if !mid.is_nan() && !atr_val.is_nan() {
            upper_band.push(mid + multiplier * atr_val);
            lower_band.push(mid - multiplier * atr_val);
        } else {
            upper_band.push(f64::NAN);
            lower_band.push(f64::NAN);
        }
    }
    
    // Create the result DataFrame
    let middle_series = Series::new("keltner_middle".into(), middle_band);
    let upper_series = Series::new("keltner_upper".into(), upper_band);
    let lower_series = Series::new("keltner_lower".into(), lower_band);
    
    DataFrame::new(vec![upper_series.into(), middle_series.into(), lower_series.into()])
} 