use polars::prelude::*;

/// Calculates the Stochastic Oscillator, which consists of %K and %D lines
///
/// The Stochastic Oscillator is a momentum indicator that compares a security's closing price
/// to its price range over a given time period. It's particularly useful for intraday trading
/// to identify overbought and oversold conditions.
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLCV data with "high", "low", and "close" columns
/// * `k_period` - Lookback period for %K calculation (typically 14)
/// * `d_period` - Smoothing period for %D calculation (typically 3)
/// * `slowing` - Slowing period (typically 3)
///
/// # Returns
///
/// * `PolarsResult<(Series, Series)>` - Tuple containing %K and %D Series
///
/// # Formula
///
/// %K = 100 * (Close - Lowest Low) / (Highest High - Lowest Low)
/// %D = SMA of %K over d_period
///
/// # Example
///
/// ```
/// use polars::prelude::*;
/// use ta_lib_in_rust::indicators::oscillators::calculate_stochastic;
///
/// // Create or load a DataFrame with OHLCV data
/// let df = DataFrame::default(); // Replace with actual data
///
/// // Calculate Stochastic Oscillator with default parameters
/// let (stoch_k, stoch_d) = calculate_stochastic(&df, 14, 3, 3).unwrap();
/// ```
pub fn calculate_stochastic(
    df: &DataFrame,
    k_period: usize,
    d_period: usize,
    slowing: usize,
) -> PolarsResult<(Series, Series)> {
    // Validate required columns
    if !df.schema().contains("high")
        || !df.schema().contains("low")
        || !df.schema().contains("close")
    {
        return Err(PolarsError::ShapeMismatch(
            "Missing required columns for Stochastic calculation. Required: high, low, close"
                .to_string()
                .into(),
        ));
    }

    // Extract required columns
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;

    // Calculate raw %K values
    let mut raw_k_values = Vec::with_capacity(df.height());

    // Fill initial values with NaN
    for _ in 0..k_period - 1 {
        raw_k_values.push(f64::NAN);
    }

    // Calculate raw %K for each data point
    for i in k_period - 1..df.height() {
        let mut highest_high = f64::NEG_INFINITY;
        let mut lowest_low = f64::INFINITY;
        let mut valid_data = true;

        // Find highest high and lowest low in the period
        for j in i - (k_period - 1)..=i {
            let h = high.get(j).unwrap_or(f64::NAN);
            let l = low.get(j).unwrap_or(f64::NAN);

            if h.is_nan() || l.is_nan() {
                valid_data = false;
                break;
            }

            highest_high = highest_high.max(h);
            lowest_low = lowest_low.min(l);
        }

        if !valid_data || (highest_high - lowest_low).abs() < 1e-10 {
            raw_k_values.push(f64::NAN);
        } else {
            let c = close.get(i).unwrap_or(f64::NAN);
            if c.is_nan() {
                raw_k_values.push(f64::NAN);
            } else {
                let raw_k = 100.0 * (c - lowest_low) / (highest_high - lowest_low);
                raw_k_values.push(raw_k);
            }
        }
    }

    // Apply slowing to %K (if slowing > 1)
    let mut k_values = Vec::with_capacity(df.height());

    // Fill initial values with NaN - ensure we have NaN for all values before k_period + slowing - 1
    let k_offset = k_period + slowing - 1;
    for _ in 0..k_offset {
        k_values.push(f64::NAN);
    }

    // Calculate slowed %K
    for i in k_offset..df.height() {
        let mut sum = 0.0;
        let mut count = 0;
        let mut has_nan = false;

        for j in 0..slowing {
            let val = raw_k_values[i - j];
            if val.is_nan() {
                has_nan = true;
                break;
            }
            sum += val;
            count += 1;
        }

        if has_nan || count == 0 {
            k_values.push(f64::NAN);
        } else {
            k_values.push(sum / count as f64);
        }
    }

    // Calculate %D (SMA of %K)
    let mut d_values = Vec::with_capacity(df.height());

    // Fill initial values with NaN
    let d_offset = k_offset + d_period - 1;
    for _ in 0..d_offset {
        d_values.push(f64::NAN);
    }

    // Calculate %D
    for i in d_offset..df.height() {
        let mut sum = 0.0;
        let mut count = 0;
        let mut has_nan = false;

        for j in 0..d_period {
            let val = k_values[i - j];
            if val.is_nan() {
                has_nan = true;
                break;
            }
            sum += val;
            count += 1;
        }

        if has_nan || count == 0 {
            d_values.push(f64::NAN);
        } else {
            d_values.push(sum / count as f64);
        }
    }

    // Create Series with names that reflect parameters
    let k_name = format!("stoch_k_{}_{}_{}", k_period, slowing, d_period);
    let d_name = format!("stoch_d_{}_{}_{}", k_period, slowing, d_period);

    Ok((
        Series::new(k_name.into(), k_values),
        Series::new(d_name.into(), d_values),
    ))
}
