use polars::prelude::*;
use crate::util::dataframe_utils::check_window_size;

/// Calculates Bollinger Bands
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for the SMA (typically 20)
/// * `num_std` - Number of standard deviations (typically 2.0)
/// * `column` - Column name to use for calculations (default "close")
///
/// # Returns
///
/// Returns a PolarsResult containing a tuple of (middle, upper, lower) bands
pub fn calculate_bollinger_bands(
    df: &DataFrame,
    window: usize,
    num_std: f64,
    column: &str,
) -> PolarsResult<(Series, Series, Series)> {
    check_window_size(df, window, "Bollinger Bands")?;
    
    let series = df.column(column)?.f64()?.clone().into_series();
    
    let sma = series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })?;
    
    let std = series.rolling_std(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })?;
    
    let mut upper_band = Vec::with_capacity(series.len());
    let mut lower_band = Vec::with_capacity(series.len());
    
    for i in 0..series.len() {
        let ma = sma.f64()?.get(i).unwrap_or(0.0);
        let std_val = std.f64()?.get(i).unwrap_or(0.0);
        
        upper_band.push(ma + num_std * std_val);
        lower_band.push(ma - num_std * std_val);
    }
    
    Ok((
        sma.with_name("bb_middle".into()),
        Series::new("bb_upper".into(), upper_band),
        Series::new("bb_lower".into(), lower_band)
    ))
}

/// Calculates Bollinger Band %B indicator
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for Bollinger Bands (typically 20)
/// * `num_std` - Number of standard deviations (typically 2.0)
/// * `column` - Column name to use for calculations (default "close")
///
/// # Returns
///
/// Returns a PolarsResult containing the %B Series
pub fn calculate_bb_b(
    df: &DataFrame, 
    window: usize, 
    num_std: f64,
    column: &str
) -> PolarsResult<Series> {
    let (_, bb_upper, bb_lower) = calculate_bollinger_bands(df, window, num_std, column)?;
    
    let close = df.column(column)?.f64()?;
    
    // Calculate %B: (Price - Lower Band) / (Upper Band - Lower Band)
    let bb_b = (close - bb_lower.f64()?) / (bb_upper.f64()? - bb_lower.f64()?);
    
    Ok(bb_b.into_series().with_name("bb_b".into()))
}

/// Calculates Average True Range (ATR)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for ATR (typically 14)
///
/// # Returns
///
/// Returns a PolarsResult containing the ATR Series
pub fn calculate_atr(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    check_window_size(df, window, "ATR")?;
    
    let high = df.column("high")?.f64()?.clone().into_series();
    let low = df.column("low")?.f64()?.clone().into_series();
    let close = df.column("close")?.f64()?.clone().into_series();
    
    let prev_close = close.shift(1);
    let mut tr_values = Vec::with_capacity(df.height());
    
    let first_tr = {
        let h = high.f64()?.get(0).unwrap_or(0.0);
        let l = low.f64()?.get(0).unwrap_or(0.0);
        h - l
    };
    tr_values.push(first_tr);
    
    for i in 1..df.height() {
        let h = high.f64()?.get(i).unwrap_or(0.0);
        let l = low.f64()?.get(i).unwrap_or(0.0);
        let pc = prev_close.f64()?.get(i).unwrap_or(0.0);
        
        let tr = if pc == 0.0 {
            h - l
        } else {
            (h - l).max((h - pc).abs()).max((l - pc).abs())
        };
        tr_values.push(tr);
    }
    
    let tr_series = Series::new("tr".into(), tr_values);
    let atr = tr_series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        center: false,
        weights: None,
        fn_params: None,
    })?;
    
    Ok(atr.with_name("atr".into()))
}

/// Calculates Garman-Klass volatility estimator (uses OHLC data)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for smoothing (typically 10)
///
/// # Returns
///
/// Returns a PolarsResult containing the GK volatility Series
pub fn calculate_gk_volatility(df: &DataFrame, window: usize) -> PolarsResult<Series> {
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let open = df.column("open")?.f64()?;
    let close = df.column("close")?.f64()?;
    
    let mut gk_values = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let h = high.get(i).unwrap_or(0.0);
        let l = low.get(i).unwrap_or(0.0);
        let o = open.get(i).unwrap_or(0.0);
        let c = close.get(i).unwrap_or(0.0);
        
        if h > 0.0 && l > 0.0 && o > 0.0 {
            let hl = (h/l).ln().powi(2) * 0.5;
            let co = (c/o).ln().powi(2);
            gk_values.push(hl - (2.0 * 0.386) * co);
        } else {
            gk_values.push(0.0);
        }
    }
    
    let gk_series = Series::new("gk_raw".into(), gk_values);
    
    // Apply rolling mean to get smoother estimate
    let gk_volatility = gk_series.rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: 1, // Allow calculation with fewer values
        center: false,
        weights: None,
        fn_params: None,
    })?;
    
    Ok(gk_volatility.with_name("gk_volatility".into()))
} 