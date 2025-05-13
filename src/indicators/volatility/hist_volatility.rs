use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Historical Volatility (annualized standard deviation of returns)
///
/// Historical volatility is a statistical measure of the dispersion of returns for a given security
/// or market index over a given period of time. It is calculated by taking the standard deviation
/// of log returns and then annualizing the result.
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for volatility calculation (typically 20 days)
/// * `column` - Column to calculate volatility on (usually "close")
/// * `trading_periods` - Number of trading periods in a year (252 for daily data, 
///                      52 for weekly, 12 for monthly, etc.)
///
/// # Returns
///
/// Returns a PolarsResult containing the Historical Volatility Series as a percentage
///
/// # Example
///
/// ```
/// use polars::prelude::*;
/// use ta_lib_in_rust::indicators::volatility::calculate_hist_volatility;
///
/// let close = Series::new("close".into(), &[100.0, 102.0, 104.0, 103.0, 105.0, 107.0]);
/// let df = DataFrame::new(vec![close.into()]).unwrap();
///
/// // Calculate 5-day historical volatility on daily data (252 trading days per year)
/// let hist_vol = calculate_hist_volatility(&df, 5, "close", 252).unwrap();
/// ```
pub fn calculate_hist_volatility(
    df: &DataFrame,
    window: usize,
    column: &str,
    trading_periods: usize,
) -> PolarsResult<Series> {
    // Check window size
    check_window_size(df, window, "Historical Volatility")?;

    // Check if the specified column exists
    if !df.schema().contains(column) {
        return Err(PolarsError::ShapeMismatch(
            format!("DataFrame must contain '{}' column for Historical Volatility calculation", column).into(),
        ));
    }

    // Get the column to calculate returns
    let price = df.column(column)?.f64()?;
    
    // Calculate log returns: ln(price_t / price_t-1)
    let mut returns = Vec::with_capacity(df.height());
    
    returns.push(f64::NAN); // First element has no return
    
    for i in 1..df.height() {
        let current = price.get(i).unwrap_or(f64::NAN);
        let previous = price.get(i - 1).unwrap_or(f64::NAN);
        
        if !current.is_nan() && !previous.is_nan() && previous > 0.0 {
            let log_return = (current / previous).ln();
            returns.push(log_return);
        } else {
            returns.push(f64::NAN);
        }
    }
    
    // Calculate rolling standard deviation of returns
    let mut volatility = Vec::with_capacity(df.height());
    
    // Fill NaN for the first window elements
    for _ in 0..window {
        volatility.push(f64::NAN);
    }
    
    // Calculate rolling standard deviation and annualize
    for i in window..df.height() {
        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        let mut count = 0;
        
        // Calculate variance within the window
        for j in (i - window + 1)..=i {
            let ret = returns[j];
            if !ret.is_nan() {
                sum += ret;
                sum_sq += ret * ret;
                count += 1;
            }
        }
        
        if count > 1 {
            // Calculate variance: E[X²] - (E[X])²
            let mean = sum / count as f64;
            let variance = sum_sq / count as f64 - mean * mean;
            
            // Annualize volatility and convert to percentage
            // Formula: σ_annual = σ_daily * sqrt(trading_periods)
            let annualized_vol = if variance > 0.0 {
                variance.sqrt() * (trading_periods as f64).sqrt() * 100.0
            } else {
                0.0
            };
            
            volatility.push(annualized_vol);
        } else {
            volatility.push(f64::NAN);
        }
    }
    
    Ok(Series::new("hist_volatility".into(), volatility))
} 