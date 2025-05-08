//! Options Greeks Analysis
//! 
//! This module provides functions to calculate and analyze options greeks
//! including delta, gamma, theta, vega, and rho.

use polars::prelude::*;
use polars::frame::DataFrame;
use std::f64::consts::PI;

/// Calculate delta for options
///
/// Delta measures the rate of change of the option price with respect to changes
/// in the underlying asset's price.
///
/// # Arguments
/// * `df` - DataFrame with options data
/// * `price_column` - Column name for underlying price
/// * `strike_column` - Column name for strike price
/// * `iv_column` - Column name for implied volatility
/// * `time_column` - Column name for time to expiry (in years)
/// * `is_call_column` - Column name indicating if option is a call (true) or put (false)
///
/// # Returns
/// * `PolarsResult<Series>` - Series with delta values
pub fn calculate_delta(
    df: &DataFrame,
    price_column: &str,
    strike_column: &str,
    iv_column: &str,
    time_column: &str,
    is_call_column: &str,
) -> PolarsResult<Series> {
    // Extract required columns
    let price = df.column(price_column)?.f64()?;
    let strike = df.column(strike_column)?.f64()?;
    let iv = df.column(iv_column)?.f64()?;
    let time = df.column(time_column)?.f64()?;
    let is_call = df.column(is_call_column)?.bool()?;
    
    let len = df.height();
    let mut delta_values = vec![f64::NAN; len];
    
    for i in 0..len {
        let s = price.get(i).unwrap_or(f64::NAN);
        let k = strike.get(i).unwrap_or(f64::NAN);
        let v = iv.get(i).unwrap_or(f64::NAN);
        let t = time.get(i).unwrap_or(f64::NAN);
        let call = is_call.get(i).unwrap_or(false);
        
        if s.is_nan() || k.is_nan() || v.is_nan() || t.is_nan() || t <= 0.0 {
            continue;
        }
        
        // Calculate d1 from Black-Scholes
        let d1 = ((s / k).ln() + (0.5 * v * v) * t) / (v * t.sqrt());
        
        // Calculate delta based on normal CDF of d1
        let delta = if call {
            norm_cdf(d1)
        } else {
            norm_cdf(d1) - 1.0
        };
        
        delta_values[i] = delta;
    }
    
    Ok(Series::new("delta", delta_values))
}

/// Calculate gamma for options
///
/// Gamma measures the rate of change of delta with respect to changes
/// in the underlying asset's price.
///
/// # Arguments
/// * `df` - DataFrame with options data
/// * `price_column` - Column name for underlying price
/// * `strike_column` - Column name for strike price
/// * `iv_column` - Column name for implied volatility
/// * `time_column` - Column name for time to expiry (in years)
///
/// # Returns
/// * `PolarsResult<Series>` - Series with gamma values
pub fn calculate_gamma(
    df: &DataFrame,
    price_column: &str,
    strike_column: &str,
    iv_column: &str,
    time_column: &str,
) -> PolarsResult<Series> {
    // Extract required columns
    let price = df.column(price_column)?.f64()?;
    let strike = df.column(strike_column)?.f64()?;
    let iv = df.column(iv_column)?.f64()?;
    let time = df.column(time_column)?.f64()?;
    
    let len = df.height();
    let mut gamma_values = vec![f64::NAN; len];
    
    for i in 0..len {
        let s = price.get(i).unwrap_or(f64::NAN);
        let k = strike.get(i).unwrap_or(f64::NAN);
        let v = iv.get(i).unwrap_or(f64::NAN);
        let t = time.get(i).unwrap_or(f64::NAN);
        
        if s.is_nan() || k.is_nan() || v.is_nan() || t.is_nan() || t <= 0.0 {
            continue;
        }
        
        // Calculate d1 from Black-Scholes
        let d1 = ((s / k).ln() + (0.5 * v * v) * t) / (v * t.sqrt());
        
        // Calculate gamma (same for calls and puts)
        let gamma = norm_pdf(d1) / (s * v * t.sqrt());
        
        gamma_values[i] = gamma;
    }
    
    Ok(Series::new("gamma", gamma_values))
}

/// Calculate theta for options
///
/// Theta measures the rate of change of the option price with respect to time.
///
/// # Arguments
/// * `df` - DataFrame with options data
/// * `price_column` - Column name for underlying price
/// * `strike_column` - Column name for strike price
/// * `iv_column` - Column name for implied volatility
/// * `time_column` - Column name for time to expiry (in years)
/// * `rate_column` - Column name for risk-free rate
/// * `is_call_column` - Column name indicating if option is a call (true) or put (false)
///
/// # Returns
/// * `PolarsResult<Series>` - Series with theta values (per day)
pub fn calculate_theta(
    df: &DataFrame,
    price_column: &str,
    strike_column: &str,
    iv_column: &str,
    time_column: &str,
    rate_column: &str,
    is_call_column: &str,
) -> PolarsResult<Series> {
    // Extract required columns
    let price = df.column(price_column)?.f64()?;
    let strike = df.column(strike_column)?.f64()?;
    let iv = df.column(iv_column)?.f64()?;
    let time = df.column(time_column)?.f64()?;
    let rate = df.column(rate_column)?.f64()?;
    let is_call = df.column(is_call_column)?.bool()?;
    
    let len = df.height();
    let mut theta_values = vec![f64::NAN; len];
    
    for i in 0..len {
        let s = price.get(i).unwrap_or(f64::NAN);
        let k = strike.get(i).unwrap_or(f64::NAN);
        let v = iv.get(i).unwrap_or(f64::NAN);
        let t = time.get(i).unwrap_or(f64::NAN);
        let r = rate.get(i).unwrap_or(f64::NAN);
        let call = is_call.get(i).unwrap_or(false);
        
        if s.is_nan() || k.is_nan() || v.is_nan() || t.is_nan() || r.is_nan() || t <= 0.0 {
            continue;
        }
        
        // Calculate d1 and d2 from Black-Scholes
        let d1 = ((s / k).ln() + (r + 0.5 * v * v) * t) / (v * t.sqrt());
        let d2 = d1 - v * t.sqrt();
        
        // Calculate theta (per year, then convert to per day)
        let theta = if call {
            -(s * v * norm_pdf(d1)) / (2.0 * t.sqrt()) - r * k * (-r * t).exp() * norm_cdf(d2)
        } else {
            -(s * v * norm_pdf(d1)) / (2.0 * t.sqrt()) + r * k * (-r * t).exp() * norm_cdf(-d2)
        };
        
        // Convert to daily theta (divide by 365)
        theta_values[i] = theta / 365.0;
    }
    
    Ok(Series::new("theta", theta_values))
}

/// Calculate vega for options
///
/// Vega measures the rate of change of the option price with respect to volatility.
///
/// # Arguments
/// * `df` - DataFrame with options data
/// * `price_column` - Column name for underlying price
/// * `strike_column` - Column name for strike price
/// * `iv_column` - Column name for implied volatility
/// * `time_column` - Column name for time to expiry (in years)
/// * `rate_column` - Column name for risk-free rate
///
/// # Returns
/// * `PolarsResult<Series>` - Series with vega values (for 1% change in IV)
pub fn calculate_vega(
    df: &DataFrame,
    price_column: &str,
    strike_column: &str,
    iv_column: &str,
    time_column: &str,
    rate_column: &str,
) -> PolarsResult<Series> {
    // Extract required columns
    let price = df.column(price_column)?.f64()?;
    let strike = df.column(strike_column)?.f64()?;
    let iv = df.column(iv_column)?.f64()?;
    let time = df.column(time_column)?.f64()?;
    let rate = df.column(rate_column)?.f64()?;
    
    let len = df.height();
    let mut vega_values = vec![f64::NAN; len];
    
    for i in 0..len {
        let s = price.get(i).unwrap_or(f64::NAN);
        let k = strike.get(i).unwrap_or(f64::NAN);
        let v = iv.get(i).unwrap_or(f64::NAN);
        let t = time.get(i).unwrap_or(f64::NAN);
        let r = rate.get(i).unwrap_or(f64::NAN);
        
        if s.is_nan() || k.is_nan() || v.is_nan() || t.is_nan() || r.is_nan() || t <= 0.0 {
            continue;
        }
        
        // Calculate d1 from Black-Scholes
        let d1 = ((s / k).ln() + (r + 0.5 * v * v) * t) / (v * t.sqrt());
        
        // Calculate vega (same for calls and puts)
        // Standard vega is for 0.01 (1%) change in volatility
        let vega = 0.01 * s * t.sqrt() * norm_pdf(d1);
        
        vega_values[i] = vega;
    }
    
    Ok(Series::new("vega", vega_values))
}

/// Calculate gamma exposure
///
/// Gamma exposure measures the dollar impact of gamma across all options
/// positions at different price levels.
///
/// # Arguments
/// * `df` - DataFrame with options data
/// * `gamma_column` - Column name for calculated gamma
/// * `contracts_column` - Column name for number of contracts
/// * `multiplier_column` - Column name for contract multiplier (e.g., 100)
///
/// # Returns
/// * `PolarsResult<Series>` - Series with gamma exposure values
pub fn calculate_gamma_exposure(
    df: &DataFrame,
    gamma_column: &str,
    contracts_column: &str,
    multiplier_column: &str,
) -> PolarsResult<Series> {
    // Extract required columns
    let gamma = df.column(gamma_column)?.f64()?;
    let contracts = df.column(contracts_column)?.i64()?;
    let multiplier = df.column(multiplier_column)?.f64()?;
    
    let len = df.height();
    let mut gamma_exposure = vec![f64::NAN; len];
    
    for i in 0..len {
        let g = gamma.get(i).unwrap_or(f64::NAN);
        let c = contracts.get(i).unwrap_or(0);
        let m = multiplier.get(i).unwrap_or(f64::NAN);
        
        if g.is_nan() || m.is_nan() {
            continue;
        }
        
        // Calculate gamma exposure
        gamma_exposure[i] = g * c as f64 * m;
    }
    
    Ok(Series::new("gamma_exposure", gamma_exposure))
}

/// Normal probability density function
fn norm_pdf(x: f64) -> f64 {
    (-(x * x) / 2.0).exp() / (2.0 * PI).sqrt()
}

/// Normal cumulative distribution function
fn norm_cdf(x: f64) -> f64 {
    // Simple approximation of the normal CDF
    if x > 6.0 {
        1.0
    } else if x < -6.0 {
        0.0
    } else {
        let b1 = 0.31938153;
        let b2 = -0.356563782;
        let b3 = 1.781477937;
        let b4 = -1.821255978;
        let b5 = 1.330274429;
        let p = 0.2316419;
        let c = 0.39894228;
        
        let t = 1.0 / (1.0 + p * x.abs());
        let poly = t * (b1 + t * (b2 + t * (b3 + t * (b4 + t * b5))));
        
        if x >= 0.0 {
            1.0 - c * (-x * x / 2.0).exp() * poly
        } else {
            c * (-x * x / 2.0).exp() * poly
        }
    }
}

/// Add all Greeks indicators to the DataFrame
///
/// # Arguments
/// * `df` - DataFrame to add indicators to
///
/// # Returns
/// * `PolarsResult<()>` - Result of the operation
pub fn add_greeks_indicators(df: &mut DataFrame) -> PolarsResult<()> {
    // Check if we have the required columns
    let required_columns = [
        "price", "strike", "iv", "time_to_expiry", "rate", "is_call"
    ];
    
    for &col in required_columns.iter() {
        if !df.schema().contains(col) {
            return Err(PolarsError::ComputeError(
                format!("Required column '{}' not found", col).into(),
            ));
        }
    }
    
    // Calculate all Greeks
    let delta = calculate_delta(df, "price", "strike", "iv", "time_to_expiry", "is_call")?;
    df.with_column(delta)?;
    
    let gamma = calculate_gamma(df, "price", "strike", "iv", "time_to_expiry")?;
    df.with_column(gamma)?;
    
    let theta = calculate_theta(df, "price", "strike", "iv", "time_to_expiry", "rate", "is_call")?;
    df.with_column(theta)?;
    
    let vega = calculate_vega(df, "price", "strike", "iv", "time_to_expiry", "rate")?;
    df.with_column(vega)?;
    
    // Add gamma exposure if we have contract information
    if df.schema().contains("contracts") && df.schema().contains("multiplier") {
        let gamma_exposure = calculate_gamma_exposure(df, "gamma", "contracts", "multiplier")?;
        df.with_column(gamma_exposure)?;
    }
    
    Ok(())
} 