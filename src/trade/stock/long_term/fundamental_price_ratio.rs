use polars::prelude::*;
use crate::indicators::moving_averages::{calculate_sma, calculate_ema};

/// Calculate Price to Moving Average Ratio
///
/// This function calculates the ratio of current price to long-term moving average,
/// which helps assess whether a stock is overvalued or undervalued from a technical perspective.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `ma_period` - Moving average period (default: 200)
/// * `smoothing` - Smoothing period for ratio (default: 5)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with price to MA ratio values
pub fn calculate_price_to_ma_ratio(
    df: &DataFrame,
    ma_period: Option<usize>,
    smoothing: Option<usize>,
) -> PolarsResult<Series> {
    let period = ma_period.unwrap_or(200);
    let smooth_period = smoothing.unwrap_or(5);
    
    // Calculate long-term moving average
    let ma = calculate_sma(df, "close", period)?;
    let ma_vals = ma.f64()?;
    
    // Get closing prices
    let close = df.column("close")?.f64()?;
    
    let mut ratio = Vec::with_capacity(df.height());
    
    // First values will be NaN until we have enough data
    for i in 0..period.min(df.height()) {
        ratio.push(f64::NAN);
    }
    
    // Calculate ratio for each point
    for i in period..df.height() {
        let ma_val = ma_vals.get(i).unwrap_or(f64::NAN);
        let close_val = close.get(i).unwrap_or(f64::NAN);
        
        if ma_val.is_nan() || close_val.is_nan() || ma_val == 0.0 {
            ratio.push(f64::NAN);
            continue;
        }
        
        let current_ratio = close_val / ma_val;
        
        // Apply smoothing if enough data points
        if i >= period + smooth_period {
            let mut sum = current_ratio;
            let mut count = 1;
            
            for j in 1..=smooth_period {
                if i >= j {
                    let prev_ratio = ratio[i - j];
                    if !prev_ratio.is_nan() {
                        sum += prev_ratio;
                        count += 1;
                    }
                }
            }
            
            if count > 0 {
                ratio.push(sum / count as f64);
            } else {
                ratio.push(current_ratio);
            }
        } else {
            ratio.push(current_ratio);
        }
    }
    
    Ok(Series::new("price_to_ma_ratio", ratio))
}

/// Calculate Multi-Year Price Ratio Percentile
///
/// This function places the current price/MA ratio in a historical percentile
/// to identify extremes in valuations.
///
/// # Arguments
///
/// * `df` - DataFrame with price_to_ma_ratio already calculated
/// * `lookback_years` - Years to look back (in trading days) (default: 5)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with percentile values (0-100)
pub fn calculate_price_ratio_percentile(
    df: &DataFrame,
    lookback_years: Option<usize>,
) -> PolarsResult<Series> {
    let trading_days_per_year = 252;
    let years = lookback_years.unwrap_or(5);
    let lookback = years * trading_days_per_year;
    
    // Check if price/MA ratio is already calculated
    if !df.schema().contains("price_to_ma_ratio") {
        return Err(PolarsError::ComputeError(
            "price_to_ma_ratio column not found. Calculate it first.".into(),
        ));
    }
    
    let ratio = df.column("price_to_ma_ratio")?.f64()?;
    let mut percentile = Vec::with_capacity(df.height());
    
    // First values will be undefined until we have enough data
    for i in 0..lookback.min(df.height()) {
        percentile.push(f64::NAN);
    }
    
    // Calculate percentile for each point
    for i in lookback..df.height() {
        let current_ratio = ratio.get(i).unwrap_or(f64::NAN);
        
        if current_ratio.is_nan() {
            percentile.push(f64::NAN);
            continue;
        }
        
        // Collect historical ratios
        let mut historical_ratios = Vec::new();
        for j in (i - lookback + 1)..=i {
            let historical = ratio.get(j).unwrap_or(f64::NAN);
            if !historical.is_nan() {
                historical_ratios.push(historical);
            }
        }
        
        if historical_ratios.is_empty() {
            percentile.push(f64::NAN);
            continue;
        }
        
        // Sort ratios to calculate percentile
        historical_ratios.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        // Find position of current ratio
        let position = historical_ratios.iter()
            .position(|&x| x >= current_ratio)
            .unwrap_or(historical_ratios.len());
        
        // Calculate percentile (0-100)
        let pct = (position as f64 / historical_ratios.len() as f64) * 100.0;
        percentile.push(pct);
    }
    
    Ok(Series::new("price_ratio_percentile", percentile))
}

/// Calculate Long-Term Mean Reversion Potential
///
/// This function estimates the potential for mean reversion based on
/// the deviation from historical average price/MA ratio.
///
/// # Arguments
///
/// * `df` - DataFrame with price_to_ma_ratio and price_ratio_percentile already calculated
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with mean reversion potential (-100 to 100,
///                           positive values indicate upside potential, negative indicate downside)
pub fn calculate_mean_reversion_potential(df: &DataFrame) -> PolarsResult<Series> {
    // Check if required columns exist
    for col in ["price_to_ma_ratio", "price_ratio_percentile"].iter() {
        if !df.schema().contains(col) {
            return Err(PolarsError::ComputeError(
                format!("Required column '{}' not found", col).into(),
            ));
        }
    }
    
    let ratio = df.column("price_to_ma_ratio")?.f64()?;
    let percentile = df.column("price_ratio_percentile")?.f64()?;
    
    let mut reversion_potential = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let current_ratio = ratio.get(i).unwrap_or(f64::NAN);
        let current_percentile = percentile.get(i).unwrap_or(f64::NAN);
        
        if current_ratio.is_nan() || current_percentile.is_nan() {
            reversion_potential.push(f64::NAN);
            continue;
        }
        
        // Calculate reversion potential based on percentile
        let potential = if current_percentile > 50.0 {
            // Above average - downside potential
            -((current_percentile - 50.0) * 2.0)
        } else {
            // Below average - upside potential
            ((50.0 - current_percentile) * 2.0)
        };
        
        // Scale by how extreme the ratio is from 1.0 (fair value)
        let extremeness = (current_ratio - 1.0).abs();
        let scaled_potential = potential * (1.0 + extremeness);
        
        // Cap at ±100
        reversion_potential.push(scaled_potential.max(-100.0).min(100.0));
    }
    
    Ok(Series::new("mean_reversion_potential", reversion_potential))
}

/// Calculate Technical Valuation Rating
///
/// This function assigns a rating (1-5) based on the technical valuation metrics,
/// where 5 is extremely undervalued and 1 is extremely overvalued.
///
/// # Arguments
///
/// * `df` - DataFrame with price ratios already calculated
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with valuation ratings (1-5)
pub fn calculate_technical_valuation(df: &DataFrame) -> PolarsResult<Series> {
    // Check if required columns exist
    for col in ["price_to_ma_ratio", "price_ratio_percentile"].iter() {
        if !df.schema().contains(col) {
            return Err(PolarsError::ComputeError(
                format!("Required column '{}' not found", col).into(),
            ));
        }
    }
    
    let ratio = df.column("price_to_ma_ratio")?.f64()?;
    let percentile = df.column("price_ratio_percentile")?.f64()?;
    
    // Get reversion potential if available
    let has_reversion = df.schema().contains("mean_reversion_potential");
    let reversion = if has_reversion {
        Some(df.column("mean_reversion_potential")?.f64()?)
    } else {
        None
    };
    
    let mut valuation_rating = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let current_ratio = ratio.get(i).unwrap_or(f64::NAN);
        let current_percentile = percentile.get(i).unwrap_or(f64::NAN);
        
        if current_ratio.is_nan() || current_percentile.is_nan() {
            valuation_rating.push(3); // Neutral if no data
            continue;
        }
        
        // Base rating on percentile
        let base_rating = if current_percentile < 10.0 {
            5 // Extremely undervalued (bottom 10%)
        } else if current_percentile < 30.0 {
            4 // Undervalued (10-30%)
        } else if current_percentile > 90.0 {
            1 // Extremely overvalued (top 10%)
        } else if current_percentile > 70.0 {
            2 // Overvalued (70-90%)
        } else {
            3 // Fair value (30-70%)
        };
        
        // Adjust based on reversion potential if available
        let final_rating = if let Some(rev) = &reversion {
            let rev_val = rev.get(i).unwrap_or(0.0);
            
            if rev_val > 50.0 && base_rating < 5 {
                base_rating + 1 // Strong upside potential
            } else if rev_val < -50.0 && base_rating > 1 {
                base_rating - 1 // Strong downside potential
            } else {
                base_rating
            }
        } else {
            base_rating
        };
        
        valuation_rating.push(final_rating);
    }
    
    Ok(Series::new("value_rating", valuation_rating))
}

/// Add price ratio analysis to DataFrame
///
/// # Arguments
///
/// * `df` - Mutable reference to DataFrame
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_price_ratio_analysis(df: &mut DataFrame) -> PolarsResult<()> {
    let ratio = calculate_price_to_ma_ratio(df, None, None)?;
    df.with_column(ratio)?;
    
    let percentile = calculate_price_ratio_percentile(df, None)?;
    df.with_column(percentile)?;
    
    let reversion = calculate_mean_reversion_potential(df)?;
    df.with_column(reversion)?;
    
    let valuation = calculate_technical_valuation(df)?;
    df.with_column(valuation)?;
    
    Ok(())
} 