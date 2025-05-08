use polars::prelude::*;
use crate::indicators::moving_averages::{calculate_sma, calculate_ema, calculate_bollinger_bands};
use crate::indicators::oscillators::calculate_rsi;

/// Calculate Relative Strength Mean Reversion (RSMR) indicator
///
/// This indicator identifies mean reversion opportunities by measuring
/// the magnitude of a stock's deviation from its historical mean relative
/// to its normal behavior. Optimized for 3-5 day reversions.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `lookback_period` - Lookback period for baseline (default: 50)
/// * `std_dev_period` - Period for standard deviation (default: 100)
/// * `oversold_threshold` - Oversold threshold (default: -2.0)
/// * `overbought_threshold` - Overbought threshold (default: 2.0)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series containing RSMR values (z-score)
pub fn calculate_relative_strength_mean_reversion(
    df: &DataFrame,
    lookback_period: Option<usize>,
    std_dev_period: Option<usize>,
    oversold_threshold: Option<f64>,
    overbought_threshold: Option<f64>,
) -> PolarsResult<Series> {
    let lookback = lookback_period.unwrap_or(50);
    let std_dev_len = std_dev_period.unwrap_or(100);
    let oversold = oversold_threshold.unwrap_or(-2.0);
    let overbought = overbought_threshold.unwrap_or(2.0);
    
    // Calculate mean (SMA)
    let mean = calculate_sma(df, "close", lookback)?;
    let mean_vals = mean.f64()?;
    
    // Get closing prices
    let close = df.column("close")?.f64()?;
    
    // Calculate deviation from mean
    let mut deviation = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let c = close.get(i).unwrap_or(f64::NAN);
        let m = mean_vals.get(i).unwrap_or(f64::NAN);
        
        if c.is_nan() || m.is_nan() || m == 0.0 {
            deviation.push(f64::NAN);
        } else {
            deviation.push((c - m) / m * 100.0); // Percent deviation
        }
    }
    
    // Calculate rolling standard deviation of deviations
    let mut rsmr = Vec::with_capacity(df.height());
    
    // First values will be NaN until we have enough data
    for i in 0..std_dev_len.min(df.height()) {
        rsmr.push(f64::NAN);
    }
    
    for i in std_dev_len..df.height() {
        // Calculate standard deviation of deviations over the std_dev_period
        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        let mut count = 0;
        
        for j in (i - std_dev_len + 1)..=i {
            let dev = deviation[j];
            if !dev.is_nan() {
                sum += dev;
                sum_sq += dev * dev;
                count += 1;
            }
        }
        
        if count > 0 {
            let avg = sum / count as f64;
            let variance = (sum_sq / count as f64) - (avg * avg);
            let std_dev = variance.sqrt();
            
            // Calculate z-score (how many standard deviations away from mean)
            let z_score = if std_dev > 0.0 {
                (deviation[i] - avg) / std_dev
            } else {
                0.0 // Default to 0 if std_dev is 0
            };
            
            rsmr.push(z_score);
        } else {
            rsmr.push(f64::NAN);
        }
    }
    
    Ok(Series::new("rsmr", rsmr))
}

/// Calculate mean reversion signals
///
/// Generates buy/sell signals based on mean reversion principles.
///
/// # Arguments
///
/// * `df` - DataFrame with calculated RSMR
/// * `oversold_threshold` - Threshold for oversold condition (default: -2.0)
/// * `overbought_threshold` - Threshold for overbought condition (default: 2.0)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with mean reversion signals (1: buy, -1: sell, 0: no signal)
pub fn calculate_mean_reversion_signals(
    df: &DataFrame,
    oversold_threshold: Option<f64>,
    overbought_threshold: Option<f64>,
) -> PolarsResult<Series> {
    let oversold = oversold_threshold.unwrap_or(-2.0);
    let overbought = overbought_threshold.unwrap_or(2.0);
    
    if !df.schema().contains("rsmr") {
        return Err(PolarsError::ComputeError(
            "RSMR column not found. Calculate RSMR first.".into(),
        ));
    }
    
    let rsmr = df.column("rsmr")?.f64()?;
    let mut signals = Vec::with_capacity(df.height());
    
    // Calculate RSI to confirm signals
    let rsi = calculate_rsi(df, 14, "close")?;
    let rsi_vals = rsi.f64()?;
    
    for i in 0..df.height() {
        let r = rsmr.get(i).unwrap_or(f64::NAN);
        let rsi_val = rsi_vals.get(i).unwrap_or(f64::NAN);
        
        if r.is_nan() || rsi_val.is_nan() {
            signals.push(0);
        } else if r <= oversold && rsi_val < 30.0 {
            // Oversold and RSI confirms - buy signal
            signals.push(1);
        } else if r >= overbought && rsi_val > 70.0 {
            // Overbought and RSI confirms - sell signal
            signals.push(-1);
        } else {
            signals.push(0);
        }
    }
    
    Ok(Series::new("mean_reversion_signal", signals))
}

/// Calculate Bollinger Band Reversion
///
/// This indicator combines Bollinger Bands with RSI to identify
/// high-probability mean reversion opportunities.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `band_period` - Period for Bollinger Bands (default: 20)
/// * `band_std_dev` - Standard deviations for bands (default: 2.0)
/// * `rsi_period` - Period for RSI (default: 14)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with BB reversion signals (1: buy, -1: sell, 0: none)
pub fn calculate_bollinger_band_reversion(
    df: &DataFrame,
    band_period: Option<usize>,
    band_std_dev: Option<f64>,
    rsi_period: Option<usize>,
) -> PolarsResult<Series> {
    let period = band_period.unwrap_or(20);
    let std_dev = band_std_dev.unwrap_or(2.0);
    let rsi_len = rsi_period.unwrap_or(14);
    
    // Calculate Bollinger Bands
    let (bb_middle, bb_upper, bb_lower) = calculate_bollinger_bands(df, period, std_dev, "close")?;
    
    let middle_vals = bb_middle.f64()?;
    let upper_vals = bb_upper.f64()?;
    let lower_vals = bb_lower.f64()?;
    
    // Calculate RSI
    let rsi = calculate_rsi(df, rsi_len, "close")?;
    let rsi_vals = rsi.f64()?;
    
    // Get price data
    let close = df.column("close")?.f64()?;
    
    let mut bb_reversion = Vec::with_capacity(df.height());
    
    // First values will have no signal until we have enough data
    let min_periods = period.max(rsi_len);
    for i in 0..min_periods.min(df.height()) {
        bb_reversion.push(0);
    }
    
    // Calculate reversion signals
    for i in min_periods..df.height() {
        let c = close.get(i).unwrap_or(f64::NAN);
        let upper = upper_vals.get(i).unwrap_or(f64::NAN);
        let lower = lower_vals.get(i).unwrap_or(f64::NAN);
        let rsi_val = rsi_vals.get(i).unwrap_or(f64::NAN);
        
        if c.is_nan() || upper.is_nan() || lower.is_nan() || rsi_val.is_nan() {
            bb_reversion.push(0);
            continue;
        }
        
        // Check for mean reversion signals
        if c >= upper && rsi_val >= 70.0 {
            // Price above upper band and RSI overbought - potential reversal down
            bb_reversion.push(-1);
        } else if c <= lower && rsi_val <= 30.0 {
            // Price below lower band and RSI oversold - potential reversal up
            bb_reversion.push(1);
        } else {
            bb_reversion.push(0);
        }
    }
    
    Ok(Series::new("bollinger_band_reversion", bb_reversion))
}

/// Calculate mean reversion probability
///
/// This function estimates the probability of a successful mean reversion
/// trade based on historical behavior, deviation magnitude, and volume patterns.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data and RSMR indicator
/// * `lookback_period` - Lookback period for historical analysis (default: 60)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with reversion probability (0.0-1.0)
pub fn calculate_mean_reversion_probability(
    df: &DataFrame,
    lookback_period: Option<usize>,
) -> PolarsResult<Series> {
    let lookback = lookback_period.unwrap_or(60);
    
    // Check if required columns exist
    if !df.schema().contains("rsmr") {
        return Err(PolarsError::ComputeError(
            "RSMR column not found. Calculate RSMR first.".into(),
        ));
    }
    
    // Get RSMR and price data
    let rsmr = df.column("rsmr")?.f64()?;
    let close = df.column("close")?.f64()?;
    
    // Check if volume column exists for additional data
    let has_volume = df.schema().contains("volume");
    let volume = if has_volume {
        Some(df.column("volume")?.f64()?)
    } else {
        None
    };
    
    let mut probability = Vec::with_capacity(df.height());
    
    // First values will have no probability until we have enough data
    for i in 0..lookback.min(df.height()) {
        probability.push(0.0);
    }
    
    // Calculate probability for each point
    for i in lookback..df.height() {
        let current_rsmr = rsmr.get(i).unwrap_or(f64::NAN);
        
        if current_rsmr.is_nan() {
            probability.push(0.0);
            continue;
        }
        
        // Initialize with base probability based on RSMR magnitude
        let mut prob = 0.5;
        
        // Adjust probability based on RSMR magnitude (stronger deviation = higher probability)
        let abs_rsmr = current_rsmr.abs();
        if abs_rsmr >= 3.0 {
            prob += 0.25; // Very high probability on extreme deviations
        } else if abs_rsmr >= 2.0 {
            prob += 0.15; // Higher probability
        } else if abs_rsmr >= 1.0 {
            prob += 0.05; // Slightly higher probability
        }
        
        // Analyze historical RSMR levels and outcomes
        let mut success_count = 0;
        let mut total_similar = 0;
        
        for j in (i - lookback)..i {
            let historical_rsmr = rsmr.get(j).unwrap_or(f64::NAN);
            
            // Skip if not enough forward data or NaN
            if j + 5 >= df.height() || historical_rsmr.is_nan() {
                continue;
            }
            
            // Check if historical point had similar RSMR value
            if (historical_rsmr * current_rsmr > 0.0) && // Same direction
               (historical_rsmr.abs() > current_rsmr.abs() * 0.75) && // Similar magnitude
               (historical_rsmr.abs() < current_rsmr.abs() * 1.25) {
                
                total_similar += 1;
                
                // Check if it successfully reverted within 5 bars
                let start_close = close.get(j).unwrap_or(f64::NAN);
                let future_close = close.get(j + 5).unwrap_or(f64::NAN);
                
                if start_close.is_nan() || future_close.is_nan() {
                    continue;
                }
                
                // For negative RSMR (oversold), we expect price to increase
                // For positive RSMR (overbought), we expect price to decrease
                if (historical_rsmr < 0.0 && future_close > start_close) ||
                   (historical_rsmr > 0.0 && future_close < start_close) {
                    success_count += 1;
                }
            }
        }
        
        // Adjust probability based on historical success rate
        if total_similar > 0 {
            let historical_probability = success_count as f64 / total_similar as f64;
            prob = (prob + historical_probability) / 2.0; // Weighted average
        }
        
        // If volume data is available, check for volume confirmation
        if let Some(vol) = &volume {
            // Calculate average volume over past 10 bars
            let mut vol_sum = 0.0;
            let mut vol_count = 0;
            
            for j in (i - 10.min(i))..i {
                let v = vol.get(j).unwrap_or(f64::NAN);
                if !v.is_nan() {
                    vol_sum += v;
                    vol_count += 1;
                }
            }
            
            if vol_count > 0 {
                let avg_vol = vol_sum / vol_count as f64;
                let current_vol = vol.get(i).unwrap_or(f64::NAN);
                
                if !current_vol.is_nan() && current_vol > avg_vol * 1.5 {
                    // Higher volume on extreme readings increases probability
                    prob += 0.1;
                }
            }
        }
        
        // Cap probability at 0.95
        probability.push(prob.min(0.95));
    }
    
    Ok(Series::new("mean_reversion_probability", probability))
}

/// Add mean reversion analysis to DataFrame
///
/// # Arguments
///
/// * `df` - Mutable reference to DataFrame
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_mean_reversion_analysis(df: &mut DataFrame) -> PolarsResult<()> {
    let rsmr = calculate_relative_strength_mean_reversion(df, None, None, None, None)?;
    df.with_column(rsmr)?;
    
    let signals = calculate_mean_reversion_signals(df, None, None)?;
    df.with_column(signals)?;
    
    let bb_reversion = calculate_bollinger_band_reversion(df, None, None, None)?;
    df.with_column(bb_reversion)?;
    
    let probability = calculate_mean_reversion_probability(df, None)?;
    df.with_column(probability)?;
    
    Ok(())
} 