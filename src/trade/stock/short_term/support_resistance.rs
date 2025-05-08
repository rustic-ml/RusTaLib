use polars::prelude::*;
use std::collections::HashMap;

/// Calculate key support and resistance levels
///
/// This function identifies important price levels where a stock has
/// historically reversed direction, creating support (price floor) and
/// resistance (price ceiling) zones.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `lookback_period` - How far back to look for levels (default: 90)
/// * `price_tolerance` - Percentage tolerance for level proximity (default: 1.0)
/// * `min_touches` - Minimum number of times a level must be tested (default: 2)
///
/// # Returns
///
/// * `PolarsResult<(Vec<f64>, Vec<f64>)>` - (Support levels, Resistance levels)
pub fn identify_key_levels(
    df: &DataFrame,
    lookback_period: Option<usize>,
    price_tolerance: Option<f64>,
    min_touches: Option<usize>,
) -> PolarsResult<(Vec<f64>, Vec<f64>)> {
    let lookback = lookback_period.unwrap_or(90);
    let tolerance = price_tolerance.unwrap_or(1.0) / 100.0; // Convert to decimal
    let touches = min_touches.unwrap_or(2);
    
    // Get price data
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    
    // Calculate number of periods to analyze
    let start_idx = if df.height() > lookback {
        df.height() - lookback
    } else {
        0
    };
    
    // Collect local highs and lows
    let mut swing_highs = Vec::new();
    let mut swing_lows = Vec::new();
    
    // Look back at least 2 bars and forward 2 bars when identifying swings
    let min_bars = 2;
    
    // Identify swing points
    for i in (start_idx + min_bars)..(df.height().saturating_sub(min_bars)) {
        // Check for swing high (local peak)
        let mut is_swing_high = true;
        for j in 1..=min_bars {
            if high.get(i).unwrap_or(f64::NAN) <= high.get(i - j).unwrap_or(f64::NAN) ||
               high.get(i).unwrap_or(f64::NAN) <= high.get(i + j).unwrap_or(f64::NAN) {
                is_swing_high = false;
                break;
            }
        }
        
        if is_swing_high {
            swing_highs.push((i, high.get(i).unwrap_or(f64::NAN)));
        }
        
        // Check for swing low (local trough)
        let mut is_swing_low = true;
        for j in 1..=min_bars {
            if low.get(i).unwrap_or(f64::NAN) >= low.get(i - j).unwrap_or(f64::NAN) ||
               low.get(i).unwrap_or(f64::NAN) >= low.get(i + j).unwrap_or(f64::NAN) {
                is_swing_low = false;
                break;
            }
        }
        
        if is_swing_low {
            swing_lows.push((i, low.get(i).unwrap_or(f64::NAN)));
        }
    }
    
    // Group similar price levels using tolerance
    let mut resistance_clusters: HashMap<usize, Vec<f64>> = HashMap::new();
    let mut support_clusters: HashMap<usize, Vec<f64>> = HashMap::new();
    
    let mut next_cluster_id = 0;
    
    // Cluster resistance levels
    for (_, price) in &swing_highs {
        let mut found_cluster = false;
        
        for (cluster_id, prices) in &mut resistance_clusters {
            let avg_price = prices.iter().sum::<f64>() / prices.len() as f64;
            
            // If price is within tolerance, add to this cluster
            if (*price - avg_price).abs() / avg_price <= tolerance {
                resistance_clusters.get_mut(cluster_id).unwrap().push(*price);
                found_cluster = true;
                break;
            }
        }
        
        // If no matching cluster found, create a new one
        if !found_cluster && !price.is_nan() {
            resistance_clusters.insert(next_cluster_id, vec![*price]);
            next_cluster_id += 1;
        }
    }
    
    // Cluster support levels
    for (_, price) in &swing_lows {
        let mut found_cluster = false;
        
        for (cluster_id, prices) in &mut support_clusters {
            let avg_price = prices.iter().sum::<f64>() / prices.len() as f64;
            
            // If price is within tolerance, add to this cluster
            if (*price - avg_price).abs() / avg_price <= tolerance {
                support_clusters.get_mut(cluster_id).unwrap().push(*price);
                found_cluster = true;
                break;
            }
        }
        
        // If no matching cluster found, create a new one
        if !found_cluster && !price.is_nan() {
            support_clusters.insert(next_cluster_id, vec![*price]);
            next_cluster_id += 1;
        }
    }
    
    // Filter and calculate average for each cluster
    let mut resistance_levels = Vec::new();
    for (_, prices) in resistance_clusters {
        if prices.len() >= touches {
            let avg_price = prices.iter().sum::<f64>() / prices.len() as f64;
            resistance_levels.push(avg_price);
        }
    }
    
    let mut support_levels = Vec::new();
    for (_, prices) in support_clusters {
        if prices.len() >= touches {
            let avg_price = prices.iter().sum::<f64>() / prices.len() as f64;
            support_levels.push(avg_price);
        }
    }
    
    // Sort levels
    resistance_levels.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    support_levels.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    
    Ok((support_levels, resistance_levels))
}

/// Calculate nearest support and resistance levels
///
/// This function finds the closest support and resistance levels
/// relative to the current price.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `support_levels` - Vector of support levels
/// * `resistance_levels` - Vector of resistance levels
///
/// # Returns
///
/// * `PolarsResult<(Series, Series)>` - (Nearest support, Nearest resistance)
pub fn calculate_nearest_levels(
    df: &DataFrame,
    support_levels: &[f64],
    resistance_levels: &[f64],
) -> PolarsResult<(Series, Series)> {
    let close = df.column("close")?.f64()?;
    
    let mut nearest_support = Vec::with_capacity(df.height());
    let mut nearest_resistance = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let price = close.get(i).unwrap_or(f64::NAN);
        
        if price.is_nan() {
            nearest_support.push(f64::NAN);
            nearest_resistance.push(f64::NAN);
            continue;
        }
        
        // Find nearest support (below current price)
        let mut best_support = f64::NAN;
        let mut best_support_diff = f64::MAX;
        
        for &level in support_levels {
            if level < price {
                let diff = price - level;
                if diff < best_support_diff {
                    best_support_diff = diff;
                    best_support = level;
                }
            }
        }
        
        // Find nearest resistance (above current price)
        let mut best_resistance = f64::NAN;
        let mut best_resistance_diff = f64::MAX;
        
        for &level in resistance_levels {
            if level > price {
                let diff = level - price;
                if diff < best_resistance_diff {
                    best_resistance_diff = diff;
                    best_resistance = level;
                }
            }
        }
        
        nearest_support.push(best_support);
        nearest_resistance.push(best_resistance);
    }
    
    Ok((
        Series::new("nearest_support", nearest_support),
        Series::new("nearest_resistance", nearest_resistance),
    ))
}

/// Calculate support/resistance strength
///
/// This function determines the strength of support and resistance levels
/// based on the number of times they've been tested and respected.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `support_level` - Support level to evaluate
/// * `resistance_level` - Resistance level to evaluate
/// * `price_tolerance` - Percentage tolerance for level testing (default: 0.5)
///
/// # Returns
///
/// * `PolarsResult<(i32, i32)>` - (Support strength, Resistance strength)
///   Values typically range from 1 (weak) to 5 (strong)
pub fn calculate_level_strength(
    df: &DataFrame,
    support_level: f64,
    resistance_level: f64,
    price_tolerance: Option<f64>,
) -> PolarsResult<(i32, i32)> {
    let tolerance = price_tolerance.unwrap_or(0.5) / 100.0; // Convert to decimal
    
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    
    let mut support_tests = 0;
    let mut support_breaks = 0;
    let mut resistance_tests = 0;
    let mut resistance_breaks = 0;
    
    let support_lower = support_level * (1.0 - tolerance);
    let support_upper = support_level * (1.0 + tolerance);
    let resistance_lower = resistance_level * (1.0 - tolerance);
    let resistance_upper = resistance_level * (1.0 + tolerance);
    
    // Look for tests and breaks of levels
    for i in 1..df.height() {
        let prev_close = close.get(i - 1).unwrap_or(f64::NAN);
        let curr_close = close.get(i).unwrap_or(f64::NAN);
        let curr_low = low.get(i).unwrap_or(f64::NAN);
        let curr_high = high.get(i).unwrap_or(f64::NAN);
        
        if curr_low.is_nan() || curr_high.is_nan() || prev_close.is_nan() || curr_close.is_nan() {
            continue;
        }
        
        // Test of support
        if curr_low >= support_lower && curr_low <= support_upper {
            support_tests += 1;
            
            // Break of support
            if curr_close < support_lower {
                support_breaks += 1;
            }
        }
        
        // Test of resistance
        if curr_high >= resistance_lower && curr_high <= resistance_upper {
            resistance_tests += 1;
            
            // Break of resistance
            if curr_close > resistance_upper {
                resistance_breaks += 1;
            }
        }
    }
    
    // Calculate strength based on tests and breaks
    let support_strength = if support_tests == 0 {
        0 // No tests
    } else {
        let respect_ratio = 1.0 - (support_breaks as f64 / support_tests as f64);
        
        // Scale from 1-5 based on both number of tests and respect ratio
        let num_tests_factor = (support_tests as f64).min(5.0) / 5.0;
        let strength = ((respect_ratio * 0.7 + num_tests_factor * 0.3) * 5.0).round() as i32;
        
        // Ensure strength is between 1-5
        strength.max(1).min(5)
    };
    
    let resistance_strength = if resistance_tests == 0 {
        0 // No tests
    } else {
        let respect_ratio = 1.0 - (resistance_breaks as f64 / resistance_tests as f64);
        
        // Scale from 1-5 based on both number of tests and respect ratio
        let num_tests_factor = (resistance_tests as f64).min(5.0) / 5.0;
        let strength = ((respect_ratio * 0.7 + num_tests_factor * 0.3) * 5.0).round() as i32;
        
        // Ensure strength is between 1-5
        strength.max(1).min(5)
    };
    
    Ok((support_strength, resistance_strength))
}

/// Calculate risk-reward ratio based on support/resistance
///
/// This function calculates potential risk-reward ratio for a swing trade
/// based on current price and nearby support/resistance levels.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV and nearest_support/nearest_resistance
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with risk-reward ratios
pub fn calculate_risk_reward_ratio(df: &DataFrame) -> PolarsResult<Series> {
    // Check if required columns exist
    for col in ["close", "nearest_support", "nearest_resistance"].iter() {
        if !df.schema().contains(*col) {
            return Err(PolarsError::ComputeError(
                format!("Required column '{}' not found", col).into(),
            ));
        }
    }
    
    let close = df.column("close")?.f64()?;
    let support = df.column("nearest_support")?.f64()?;
    let resistance = df.column("nearest_resistance")?.f64()?;
    
    let mut risk_reward = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let price = close.get(i).unwrap_or(f64::NAN);
        let support_level = support.get(i).unwrap_or(f64::NAN);
        let resistance_level = resistance.get(i).unwrap_or(f64::NAN);
        
        if price.is_nan() || support_level.is_nan() || resistance_level.is_nan() {
            risk_reward.push(f64::NAN);
            continue;
        }
        
        // Calculate potential reward and risk
        let potential_reward = resistance_level - price;
        let potential_risk = price - support_level;
        
        // Calculate ratio
        if potential_risk > 0.0 {
            let ratio = potential_reward / potential_risk;
            risk_reward.push(ratio);
        } else {
            risk_reward.push(f64::NAN); // Cannot calculate ratio
        }
    }
    
    Ok(Series::new("risk_reward_ratio", risk_reward))
}

/// Add support and resistance analysis to DataFrame
///
/// # Arguments
///
/// * `df` - Mutable reference to DataFrame
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_support_resistance_analysis(df: &mut DataFrame) -> PolarsResult<()> {
    // Identify key levels
    let (support_levels, resistance_levels) = identify_key_levels(df, None, None, None)?;
    
    // Calculate nearest levels for each bar
    let (nearest_support, nearest_resistance) = calculate_nearest_levels(
        df, &support_levels, &resistance_levels
    )?;
    
    // Add to DataFrame
    df.with_column(nearest_support)?;
    df.with_column(nearest_resistance)?;
    
    // Calculate risk-reward ratio
    let risk_reward = calculate_risk_reward_ratio(df)?;
    df.with_column(risk_reward)?;
    
    // Store all support and resistance levels in a single string column
    let mut all_support = String::new();
    for level in support_levels {
        all_support.push_str(&format!("{:.2},", level));
    }
    
    let mut all_resistance = String::new();
    for level in resistance_levels {
        all_resistance.push_str(&format!("{:.2},", level));
    }
    
    // Create columns with all levels
    let all_support_series = Series::new(
        "all_support_levels", 
        vec![all_support; df.height()]
    );
    
    let all_resistance_series = Series::new(
        "all_resistance_levels", 
        vec![all_resistance; df.height()]
    );
    
    df.with_column(all_support_series)?;
    df.with_column(all_resistance_series)?;
    
    Ok(())
} 