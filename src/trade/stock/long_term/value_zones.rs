use polars::prelude::*;
use crate::indicators::moving_averages::{calculate_sma, calculate_bollinger_bands};
use std::collections::HashMap;

/// Calculate Long-Term Value Zones
///
/// This function identifies historical price zones where a stock has
/// spent significant time, indicating potential value areas for position trading.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `period` - Period for analysis (default: 1000 bars)
/// * `num_zones` - Number of zones to identify (default: 5)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with value zone identifiers (1-5, higher = stronger)
pub fn calculate_value_zones(
    df: &DataFrame,
    period: Option<usize>,
    num_zones: Option<usize>,
) -> PolarsResult<Series> {
    let lookback = period.unwrap_or(1000);
    let zones = num_zones.unwrap_or(5);
    
    // Get price data
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    
    let mut value_zones = Vec::with_capacity(df.height());
    
    // First values will have no zones until we have enough data
    for i in 0..lookback.min(df.height()) {
        value_zones.push(0);
    }
    
    // Calculate zones for each point
    for i in lookback..df.height() {
        let current_close = close.get(i).unwrap_or(f64::NAN);
        
        if current_close.is_nan() {
            value_zones.push(0);
            continue;
        }
        
        // Find min and max over the lookback period
        let mut min_price = f64::MAX;
        let mut max_price = f64::MIN;
        
        for j in (i - lookback + 1)..=i {
            let h = high.get(j).unwrap_or(f64::NAN);
            let l = low.get(j).unwrap_or(f64::NAN);
            
            if !h.is_nan() && h > max_price {
                max_price = h;
            }
            
            if !l.is_nan() && l < min_price {
                min_price = l;
            }
        }
        
        if min_price == f64::MAX || max_price == f64::MIN {
            value_zones.push(0);
            continue;
        }
        
        // Calculate price range and zone height
        let price_range = max_price - min_price;
        let zone_height = price_range / zones as f64;
        
        // Calculate histogram of prices to identify value zones
        let mut price_counts = HashMap::new();
        
        for j in (i - lookback + 1)..=i {
            let c = close.get(j).unwrap_or(f64::NAN);
            
            if c.is_nan() {
                continue;
            }
            
            // Determine which zone this price falls into
            let zone_index = ((c - min_price) / zone_height).floor() as usize;
            let zone = zone_index.min(zones - 1) + 1; // 1-based zone index
            
            *price_counts.entry(zone).or_insert(0) += 1;
        }
        
        // Find the zone with the most price points
        let mut max_count = 0;
        let mut strongest_zone = 0;
        
        for (zone, count) in &price_counts {
            if *count > max_count {
                max_count = *count;
                strongest_zone = *zone;
            }
        }
        
        // Calculate strength of each zone in relation to current price
        let current_zone = ((current_close - min_price) / zone_height).floor() as usize + 1;
        
        // Determine zone strength (higher = stronger value zone)
        let zone_strength = if let Some(count) = price_counts.get(&current_zone) {
            // Calculate as percentage of points in this zone compared to max zone
            ((*count as f64 / max_count as f64) * 5.0).round() as i32
        } else {
            0 // Price not in any defined zone
        };
        
        value_zones.push(zone_strength);
    }
    
    Ok(Series::new("value_zone_strength", value_zones))
}

/// Calculate Long-Term Price Density
///
/// This function measures the density of historical prices around
/// the current price, useful for identifying significant price levels.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `lookback_period` - Period for analysis (default: 1000 bars)
/// * `bandwidth` - Bandwidth for density calculation as % of price (default: 5%)
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with price density values (0-100)
pub fn calculate_price_density(
    df: &DataFrame,
    lookback_period: Option<usize>,
    bandwidth: Option<f64>,
) -> PolarsResult<Series> {
    let lookback = lookback_period.unwrap_or(1000);
    let bw = bandwidth.unwrap_or(5.0) / 100.0; // Convert to decimal
    
    // Get price data
    let close = df.column("close")?.f64()?;
    
    let mut density = Vec::with_capacity(df.height());
    
    // First values will have no density until we have enough data
    for i in 0..lookback.min(df.height()) {
        density.push(f64::NAN);
    }
    
    // Calculate density for each point
    for i in lookback..df.height() {
        let current_close = close.get(i).unwrap_or(f64::NAN);
        
        if current_close.is_nan() {
            density.push(f64::NAN);
            continue;
        }
        
        // Count prices within bandwidth of current price
        let mut count_in_band = 0;
        let lower_band = current_close * (1.0 - bw);
        let upper_band = current_close * (1.0 + bw);
        
        let mut total_valid = 0;
        
        for j in (i - lookback + 1)..=i {
            let c = close.get(j).unwrap_or(f64::NAN);
            
            if !c.is_nan() {
                total_valid += 1;
                
                if c >= lower_band && c <= upper_band {
                    count_in_band += 1;
                }
            }
        }
        
        // Calculate density as percentage of prices within band
        if total_valid > 0 {
            let density_pct = (count_in_band as f64 / total_valid as f64) * 100.0;
            density.push(density_pct);
        } else {
            density.push(f64::NAN);
        }
    }
    
    Ok(Series::new("price_density", density))
}

/// Identify Long-Term Value Ranges
///
/// This function identifies the historical price ranges where the stock
/// has spent significant time, useful for position trading entries and exits.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `lookback_period` - Period for analysis (default: 1000 bars)
///
/// # Returns
///
/// * `PolarsResult<(Series, Series)>` - (Lower bound, Upper bound) of value range
pub fn identify_value_ranges(
    df: &DataFrame,
    lookback_period: Option<usize>,
) -> PolarsResult<(Series, Series)> {
    let lookback = lookback_period.unwrap_or(1000);
    
    // Get price data
    let close = df.column("close")?.f64()?;
    
    // Calculate long-term Bollinger Bands for value range
    let (middle, upper, lower) = calculate_bollinger_bands(df, lookback / 5, 1.5, "close")?;
    
    let middle_vals = middle.f64()?;
    let upper_vals = upper.f64()?;
    let lower_vals = lower.f64()?;
    
    let mut value_lower = Vec::with_capacity(df.height());
    let mut value_upper = Vec::with_capacity(df.height());
    
    // First values will have no ranges until we have enough data
    for i in 0..lookback.min(df.height()) {
        value_lower.push(f64::NAN);
        value_upper.push(f64::NAN);
    }
    
    // Calculate ranges for each point
    for i in lookback..df.height() {
        let m = middle_vals.get(i).unwrap_or(f64::NAN);
        let u = upper_vals.get(i).unwrap_or(f64::NAN);
        let l = lower_vals.get(i).unwrap_or(f64::NAN);
        
        if m.is_nan() || u.is_nan() || l.is_nan() {
            value_lower.push(f64::NAN);
            value_upper.push(f64::NAN);
            continue;
        }
        
        // Find price distribution over the lookback period
        let mut prices = Vec::new();
        
        for j in (i - lookback + 1)..=i {
            let c = close.get(j).unwrap_or(f64::NAN);
            if !c.is_nan() {
                prices.push(c);
            }
        }
        
        if prices.is_empty() {
            value_lower.push(f64::NAN);
            value_upper.push(f64::NAN);
            continue;
        }
        
        // Sort prices
        prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        // Calculate 25th and 75th percentiles as value range
        let q1_idx = (prices.len() as f64 * 0.25).floor() as usize;
        let q3_idx = (prices.len() as f64 * 0.75).floor() as usize;
        
        let q1 = prices[q1_idx];
        let q3 = prices[q3_idx];
        
        // Blend with Bollinger Bands for smoothness
        let lower_bound = (q1 + l) / 2.0;
        let upper_bound = (q3 + u) / 2.0;
        
        value_lower.push(lower_bound);
        value_upper.push(upper_bound);
    }
    
    Ok((
        Series::new("value_range_lower", value_lower),
        Series::new("value_range_upper", value_upper),
    ))
}

/// Calculate Position in Value Range
///
/// This function calculates where the current price is positioned within
/// its long-term value range as a percentage.
///
/// # Arguments
///
/// * `df` - DataFrame with value_range_lower and value_range_upper already calculated
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with position values (0-100%,
///                          0% = at lower bound, 100% = at upper bound)
pub fn calculate_value_range_position(df: &DataFrame) -> PolarsResult<Series> {
    // Check if required columns exist
    for col in ["value_range_lower", "value_range_upper"].iter() {
        if !df.schema().contains(col) {
            return Err(PolarsError::ComputeError(
                format!("Required column '{}' not found", col).into(),
            ));
        }
    }
    
    let lower = df.column("value_range_lower")?.f64()?;
    let upper = df.column("value_range_upper")?.f64()?;
    let close = df.column("close")?.f64()?;
    
    let mut position = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let l = lower.get(i).unwrap_or(f64::NAN);
        let u = upper.get(i).unwrap_or(f64::NAN);
        let c = close.get(i).unwrap_or(f64::NAN);
        
        if l.is_nan() || u.is_nan() || c.is_nan() || l == u {
            position.push(f64::NAN);
            continue;
        }
        
        // Calculate position as percentage within range
        let pos_pct = ((c - l) / (u - l) * 100.0).max(0.0).min(100.0);
        position.push(pos_pct);
    }
    
    Ok(Series::new("value_range_position", position))
}

/// Add value zones analysis to DataFrame
///
/// # Arguments
///
/// * `df` - Mutable reference to DataFrame
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_value_zones_analysis(df: &mut DataFrame) -> PolarsResult<()> {
    let zones = calculate_value_zones(df, None, None)?;
    df.with_column(zones)?;
    
    let density = calculate_price_density(df, None, None)?;
    df.with_column(density)?;
    
    let (lower, upper) = identify_value_ranges(df, None)?;
    df.with_column(lower)?;
    df.with_column(upper)?;
    
    let position = calculate_value_range_position(df)?;
    df.with_column(position)?;
    
    Ok(())
} 