use polars::prelude::*;

/// Detect and analyze price gaps in intraday trading
///
/// This indicator identifies price gaps that occur between trading sessions
/// and classifies them by size and type (up/down). Gaps often signal strong
/// momentum and can be predictive of daily direction.
///
/// # Arguments
///
/// * `df` - DataFrame with OHLCV data
/// * `price_threshold` - Minimum percentage gap to be considered significant (default: 0.5%)
/// * `date_col` - Name of date column to separate trading sessions (default: "date")
///
/// # Returns
///
/// * `PolarsResult<(Series, Series, Series)>` - Gap size percentage, gap type, and gap fill percentage
pub fn analyze_price_gaps(
    df: &DataFrame,
    price_threshold: Option<f64>,
    date_col: Option<&str>,
) -> PolarsResult<(Series, Series, Series)> {
    let threshold = price_threshold.unwrap_or(0.5);
    let date_column = date_col.unwrap_or("date");
    
    // Ensure necessary columns exist
    for col in ["open", "close", date_column].iter() {
        if !df.schema().contains(*col) {
            return Err(PolarsError::ComputeError(
                format!("Required column '{}' not found", col).into(),
            ));
        }
    }
    
    // Get price data
    let open = df.column("open")?.f64()?;
    let close = df.column("close")?.f64()?;
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    
    // Create vectors for results
    let mut gap_size = Vec::with_capacity(df.height());
    let mut gap_type = Vec::with_capacity(df.height());
    let mut gap_fill_pct = Vec::with_capacity(df.height());
    
    // Get date column to detect session boundaries
    let dates = df.column(date_column)?;
    
    // First row has no previous day to compare with
    gap_size.push(0.0);
    gap_type.push(0); // 0 = no gap, 1 = gap up, -1 = gap down
    gap_fill_pct.push(100.0); // Fully filled by default
    
    // Find gaps between trading sessions
    for i in 1..df.height() {
        let current_open = open.get(i).unwrap_or(f64::NAN);
        let prev_close = close.get(i - 1).unwrap_or(f64::NAN);
        let current_high = high.get(i).unwrap_or(f64::NAN);
        let current_low = low.get(i).unwrap_or(f64::NAN);
        
        // Check if this is a new trading session
        let is_new_session = match dates.dtype() {
            DataType::Date => {
                let current_date = dates.date()?.get(i);
                let prev_date = dates.date()?.get(i - 1);
                current_date != prev_date
            },
            DataType::Utf8 => {
                let current_date = dates.str()?.get(i).unwrap_or("");
                let prev_date = dates.str()?.get(i - 1).unwrap_or("");
                current_date != prev_date
            },
            DataType::Datetime(_, _) => {
                let current_date = dates.datetime()?.get(i);
                let prev_date = dates.datetime()?.get(i - 1);
                current_date.map(|d| d / 86400000).unwrap_or(0) != 
                    prev_date.map(|d| d / 86400000).unwrap_or(0)
            },
            _ => false, // Unknown date type
        };
        
        if is_new_session && !current_open.is_nan() && !prev_close.is_nan() && prev_close != 0.0 {
            // Calculate gap percentage
            let gap_pct = (current_open - prev_close) / prev_close * 100.0;
            
            // Only consider gaps above threshold
            if gap_pct.abs() >= threshold {
                gap_size.push(gap_pct);
                
                if gap_pct > 0.0 {
                    gap_type.push(1); // Gap up
                    
                    // Calculate gap fill percentage for gap up
                    // (How much the price moved back towards the previous close)
                    if !current_low.is_nan() && current_low < current_open {
                        let gap_size_points = current_open - prev_close;
                        let fill_points = current_open - current_low.max(prev_close);
                        let fill_percentage = if gap_size_points > 0.0 {
                            (fill_points / gap_size_points * 100.0).min(100.0)
                        } else {
                            100.0
                        };
                        gap_fill_pct.push(fill_percentage);
                    } else {
                        gap_fill_pct.push(0.0); // No fill
                    }
                } else {
                    gap_type.push(-1); // Gap down
                    
                    // Calculate gap fill percentage for gap down
                    // (How much the price moved back towards the previous close)
                    if !current_high.is_nan() && current_high > current_open {
                        let gap_size_points = prev_close - current_open;
                        let fill_points = current_high.min(prev_close) - current_open;
                        let fill_percentage = if gap_size_points > 0.0 {
                            (fill_points / gap_size_points * 100.0).min(100.0)
                        } else {
                            100.0
                        };
                        gap_fill_pct.push(fill_percentage);
                    } else {
                        gap_fill_pct.push(0.0); // No fill
                    }
                }
            } else {
                // No significant gap
                gap_size.push(0.0);
                gap_type.push(0);
                gap_fill_pct.push(100.0);
            }
        } else {
            // Not a new session or missing data
            gap_size.push(0.0);
            gap_type.push(0);
            gap_fill_pct.push(100.0);
        }
    }
    
    Ok((
        Series::new("gap_size_pct", gap_size),
        Series::new("gap_type", gap_type),
        Series::new("gap_fill_pct", gap_fill_pct),
    ))
}

/// Add gap analysis to DataFrame
///
/// # Arguments
///
/// * `df` - Mutable reference to DataFrame
/// * `price_threshold` - Optional minimum gap percentage
///
/// # Returns
///
/// * `PolarsResult<()>` - Result indicating success or failure
pub fn add_gap_analysis(df: &mut DataFrame, price_threshold: Option<f64>) -> PolarsResult<()> {
    let (gap_size, gap_type, gap_fill) = analyze_price_gaps(df, price_threshold, None)?;
    
    df.with_column(gap_size)?;
    df.with_column(gap_type)?;
    df.with_column(gap_fill)?;
    
    // Add signal based on gap type and historical fill rate
    let gap_type_vals = gap_type.i32()?;
    let gap_size_vals = gap_size.f64()?;
    
    let mut gap_fade_signal = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let g_type = gap_type_vals.get(i).unwrap_or(0);
        let g_size = gap_size_vals.get(i).unwrap_or(0.0);
        
        // Oversized gaps tend to fill (fade) more often
        // Generate a contrarian signal on large gaps
        if g_type > 0 && g_size > 2.0 {
            // Fade large gap ups
            gap_fade_signal.push(-1);
        } else if g_type < 0 && g_size.abs() > 2.0 {
            // Fade large gap downs
            gap_fade_signal.push(1);
        } else if g_type != 0 {
            // For smaller gaps, go with the direction
            gap_fade_signal.push(g_type);
        } else {
            // No gap
            gap_fade_signal.push(0);
        }
    }
    
    df.with_column(Series::new("gap_trade_signal", gap_fade_signal))?;
    
    Ok(())
}

/// Calculate historical gap fill probability
///
/// This function analyzes how often gaps of different sizes fill,
/// helping traders assess the probability of a gap filling during the session.
///
/// # Arguments
///
/// * `df` - DataFrame with gap analysis columns
/// * `gap_size_bins` - Array of gap size thresholds to analyze
///
/// # Returns
///
/// * `PolarsResult<Vec<(f64, f64)>>` - Vec of (gap_size_threshold, fill_probability)
pub fn calculate_gap_fill_probability(
    df: &DataFrame,
    gap_size_bins: &[f64],
) -> PolarsResult<Vec<(f64, f64)>> {
    if !df.schema().contains("gap_type") || 
       !df.schema().contains("gap_size_pct") || 
       !df.schema().contains("gap_fill_pct") {
        return Err(PolarsError::ComputeError(
            "Required gap analysis columns not found. Run gap analysis first.".into(),
        ));
    }
    
    let gap_type = df.column("gap_type")?.i32()?;
    let gap_size = df.column("gap_size_pct")?.f64()?;
    let gap_fill = df.column("gap_fill_pct")?.f64()?;
    
    let mut results = Vec::new();
    
    // Group gaps by size bins and calculate fill probability
    for &size_threshold in gap_size_bins {
        let mut total_count = 0;
        let mut filled_count = 0;
        
        for i in 0..df.height() {
            let g_type = gap_type.get(i).unwrap_or(0);
            let g_size = gap_size.get(i).unwrap_or(0.0).abs();
            let g_fill = gap_fill.get(i).unwrap_or(0.0);
            
            // Only consider actual gaps within the size bin
            if g_type != 0 && g_size >= size_threshold && g_size < size_threshold + 1.0 {
                total_count += 1;
                
                // Consider a gap "filled" if it filled at least 80%
                if g_fill >= 80.0 {
                    filled_count += 1;
                }
            }
        }
        
        let probability = if total_count > 0 {
            filled_count as f64 / total_count as f64 * 100.0
        } else {
            0.0
        };
        
        results.push((size_threshold, probability));
    }
    
    Ok(results)
} 