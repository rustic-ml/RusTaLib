use polars::prelude::*;
use crate::util::dataframe_utils::check_window_size;

/// Calculates the Aroon indicator (Aroon Up and Aroon Down)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data with high, low columns
/// * `window` - Window size for calculation (typically 25)
///
/// # Returns
///
/// Returns a PolarsResult containing a tuple of (Aroon Up, Aroon Down) Series
pub fn calculate_aroon(df: &DataFrame, window: usize) -> PolarsResult<(Series, Series)> {
    check_window_size(df, window, "Aroon")?;
    
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    
    let mut aroon_up = Vec::with_capacity(df.height());
    let mut aroon_down = Vec::with_capacity(df.height());
    
    // First values are NaN until we have enough data for window
    for _ in 0..window-1 {
        aroon_up.push(f64::NAN);
        aroon_down.push(f64::NAN);
    }
    
    for i in window-1..df.height() {
        let mut high_idx = 0;
        let mut low_idx = 0;
        let mut high_val = f64::MIN;
        let mut low_val = f64::MAX;
        
        // Find highest high and lowest low in window
        for j in 0..window {
            let h = high.get(i - j).unwrap_or(0.0);
            let l = low.get(i - j).unwrap_or(0.0);
            
            if h > high_val {
                high_val = h;
                high_idx = j;
            }
            
            if l < low_val {
                low_val = l;
                low_idx = j;
            }
        }
        
        // Calculate Aroon Up and Aroon Down
        let up = 100.0 * ((window as f64 - high_idx as f64) / window as f64);
        let down = 100.0 * ((window as f64 - low_idx as f64) / window as f64);
        
        aroon_up.push(up);
        aroon_down.push(down);
    }
    
    Ok((Series::new("aroon_up".into(), aroon_up), Series::new("aroon_down".into(), aroon_down)))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_calculate_aroon_basic() {
        // Create a simple test DataFrame
        let high = Series::new("high".into(), &[10.0, 20.0, 30.0, 25.0, 20.0]);
        let low = Series::new("low".into(), &[8.0, 15.0, 25.0, 20.0, 10.0]);
        let test_df = DataFrame::new(vec![high.into(), low.into()]).unwrap();
        
        let window = 3; // Small window for testing
        let (aroon_up, aroon_down) = calculate_aroon(&test_df, window).unwrap();
        
        // Both Aroon Up and Aroon Down should be within the range of 0 to 100
        for i in window-1..test_df.height() {
            let up = aroon_up.f64().unwrap().get(i).unwrap();
            let down = aroon_down.f64().unwrap().get(i).unwrap();
            
            assert!(up >= 0.0 && up <= 100.0);
            assert!(down >= 0.0 && down <= 100.0);
        }
        
        // For the last point, the highest high was 2 periods ago (index 2), so Aroon Up = (3-2)/3*100 = 33.33%
        // For the last point, the lowest low was 0 periods ago (current point), so Aroon Down = (3-0)/3*100 = 100%
        assert!((aroon_up.f64().unwrap().get(4).unwrap() - 33.33).abs() < 0.01);
        assert!((aroon_down.f64().unwrap().get(4).unwrap() - 100.0).abs() < 0.01);
    }
} 