use polars::prelude::*;

/// Recognize basic candlestick patterns (bullish/bearish engulfing, doji, hammer, shooting star)
/// Returns a Series with pattern labels ("none", "bullish_engulfing", etc.)
pub fn recognize_candlestick_patterns(df: &DataFrame, open_col: &str, high_col: &str, low_col: &str, close_col: &str) -> PolarsResult<Series> {
    let open = df.column(open_col)?.f64()?;
    let high = df.column(high_col)?.f64()?;
    let low = df.column(low_col)?.f64()?;
    let close = df.column(close_col)?.f64()?;
    let len = df.height();
    let mut patterns = vec!["none".to_string(); len];
    for i in 1..len {
        let o = open.get(i).unwrap_or(f64::NAN);
        let h = high.get(i).unwrap_or(f64::NAN);
        let l = low.get(i).unwrap_or(f64::NAN);
        let c = close.get(i).unwrap_or(f64::NAN);
        let prev_o = open.get(i-1).unwrap_or(f64::NAN);
        let prev_c = close.get(i-1).unwrap_or(f64::NAN);
        // Bullish Engulfing
        if c > o && prev_c < prev_o && c > prev_o && o < prev_c {
            patterns[i] = "bullish_engulfing".to_string();
        }
        // Bearish Engulfing
        else if c < o && prev_c > prev_o && c < prev_o && o > prev_c {
            patterns[i] = "bearish_engulfing".to_string();
        }
        // Doji
        else if (c - o).abs() < 0.1 * (h - l) {
            patterns[i] = "doji".to_string();
        }
        // Hammer
        else if (c > o) && ((o - l) > 2.0 * (h - c)) {
            patterns[i] = "hammer".to_string();
        }
        // Shooting Star
        else if (o > c) && ((h - o) > 2.0 * (c - l)) {
            patterns[i] = "shooting_star".to_string();
        }
    }
    Ok(Series::new("candlestick_pattern".into(), patterns))
} 