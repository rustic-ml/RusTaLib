use polars::prelude::*;

/// Calculate VWAP Bands
/// Returns (vwap, upper_band, lower_band)
pub fn calculate_vwap_bands(df: &DataFrame, price_col: &str, volume_col: &str, window: usize, num_std: f64) -> PolarsResult<(Series, Series, Series)> {
    let price = df.column(price_col)?.f64()?;
    let volume = df.column(volume_col)?.f64()?;
    let len = df.height();
    let mut vwap = vec![f64::NAN; len];
    let mut upper = vec![f64::NAN; len];
    let mut lower = vec![f64::NAN; len];
    for i in 0..len {
        if i+1 >= window {
            let p = price.slice((i+1-window) as i64, window);
            let v = volume.slice((i+1-window) as i64, window);
            let mut sum_pv = 0.0;
            let mut sum_v = 0.0;
            let mut prices = Vec::with_capacity(window);
            for j in 0..window {
                let px = p.get(j).unwrap_or(f64::NAN);
                let vol = v.get(j).unwrap_or(f64::NAN);
                sum_pv += px * vol;
                sum_v += vol;
                prices.push(px);
            }
            if sum_v > 0.0 {
                vwap[i] = sum_pv / sum_v;
                let mean = vwap[i];
                let std = (prices.iter().map(|x| (x-mean).powi(2)).sum::<f64>() / window as f64).sqrt();
                upper[i] = mean + num_std * std;
                lower[i] = mean - num_std * std;
            }
        }
    }
    Ok((
        Series::new("vwap_band_vwap".into(), vwap),
        Series::new("vwap_band_upper".into(), upper),
        Series::new("vwap_band_lower".into(), lower),
    ))
} 