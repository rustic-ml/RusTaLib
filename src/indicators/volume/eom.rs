use polars::prelude::*;

/// Calculate Ease of Movement (EOM)
///
/// Returns a Series with EOM values
pub fn calculate_eom(df: &DataFrame, high_col: &str, low_col: &str, volume_col: &str, period: usize) -> PolarsResult<Series> {
    let high = df.column(high_col)?.f64()?;
    let low = df.column(low_col)?.f64()?;
    let volume = df.column(volume_col)?.f64()?;
    let len = df.height();
    let mut eom = vec![f64::NAN; len];
    for i in 1..len {
        let distance = ((high.get(i).unwrap_or(f64::NAN) + low.get(i).unwrap_or(f64::NAN)) / 2.0)
            - ((high.get(i-1).unwrap_or(f64::NAN) + low.get(i-1).unwrap_or(f64::NAN)) / 2.0);
        let box_ratio = if (volume.get(i).unwrap_or(f64::NAN) != 0.0) && ((high.get(i).unwrap_or(f64::NAN) - low.get(i).unwrap_or(f64::NAN)) != 0.0) {
            volume.get(i).unwrap_or(f64::NAN) / (high.get(i).unwrap_or(f64::NAN) - low.get(i).unwrap_or(f64::NAN))
        } else {
            f64::NAN
        };
        eom[i] = if !box_ratio.is_nan() && box_ratio != 0.0 {
            distance / box_ratio
        } else {
            f64::NAN
        };
    }
    // Optionally smooth with SMA
    let mut eom_sma = vec![f64::NAN; len];
    for i in 0..len {
        if i+1 >= period {
            let sum: f64 = eom[(i+1-period)..=i].iter().filter(|x| !x.is_nan()).sum();
            let count = eom[(i+1-period)..=i].iter().filter(|x| !x.is_nan()).count();
            if count > 0 {
                eom_sma[i] = sum / count as f64;
            }
        }
    }
    Ok(Series::new("eom".into(), eom_sma))
} 