use polars::prelude::*;

/// Calculate Advance/Decline Line (AD Line)
/// Expects a DataFrame with a column 'advance' (1 for advancing, 0 otherwise)
/// and 'decline' (1 for declining, 0 otherwise)
pub fn calculate_advance_decline_line(df: &DataFrame, advance_col: &str, decline_col: &str) -> PolarsResult<Series> {
    let advance = df.column(advance_col)?.i32()?;
    let decline = df.column(decline_col)?.i32()?;
    let len = df.height();
    let mut ad_line = vec![0i32; len];
    for i in 0..len {
        let net = advance.get(i).unwrap_or(0) - decline.get(i).unwrap_or(0);
        ad_line[i] = if i == 0 { net } else { ad_line[i-1] + net };
    }
    Ok(Series::new("ad_line".into(), ad_line))
}

/// Calculate TRIN (Arms Index)
/// Expects columns: 'advancing_volume', 'declining_volume', 'advance', 'decline'
pub fn calculate_trin(df: &DataFrame, adv_vol_col: &str, dec_vol_col: &str, adv_col: &str, dec_col: &str) -> PolarsResult<Series> {
    let adv_vol = df.column(adv_vol_col)?.f64()?;
    let dec_vol = df.column(dec_vol_col)?.f64()?;
    let adv = df.column(adv_col)?.f64()?;
    let dec = df.column(dec_col)?.f64()?;
    let len = df.height();
    let mut trin = vec![f64::NAN; len];
    for i in 0..len {
        let advv = adv_vol.get(i).unwrap_or(f64::NAN);
        let decv = dec_vol.get(i).unwrap_or(f64::NAN);
        let advn = adv.get(i).unwrap_or(f64::NAN);
        let decn = dec.get(i).unwrap_or(f64::NAN);
        if advn > 0.0 && decn > 0.0 && advv > 0.0 && decv > 0.0 {
            trin[i] = (advn / decn) / (advv / decv);
        }
    }
    Ok(Series::new("trin".into(), trin))
}

/// Calculate McClellan Oscillator
/// Expects a DataFrame with 'advance' and 'decline' columns
pub fn calculate_mcclellan_oscillator(df: &DataFrame, advance_col: &str, decline_col: &str, fast: usize, slow: usize) -> PolarsResult<Series> {
    let advance = df.column(advance_col)?.f64()?;
    let decline = df.column(decline_col)?.f64()?;
    let len = df.height();
    let mut net_adv = vec![0.0; len];
    for i in 0..len {
        net_adv[i] = advance.get(i).unwrap_or(0.0) - decline.get(i).unwrap_or(0.0);
    }
    let mut fast_ema = vec![0.0; len];
    let mut slow_ema = vec![0.0; len];
    let alpha_fast = 2.0 / (fast as f64 + 1.0);
    let alpha_slow = 2.0 / (slow as f64 + 1.0);
    for i in 0..len {
        if i == 0 {
            fast_ema[i] = net_adv[i];
            slow_ema[i] = net_adv[i];
        } else {
            fast_ema[i] = alpha_fast * net_adv[i] + (1.0 - alpha_fast) * fast_ema[i-1];
            slow_ema[i] = alpha_slow * net_adv[i] + (1.0 - alpha_slow) * slow_ema[i-1];
        }
    }
    let mut mcclellan = vec![0.0; len];
    for i in 0..len {
        mcclellan[i] = fast_ema[i] - slow_ema[i];
    }
    Ok(Series::new("mcclellan_oscillator".into(), mcclellan))
} 