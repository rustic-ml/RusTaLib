use crate::util::dataframe_utils::check_window_size;
use polars::prelude::*;

/// Calculates Chande Momentum Oscillator (CMO)
/// Formula: CMO = 100 * ((Sum of gains - Sum of losses) / (Sum of gains + Sum of losses))
///
/// The CMO indicator is similar to other momentum oscillators but has a different formula.
/// It oscillates between -100 and +100, with overbought/oversold typically at +/-50.
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `window` - Window size for CMO calculation (typically 14)
/// * `column` - Column name to use for calculations (default "close")
///
/// # Returns
///
/// Returns a PolarsResult containing the CMO Series
pub fn calculate_cmo(df: &DataFrame, window: usize, column: &str) -> PolarsResult<Series> {
    check_window_size(df, window, "CMO")?;

    let price = df.column(column)?.f64()?;
    let mut cmo_values = Vec::with_capacity(df.height());

    // Fill initial values with NaN
    for _ in 0..window {
        cmo_values.push(f64::NAN);
    }

    if df.height() <= window {
        return Ok(Series::new("cmo".into(), cmo_values));
    }

    // Calculate price changes
    let mut changes = Vec::with_capacity(df.height() - 1);

    for i in 1..df.height() {
        let current = price.get(i).unwrap_or(f64::NAN);
        let previous = price.get(i - 1).unwrap_or(f64::NAN);

        if !current.is_nan() && !previous.is_nan() {
            changes.push(current - previous);
        } else {
            changes.push(f64::NAN);
        }
    }

    // Calculate CMO for each window
    for i in window..df.height() {
        let mut sum_gains = 0.0;
        let mut sum_losses = 0.0;
        let mut valid_periods = 0;

        // Sum up gains and losses in the window
        for j in (i - window)..(i) {
            if j >= changes.len() {
                continue;
            }

            let change = changes[j];
            if !change.is_nan() {
                valid_periods += 1;
                if change > 0.0 {
                    sum_gains += change;
                } else if change < 0.0 {
                    sum_losses += change.abs();
                }
            }
        }

        // Calculate CMO
        if valid_periods > 0 && (sum_gains + sum_losses) > 0.0 {
            let cmo = 100.0 * ((sum_gains - sum_losses) / (sum_gains + sum_losses));
            cmo_values.push(cmo);
        } else {
            cmo_values.push(f64::NAN);
        }
    }

    Ok(Series::new("cmo".into(), cmo_values))
}
