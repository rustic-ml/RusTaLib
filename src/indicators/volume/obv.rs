use polars::prelude::*;

/// Calculates On-Balance Volume (OBV)
///
/// # Arguments
///
/// * `df` - DataFrame containing the price and volume data
///
/// # Returns
///
/// Returns a PolarsResult containing the OBV Series
pub fn calculate_obv(df: &DataFrame) -> PolarsResult<Series> {
    // Check for required columns
    if !df.schema().contains("close") || !df.schema().contains("volume") {
        return Err(PolarsError::ComputeError(
            "OBV calculation requires both close and volume columns".into(),
        ));
    }

    let close = df.column("close")?.f64()?;
    let volume = df.column("volume")?.f64()?;

    let mut obv = Vec::with_capacity(df.height());

    // First value
    obv.push(volume.get(0).unwrap_or(0.0));

    for i in 1..df.height() {
        let curr_close = close.get(i).unwrap_or(0.0);
        let prev_close = close.get(i - 1).unwrap_or(0.0);
        let curr_volume = volume.get(i).unwrap_or(0.0);

        if curr_close > prev_close {
            obv.push(obv[i - 1] + curr_volume);
        } else if curr_close < prev_close {
            obv.push(obv[i - 1] - curr_volume);
        } else {
            // If prices are equal, OBV doesn't change
            obv.push(obv[i - 1]);
        }
    }

    Ok(Series::new("obv".into(), obv))
}

#[cfg(test)]
mod tests {
    use crate::indicators;

    use super::*;
    use indicators::test_util::create_test_ohlcv_df;

    #[test]
    fn test_calculate_obv_basic() {
        let df = create_test_ohlcv_df();
        let obv = calculate_obv(&df).unwrap();

        // OBV should have the same length as the dataframe
        assert_eq!(obv.len(), df.height());

        // First value should be equal to the first volume
        assert_eq!(obv.f64().unwrap().get(0).unwrap(), 1000.0);

        // Get actual values for verification
        let close = df.column("close").unwrap().f64().unwrap();
        let volume = df.column("volume").unwrap().f64().unwrap();
        
        // Manual check:
        // i=0: OBV = 1000
        let mut expected_obv = vec![1000.0];
        
        // Calculate expected OBV values
        for i in 1..7 {
            let curr_close = close.get(i).unwrap();
            let prev_close = close.get(i-1).unwrap();
            let curr_volume = volume.get(i).unwrap();
            
            if curr_close > prev_close {
                expected_obv.push(expected_obv[i-1] + curr_volume);
            } else if curr_close < prev_close {
                expected_obv.push(expected_obv[i-1] - curr_volume);
            } else {
                expected_obv.push(expected_obv[i-1]);
            }
        }
        
        // Verify each calculated value matches the expected value
        for i in 0..7 {
            assert_eq!(
                obv.f64().unwrap().get(i).unwrap(), 
                expected_obv[i],
                "OBV mismatch at index {}: expected {}, got {}", 
                i, 
                expected_obv[i], 
                obv.f64().unwrap().get(i).unwrap()
            );
        }
    }

    #[test]
    fn test_calculate_obv_equal_prices() {
        // Test case where prices are equal (no change in OBV)
        let close = Series::new("close".into(), &[10.0, 10.0, 10.0, 10.0]);
        let volume = Series::new("volume".into(), &[1000.0, 1500.0, 2000.0, 2500.0]);
        let df = DataFrame::new(vec![close.into(), volume.into()]).unwrap();

        let obv = calculate_obv(&df).unwrap();

        // First value should be equal to the first volume
        assert_eq!(obv.f64().unwrap().get(0).unwrap(), 1000.0);

        // Since all prices are equal, subsequent OBV values should remain the same
        for i in 1..df.height() {
            assert_eq!(obv.f64().unwrap().get(i).unwrap(), 1000.0);
        }
    }

    #[test]
    fn test_calculate_obv_edge_cases() {
        // Test with empty volume values
        let close = Series::new("close".into(), &[10.0, 12.0, 11.0, 13.0]);
        let volume = Series::new("volume".into(), &[0.0, 0.0, 0.0, 0.0]);
        let df = DataFrame::new(vec![close.into(), volume.into()]).unwrap();

        let obv = calculate_obv(&df).unwrap();

        // All OBV values should be zero
        for i in 0..df.height() {
            assert_eq!(obv.f64().unwrap().get(i).unwrap(), 0.0);
        }
    }

    #[test]
    #[should_panic(expected = "requires both close and volume columns")]
    fn test_calculate_obv_missing_columns() {
        // Test missing required columns
        let dummy = Series::new("dummy".into(), &[10.0, 12.0, 11.0, 13.0]);
        let df = DataFrame::new(vec![dummy.into()]).unwrap();

        // This should panic as we're missing close and volume columns
        let _ = calculate_obv(&df).unwrap();
    }

    #[test]
    fn test_obv_accuracy_with_precise_sequence() {
        // Create a specific test case with known expected outcomes
        let close = Series::new(
            "close".into(),
            &[
                10.0, // Initial price
                11.0, // Up
                12.0, // Up
                11.5, // Down
                11.5, // Equal
                12.5, // Up
                12.0, // Down
                12.0, // Equal
                11.0, // Down
                13.0, // Up
            ],
        );

        let volume = Series::new(
            "volume".into(),
            &[
                1000.0, 2000.0, 1500.0, 3000.0, 1000.0, 2500.0, 1800.0, 500.0, 3500.0, 4500.0,
            ],
        );

        let df = DataFrame::new(vec![close.into(), volume.into()]).unwrap();
        let obv = calculate_obv(&df).unwrap();
        let obv_ca = obv.f64().unwrap();

        // Manual calculation:
        // idx 0: OBV = 1000
        // idx 1: UP, OBV = 1000 + 2000 = 3000
        // idx 2: UP, OBV = 3000 + 1500 = 4500
        // idx 3: DOWN, OBV = 4500 - 3000 = 1500
        // idx 4: EQUAL, OBV = 1500 (no change)
        // idx 5: UP, OBV = 1500 + 2500 = 4000
        // idx 6: DOWN, OBV = 4000 - 1800 = 2200
        // idx 7: EQUAL, OBV = 2200 (no change)
        // idx 8: DOWN, OBV = 2200 - 3500 = -1300
        // idx 9: UP, OBV = -1300 + 4500 = 3200

        let expected_values = [
            1000.0, 3000.0, 4500.0, 1500.0, 1500.0, 4000.0, 2200.0, 2200.0, -1300.0, 3200.0,
        ];

        for i in 0..expected_values.len() {
            assert_eq!(obv_ca.get(i).unwrap(), expected_values[i]);
        }
    }

    #[test]
    fn test_obv_trend_detection() {
        // Test OBV's ability to detect trends
        // Create an uptrend where price and volume increase together
        let up_close = Series::new("close".into(), &[10.0, 11.0, 12.0, 13.0, 14.0, 15.0]);
        let up_volume = Series::new(
            "volume".into(),
            &[1000.0, 1200.0, 1400.0, 1600.0, 1800.0, 2000.0],
        );
        let up_df = DataFrame::new(vec![up_close.into(), up_volume.clone().into()]).unwrap();

        // Create a downtrend where price decreases but volume increases
        let down_close = Series::new("close".into(), &[15.0, 14.0, 13.0, 12.0, 11.0, 10.0]);
        let down_volume = Series::new(
            "volume".into(),
            &[1000.0, 1200.0, 1400.0, 1600.0, 1800.0, 2000.0],
        );
        let down_df = DataFrame::new(vec![down_close.into(), down_volume.clone().into()]).unwrap();

        let up_obv = calculate_obv(&up_df).unwrap();
        let down_obv = calculate_obv(&down_df).unwrap();

        // In an uptrend with increasing volume, OBV should consistently increase
        for i in 1..up_df.height() {
            assert!(
                up_obv.f64().unwrap().get(i).unwrap() > up_obv.f64().unwrap().get(i - 1).unwrap()
            );
        }

        // In a downtrend with increasing volume, OBV should consistently decrease
        for i in 1..down_df.height() {
            assert!(
                down_obv.f64().unwrap().get(i).unwrap()
                    < down_obv.f64().unwrap().get(i - 1).unwrap()
            );
        }

        // The last value of the uptrend OBV should be the sum of all volumes
        let up_total = up_volume.f64().unwrap().sum().unwrap();
        assert_eq!(
            up_obv.f64().unwrap().get(up_df.height() - 1).unwrap(),
            up_total
        );

        // The last value of the downtrend OBV should be the negative sum of all volumes (except the first)
        let down_total = -down_volume.f64().unwrap().sum().unwrap()
            + 2.0 * down_volume.f64().unwrap().get(0).unwrap();
        assert_eq!(
            down_obv.f64().unwrap().get(down_df.height() - 1).unwrap(),
            down_total
        );
    }
}
