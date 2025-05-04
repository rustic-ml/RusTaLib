use crate::indicators::{
    math::calculate_rate_of_change,
    moving_averages::{calculate_ema, calculate_sma},
    oscillators::{calculate_macd, calculate_rsi},
    volatility::{calculate_atr, calculate_bollinger_bands},
    volume::calculate_obv,
};
use polars::prelude::*;

/// Hybrid Adaptive Strategy Parameters
///
/// This strategy combines the best elements from strategies 1, 2, and 3
/// with additional refinements for optimized performance across different market conditions.
#[derive(Debug, Clone)]
pub struct StrategyParams {
    // Trend detection
    pub ema_short_period: usize,
    pub ema_mid_period: usize,
    pub ema_long_period: usize,
    pub sma_short_period: usize,
    pub sma_long_period: usize,

    // Mean reversion
    pub rsi_period: usize,
    pub rsi_overbought: f64,
    pub rsi_oversold: f64,

    // Volatility
    pub bb_period: usize,
    pub bb_std_dev: f64,
    pub atr_period: usize,
    pub atr_position_size_factor: f64,

    // Momentum
    pub macd_fast: usize,
    pub macd_slow: usize,
    pub macd_signal: usize,
    pub roc_period: usize,

    // Volume
    pub obv_ema_period: usize,
    pub volume_threshold: f64,

    // Signal thresholds
    pub min_signals_for_buy: usize,
    pub min_signals_for_sell: usize,

    // Risk management
    pub stop_loss_atr_multiple: f64,
    pub take_profit_atr_multiple: f64,
    pub trailing_stop_enabled: bool,
    pub trailing_stop_atr_multiple: f64,
    pub max_position_size_pct: f64,
}

impl Default for StrategyParams {
    fn default() -> Self {
        Self {
            // Trend detection
            ema_short_period: 8,
            ema_mid_period: 21,
            ema_long_period: 50,
            sma_short_period: 10,
            sma_long_period: 50,

            // Mean reversion
            rsi_period: 14,
            rsi_overbought: 70.0,
            rsi_oversold: 30.0,

            // Volatility
            bb_period: 20,
            bb_std_dev: 2.0,
            atr_period: 14,
            atr_position_size_factor: 2.0,

            // Momentum
            macd_fast: 12,
            macd_slow: 26,
            macd_signal: 9,
            roc_period: 10,

            // Volume
            obv_ema_period: 20,
            volume_threshold: 1.3,

            // Signal thresholds
            min_signals_for_buy: 3,
            min_signals_for_sell: 3,

            // Risk management
            stop_loss_atr_multiple: 3.0,
            take_profit_atr_multiple: 4.5,
            trailing_stop_enabled: true,
            trailing_stop_atr_multiple: 2.5,
            max_position_size_pct: 0.25, // 25% of capital max per position
        }
    }
}

/// Strategy signals for backtesting
pub struct StrategySignals {
    pub buy_signals: Vec<i32>,
    pub sell_signals: Vec<i32>,
    pub stop_signals: Vec<i32>,
    pub take_profit_signals: Vec<i32>,
    pub position_sizes: Vec<f64>,
    pub indicator_values: DataFrame,
}

/// Run the hybrid adaptive strategy on the provided DataFrame
///
/// This strategy combines adaptive trend following, volatility-based position sizing,
/// and dynamic signal thresholds based on market conditions.
pub fn run_strategy(
    df: &DataFrame,
    params: &StrategyParams,
) -> Result<StrategySignals, PolarsError> {
    // Calculate indicator values
    let ema_short = calculate_ema(df, "close", params.ema_short_period)?;
    let ema_mid = calculate_ema(df, "close", params.ema_mid_period)?;
    let ema_long = calculate_ema(df, "close", params.ema_long_period)?;

    let sma_short = calculate_sma(df, "close", params.sma_short_period)?;
    let sma_long = calculate_sma(df, "close", params.sma_long_period)?;

    let rsi = calculate_rsi(df, params.rsi_period, "close")?;

    let (bb_upper, bb_middle, bb_lower) =
        calculate_bollinger_bands(df, params.bb_period, params.bb_std_dev, "close")?;

    let (macd, macd_signal) = calculate_macd(
        df,
        params.macd_fast,
        params.macd_slow,
        params.macd_signal,
        "close",
    )?;

    let atr = calculate_atr(df, params.atr_period)?;
    let obv = calculate_obv(df)?;
    let roc = calculate_rate_of_change(df, "close", params.roc_period)?;

    // Calculate OBV EMA for relative strength of volume
    let obv_df = DataFrame::new(vec![obv.clone().into()])?;
    let obv_ema = calculate_ema(&obv_df, "obv", params.obv_ema_period)?;

    // Calculate volume moving average for relative volume
    let volume_sma = calculate_sma(df, "volume", 20)?;

    // Extract values for calculations
    let close = df.column("close")?.f64()?;
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let volume = df.column("volume")?.f64()?;

    // Clone for access in the iterator
    let ema_short_cloned = ema_short.clone();
    let ema_short_vals = ema_short_cloned.f64()?;

    let ema_mid_cloned = ema_mid.clone();
    let ema_mid_vals = ema_mid_cloned.f64()?;

    let ema_long_cloned = ema_long.clone();
    let ema_long_vals = ema_long_cloned.f64()?;

    let sma_short_cloned = sma_short.clone();
    let sma_short_vals = sma_short_cloned.f64()?;

    let sma_long_cloned = sma_long.clone();
    let sma_long_vals = sma_long_cloned.f64()?;

    let rsi_cloned = rsi.clone();
    let rsi_vals = rsi_cloned.f64()?;

    let bb_upper_cloned = bb_upper.clone();
    let bb_upper_vals = bb_upper_cloned.f64()?;

    let bb_lower_cloned = bb_lower.clone();
    let bb_lower_vals = bb_lower_cloned.f64()?;

    let bb_middle_cloned = bb_middle.clone();
    let bb_middle_vals = bb_middle_cloned.f64()?;

    let macd_cloned = macd.clone();
    let macd_vals = macd_cloned.f64()?;

    let macd_signal_cloned = macd_signal.clone();
    let macd_signal_vals = macd_signal_cloned.f64()?;

    let atr_cloned = atr.clone();
    let atr_vals = atr_cloned.f64()?;

    let obv_cloned = obv.clone();
    let obv_vals = obv_cloned.f64()?;

    let obv_ema_cloned = obv_ema.clone();
    let obv_ema_vals = obv_ema_cloned.f64()?;

    let volume_sma_cloned = volume_sma.clone();
    let volume_sma_vals = volume_sma_cloned.f64()?;

    let roc_cloned = roc.clone();
    let roc_vals = roc_cloned.f64()?;

    // Create arrays for signals
    let mut buy_signals = Vec::with_capacity(df.height());
    let mut sell_signals = Vec::with_capacity(df.height());
    let mut stop_signals = Vec::with_capacity(df.height());
    let mut take_profit_signals = Vec::with_capacity(df.height());
    let mut position_sizes = Vec::with_capacity(df.height());

    // Position tracking
    let mut is_in_position = false;
    let mut entry_price = 0.0;
    let mut highest_price_since_entry = 0.0;

    // The maximum window size needed
    let max_window = params
        .ema_long_period
        .max(params.sma_long_period)
        .max(params.macd_slow + params.macd_signal)
        .max(params.bb_period)
        .max(params.atr_period)
        .max(params.obv_ema_period)
        .max(params.roc_period)
        .max(20); // For volume SMA

    // Fill the first max_window elements with 0/default values
    for _ in 0..max_window {
        buy_signals.push(0);
        sell_signals.push(0);
        stop_signals.push(0);
        take_profit_signals.push(0);
        position_sizes.push(0.0);
    }

    // Main strategy logic
    for i in max_window..df.height() {
        // Skip if we don't have valid values for any needed indicator
        if ema_short_vals.get(i).is_none()
            || ema_mid_vals.get(i).is_none()
            || ema_long_vals.get(i).is_none()
            || sma_short_vals.get(i).is_none()
            || sma_long_vals.get(i).is_none()
            || rsi_vals.get(i).is_none()
            || bb_upper_vals.get(i).is_none()
            || bb_lower_vals.get(i).is_none()
            || macd_vals.get(i).is_none()
            || macd_signal_vals.get(i).is_none()
            || atr_vals.get(i).is_none()
            || obv_vals.get(i).is_none()
            || obv_ema_vals.get(i).is_none()
            || volume_sma_vals.get(i).is_none()
            || roc_vals.get(i).is_none()
        {
            buy_signals.push(0);
            sell_signals.push(0);
            stop_signals.push(0);
            take_profit_signals.push(0);
            position_sizes.push(0.0);
            continue;
        }

        // Extract current values
        let price = close.get(i).unwrap_or(0.0);
        let high_price = high.get(i).unwrap_or(0.0);
        let low_price = low.get(i).unwrap_or(0.0);
        let current_volume = volume.get(i).unwrap_or(0.0);

        // Extract indicator values
        let ema_short_val = ema_short_vals.get(i).unwrap_or(0.0);
        let ema_mid_val = ema_mid_vals.get(i).unwrap_or(0.0);
        let ema_long_val = ema_long_vals.get(i).unwrap_or(0.0);
        let sma_short_val = sma_short_vals.get(i).unwrap_or(0.0);
        let sma_long_val = sma_long_vals.get(i).unwrap_or(0.0);
        let rsi_val = rsi_vals.get(i).unwrap_or(0.0);
        let bb_upper_val = bb_upper_vals.get(i).unwrap_or(0.0);
        let bb_lower_val = bb_lower_vals.get(i).unwrap_or(0.0);
        let bb_middle_val = bb_middle_vals.get(i).unwrap_or(0.0);
        let macd_val = macd_vals.get(i).unwrap_or(0.0);
        let macd_signal_val = macd_signal_vals.get(i).unwrap_or(0.0);
        let atr_val = atr_vals.get(i).unwrap_or(0.0);
        let obv_val = obv_vals.get(i).unwrap_or(0.0);
        let obv_ema_val = obv_ema_vals.get(i).unwrap_or(0.0);
        let avg_volume = volume_sma_vals.get(i).unwrap_or(1.0);
        let roc_val = roc_vals.get(i).unwrap_or(0.0);

        // Previous values
        let prev_price = if i > 0 {
            close.get(i - 1).unwrap_or(price)
        } else {
            price
        };
        let prev_ema_short = if i > 0 {
            ema_short_vals.get(i - 1).unwrap_or(0.0)
        } else {
            0.0
        };
        let prev_ema_mid = if i > 0 {
            ema_mid_vals.get(i - 1).unwrap_or(0.0)
        } else {
            0.0
        };
        let prev_sma_short = if i > 0 {
            sma_short_vals.get(i - 1).unwrap_or(0.0)
        } else {
            0.0
        };
        let prev_sma_long = if i > 0 {
            sma_long_vals.get(i - 1).unwrap_or(0.0)
        } else {
            0.0
        };
        let prev_macd = if i > 0 {
            macd_vals.get(i - 1).unwrap_or(0.0)
        } else {
            0.0
        };
        let prev_macd_signal = if i > 0 {
            macd_signal_vals.get(i - 1).unwrap_or(0.0)
        } else {
            0.0
        };
        let prev_rsi = if i > 0 {
            rsi_vals.get(i - 1).unwrap_or(50.0)
        } else {
            50.0
        };
        let prev_obv = if i > 0 {
            obv_vals.get(i - 1).unwrap_or(0.0)
        } else {
            0.0
        };
        let prev_roc = if i > 0 {
            roc_vals.get(i - 1).unwrap_or(0.0)
        } else {
            0.0
        };

        // Market condition analysis
        // Trend detection
        let bullish_trend_ema = ema_short_val > ema_mid_val && ema_mid_val > ema_long_val;
        let bearish_trend_ema = ema_short_val < ema_mid_val && ema_mid_val < ema_long_val;
        let bullish_trend_sma = sma_short_val > sma_long_val;
        let bearish_trend_sma = sma_short_val < sma_long_val;

        // Trend strength
        let trend_strength = (ema_short_val - ema_long_val).abs() / ema_long_val * 100.0;
        let strong_trend = trend_strength > 2.5; // More than 2.5% difference

        // Volatility analysis
        let high_volatility = atr_val > (price * 0.015); // ATR more than 1.5% of price
        let price_momentum = (price - prev_price) / prev_price * 100.0;
        let strong_momentum = price_momentum.abs() > 1.0; // More than 1% price change

        // Volume analysis
        let high_relative_volume = current_volume > (avg_volume * params.volume_threshold);
        let obv_rising = obv_val > obv_ema_val && obv_val > prev_obv;
        let obv_falling = obv_val < obv_ema_val && obv_val < prev_obv;

        // Momentum conditions
        let accelerating_momentum = roc_val > prev_roc;
        let decelerating_momentum = roc_val < prev_roc;

        // Divergence detection
        let bullish_price_div = price < prev_price && macd_val > prev_macd;
        let bearish_price_div = price > prev_price && macd_val < prev_macd;

        // Mean reversion conditions
        let oversold = rsi_val < params.rsi_oversold;
        let overbought = rsi_val > params.rsi_overbought;
        let rsi_rising = rsi_val > prev_rsi;
        let rsi_falling = rsi_val < prev_rsi;
        let price_at_bb_lower = price <= bb_lower_val;
        let price_at_bb_upper = price >= bb_upper_val;
        let price_near_bb_middle = (price - bb_middle_val).abs() / bb_middle_val < 0.005; // Within 0.5% of middle band

        // Trend reversal signals
        let ema_short_cross_above_mid =
            ema_short_val > ema_mid_val && prev_ema_short <= prev_ema_mid;
        let ema_short_cross_below_mid =
            ema_short_val < ema_mid_val && prev_ema_short >= prev_ema_mid;
        let sma_cross_up = sma_short_val > sma_long_val && prev_sma_short <= prev_sma_long;
        let sma_cross_down = sma_short_val < sma_long_val && prev_sma_short >= prev_sma_long;

        // MACD crossover
        let macd_cross_up = macd_val > macd_signal_val && prev_macd <= prev_macd_signal;
        let macd_cross_down = macd_val < macd_signal_val && prev_macd >= prev_macd_signal;

        // Risk management calculations
        let mut stop_loss_hit = false;
        let mut take_profit_hit = false;
        let mut trailing_stop_hit = false;

        if is_in_position {
            // Update highest price since entry if needed
            if price > highest_price_since_entry {
                highest_price_since_entry = price;
            }

            // Calculate stop loss and take profit levels
            let stop_loss_level = entry_price - (params.stop_loss_atr_multiple * atr_val);
            let take_profit_level = entry_price + (params.take_profit_atr_multiple * atr_val);

            // Trailing stop calculation
            let trailing_stop_level = if params.trailing_stop_enabled {
                highest_price_since_entry - (params.trailing_stop_atr_multiple * atr_val)
            } else {
                0.0
            };

            // Check if any exit conditions met
            stop_loss_hit = low_price <= stop_loss_level;
            take_profit_hit = high_price >= take_profit_level;
            trailing_stop_hit = params.trailing_stop_enabled
                && (low_price <= trailing_stop_level)
                && (trailing_stop_level > stop_loss_level);
        }

        // Calculate buy/sell scores based on our signals
        let mut buy_score: i32 = 0;
        let mut sell_score: i32 = 0;

        // Base signals weighted by market conditions
        // Buy signals
        if bullish_trend_ema {
            buy_score += 1;
        }
        if bullish_trend_sma {
            buy_score += 1;
        }
        if ema_short_cross_above_mid {
            buy_score += 1;
        }
        if sma_cross_up {
            buy_score += 1;
        }
        if strong_trend && bullish_trend_ema {
            buy_score += 1;
        }
        if oversold && rsi_rising {
            buy_score += 1;
        }
        if price_at_bb_lower && (bullish_trend_ema || bullish_trend_sma) {
            buy_score += 1;
        }
        if macd_cross_up {
            buy_score += 1;
        }
        if obv_rising && high_relative_volume {
            buy_score += 1;
        }
        if bullish_price_div {
            buy_score += 1;
        }
        if accelerating_momentum && price_momentum > 0.0 {
            buy_score += 1;
        }

        // Sell signals
        if bearish_trend_ema {
            sell_score += 1;
        }
        if bearish_trend_sma {
            sell_score += 1;
        }
        if ema_short_cross_below_mid {
            sell_score += 1;
        }
        if sma_cross_down {
            sell_score += 1;
        }
        if strong_trend && bearish_trend_ema {
            sell_score += 1;
        }
        if overbought && rsi_falling {
            sell_score += 1;
        }
        if price_at_bb_upper && (bearish_trend_ema || bearish_trend_sma) {
            sell_score += 1;
        }
        if macd_cross_down {
            sell_score += 1;
        }
        if obv_falling && high_relative_volume {
            sell_score += 1;
        }
        if bearish_price_div {
            sell_score += 1;
        }
        if decelerating_momentum && price_momentum < 0.0 {
            sell_score += 1;
        }

        // Dynamic adjustment based on market condition
        if high_volatility {
            // In high volatility, be more conservative with entries
            let _ = params.min_signals_for_buy.max(3);

            // And more aggressive with exits if position is not in favor
            if is_in_position && price < entry_price {
                sell_score += 1;
            }
        } else {
            // In low volatility, we can be more nuanced
            if price_near_bb_middle && !strong_trend {
                // Lower conviction in ranging markets
                buy_score = buy_score.saturating_sub(1);
                sell_score = sell_score.saturating_sub(1);
            }
        }

        // Determine final signals
        let final_buy_signal = !is_in_position && buy_score >= params.min_signals_for_buy as i32;
        let final_sell_signal = is_in_position
            && (sell_score >= params.min_signals_for_sell as i32
                || stop_loss_hit
                || take_profit_hit
                || trailing_stop_hit);

        // Position sizing based on ATR and volatility
        let position_size_pct = if high_volatility {
            // In high volatility, reduce position size
            0.75 * params.max_position_size_pct
        } else {
            params.max_position_size_pct
        };

        // Position sizing based on ATR
        let atr_position_size =
            position_size_pct / (params.atr_position_size_factor * atr_val / price);

        // Apply final signals
        if final_buy_signal {
            buy_signals.push(1);
            sell_signals.push(0);
            stop_signals.push(0);
            take_profit_signals.push(0);
            position_sizes.push(atr_position_size.min(params.max_position_size_pct));

            is_in_position = true;
            entry_price = price;
            highest_price_since_entry = price;
        } else if final_sell_signal {
            buy_signals.push(0);
            sell_signals.push(1);

            // Record the reason for the exit
            stop_signals.push(if stop_loss_hit { 1 } else { 0 });
            take_profit_signals.push(if take_profit_hit { 1 } else { 0 });

            // If neither stop loss nor take profit, it's a trailing stop or signal-based exit
            position_sizes.push(0.0);

            is_in_position = false;
        } else {
            // No change
            buy_signals.push(0);
            sell_signals.push(0);
            stop_signals.push(0);
            take_profit_signals.push(0);

            // Keep position size if in a position
            position_sizes.push(if is_in_position {
                position_sizes[i - 1]
            } else {
                0.0
            });
        }
    }

    // Create indicator DataFrame for analysis
    let mut indicator_columns: Vec<Series> = Vec::new();

    // Extract values as vectors
    let ema_short_vec: Vec<f64> = ema_short_vals
        .iter()
        .map(|v| v.unwrap_or(f64::NAN))
        .collect();
    let ema_mid_vec: Vec<f64> = ema_mid_vals.iter().map(|v| v.unwrap_or(f64::NAN)).collect();
    let ema_long_vec: Vec<f64> = ema_long_vals
        .iter()
        .map(|v| v.unwrap_or(f64::NAN))
        .collect();
    let sma_short_vec: Vec<f64> = sma_short_vals
        .iter()
        .map(|v| v.unwrap_or(f64::NAN))
        .collect();
    let sma_long_vec: Vec<f64> = sma_long_vals
        .iter()
        .map(|v| v.unwrap_or(f64::NAN))
        .collect();
    let rsi_vec: Vec<f64> = rsi_vals.iter().map(|v| v.unwrap_or(f64::NAN)).collect();
    let bb_upper_vec: Vec<f64> = bb_upper_vals
        .iter()
        .map(|v| v.unwrap_or(f64::NAN))
        .collect();
    let bb_middle_vec: Vec<f64> = bb_middle_vals
        .iter()
        .map(|v| v.unwrap_or(f64::NAN))
        .collect();
    let bb_lower_vec: Vec<f64> = bb_lower_vals
        .iter()
        .map(|v| v.unwrap_or(f64::NAN))
        .collect();
    let macd_vec: Vec<f64> = macd_vals.iter().map(|v| v.unwrap_or(f64::NAN)).collect();
    let macd_signal_vec: Vec<f64> = macd_signal_vals
        .iter()
        .map(|v| v.unwrap_or(f64::NAN))
        .collect();
    let atr_vec: Vec<f64> = atr_vals.iter().map(|v| v.unwrap_or(f64::NAN)).collect();
    let obv_vec: Vec<f64> = obv_vals.iter().map(|v| v.unwrap_or(f64::NAN)).collect();
    let obv_ema_vec: Vec<f64> = obv_ema_vals.iter().map(|v| v.unwrap_or(f64::NAN)).collect();
    let volume_sma_vec: Vec<f64> = volume_sma_vals
        .iter()
        .map(|v| v.unwrap_or(f64::NAN))
        .collect();
    let roc_vec: Vec<f64> = roc_vals.iter().map(|v| v.unwrap_or(f64::NAN)).collect();

    // Add all indicator columns
    indicator_columns.push(Series::new("ema_short".into(), ema_short_vec));
    indicator_columns.push(Series::new("ema_mid".into(), ema_mid_vec));
    indicator_columns.push(Series::new("ema_long".into(), ema_long_vec));
    indicator_columns.push(Series::new("sma_short".into(), sma_short_vec));
    indicator_columns.push(Series::new("sma_long".into(), sma_long_vec));
    indicator_columns.push(Series::new("rsi".into(), rsi_vec));
    indicator_columns.push(Series::new("bb_upper".into(), bb_upper_vec));
    indicator_columns.push(Series::new("bb_middle".into(), bb_middle_vec));
    indicator_columns.push(Series::new("bb_lower".into(), bb_lower_vec));
    indicator_columns.push(Series::new("macd".into(), macd_vec));
    indicator_columns.push(Series::new("macd_signal".into(), macd_signal_vec));
    indicator_columns.push(Series::new("atr".into(), atr_vec));
    indicator_columns.push(Series::new("obv".into(), obv_vec));
    indicator_columns.push(Series::new("obv_ema".into(), obv_ema_vec));
    indicator_columns.push(Series::new("volume_sma".into(), volume_sma_vec));
    indicator_columns.push(Series::new("roc".into(), roc_vec));
    indicator_columns.push(Series::new("buy_signals".into(), &buy_signals));
    indicator_columns.push(Series::new("sell_signals".into(), &sell_signals));
    indicator_columns.push(Series::new("stop_signals".into(), &stop_signals));
    indicator_columns.push(Series::new(
        "take_profit_signals".into(),
        &take_profit_signals,
    ));
    indicator_columns.push(Series::new("position_sizes".into(), &position_sizes));

    // Create DataFrame from indicator columns
    let indicator_df = DataFrame::from_iter(indicator_columns);

    Ok(StrategySignals {
        buy_signals,
        sell_signals,
        stop_signals,
        take_profit_signals,
        position_sizes,
        indicator_values: indicator_df,
    })
}

/// Calculate performance metrics for the strategy
pub fn calculate_performance(
    close_prices: &Column,
    buy_signals: &[i32],
    sell_signals: &[i32],
    position_sizes: &[f64],
    start_capital: f64,
) -> (f64, f64, usize, f64, f64, f64) {
    let mut capital = start_capital;
    let mut peak_capital = start_capital;
    let mut max_drawdown: f64 = 0.0;
    let mut shares_held = 0.0;
    let mut entry_price = 0.0;
    let mut in_position = false;

    let mut num_trades = 0;
    let mut wins = 0;

    let mut total_profit = 0.0;
    let mut total_loss = 0.0;

    let prices = close_prices.f64().unwrap();

    for i in 0..buy_signals.len() {
        let current_price = prices.get(i).unwrap_or(0.0);

        // Update position value if in a position
        if in_position {
            let position_value = shares_held * current_price;

            // Track drawdown
            let current_total = capital + position_value;
            if current_total > peak_capital {
                peak_capital = current_total;
            } else {
                let drawdown = (peak_capital - current_total) / peak_capital;
                max_drawdown = max_drawdown.max(drawdown);
            }
        }

        // Buy signal
        if buy_signals[i] == 1 && !in_position {
            // Calculate position size based on the position_sizes array
            let position_size = position_sizes[i];
            let position_capital = capital * position_size;

            entry_price = current_price;
            shares_held = position_capital / current_price;
            capital -= position_capital;
            in_position = true;
        }

        // Sell signal
        if sell_signals[i] == 1 && in_position {
            let exit_price = current_price;
            let _trade_return = (exit_price - entry_price) / entry_price;

            // Calculate P&L
            let exit_position_value = shares_held * exit_price;
            capital += exit_position_value;

            // Track trade statistics
            num_trades += 1;
            if exit_price > entry_price {
                wins += 1;
                total_profit += exit_position_value - (shares_held * entry_price);
            } else {
                total_loss += (shares_held * entry_price) - exit_position_value;
            }

            // Reset position tracking
            shares_held = 0.0;
            in_position = false;
        }
    }

    // Add final position value to capital
    if in_position {
        let last_price = prices.get(prices.len() - 1).unwrap_or(0.0);
        capital += shares_held * last_price;
    }

    // Calculate performance metrics
    let total_return = (capital / start_capital - 1.0) * 100.0;
    let win_rate = if num_trades > 0 {
        (wins as f64 / num_trades as f64) * 100.0
    } else {
        0.0
    };
    let profit_factor = if total_loss > 0.0 {
        total_profit / total_loss
    } else {
        if total_profit > 0.0 {
            f64::MAX
        } else {
            0.0
        }
    };

    (
        capital,
        total_return,
        num_trades,
        win_rate,
        max_drawdown,
        profit_factor,
    )
}
