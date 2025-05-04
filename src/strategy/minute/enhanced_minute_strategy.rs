use crate::indicators::{
    moving_averages::{calculate_ema, calculate_sma},
    oscillators::{calculate_rsi, calculate_stochastic, calculate_williams_r},
    trend::calculate_psar,
    volatility::{calculate_atr, calculate_bollinger_bands},
    volume::{calculate_cmf, calculate_mfi, calculate_obv},
};
use polars::prelude::*;

/// Strategy parameters for an enhanced minute-based multi-indicator strategy
///
/// This strategy combines several specialized intraday indicators to provide
/// more effective processing and trading signals for minute-level data.
///
/// See the example at `examples/enhanced_minute_strategy_example.rs` for a full demonstration of how to use this strategy.
/// The example saves all signals and indicators to `enhanced_minute_strategy_results.csv` for further analysis.
#[derive(Clone)]
pub struct StrategyParams {
    /// Period for fast EMA
    pub ema_fast_period: usize,

    /// Period for slow EMA
    pub ema_slow_period: usize,

    /// Period for RSI calculation
    pub rsi_period: usize,

    /// RSI overbought threshold
    pub rsi_overbought: f64,

    /// RSI oversold threshold
    pub rsi_oversold: f64,

    /// Period for Williams %R
    pub williams_r_period: usize,

    /// Period for Stochastic %K
    pub stoch_k_period: usize,

    /// Period for Stochastic %D
    pub stoch_d_period: usize,

    /// Slowing period for Stochastic
    pub stoch_slowing: usize,

    /// Acceleration factor step for Parabolic SAR
    pub psar_af_step: f64,

    /// Maximum acceleration factor for Parabolic SAR
    pub psar_af_max: f64,

    /// Period for ATR calculation
    pub atr_period: usize,

    /// ATR multiplier for stop loss
    pub atr_stop_multiplier: f64,

    /// ATR multiplier for take profit
    pub atr_profit_multiplier: f64,

    /// Period for Bollinger Bands
    pub bb_period: usize,

    /// Standard deviation for Bollinger Bands
    pub bb_std_dev: f64,

    /// Period for Money Flow Index (MFI)
    pub mfi_period: usize,

    /// Period for Chaikin Money Flow (CMF)
    pub cmf_period: usize,

    /// Minimum number of signals required for buy entry
    pub min_buy_signals: usize,

    /// Minimum number of signals required for sell entry
    pub min_sell_signals: usize,

    /// Whether to use volume filtering (require above average volume for entries)
    pub use_volume_filter: bool,

    /// Volume threshold as a percentage of average volume
    pub volume_threshold: f64,

    /// Whether to use time-based filters (avoid trading during certain times)
    pub use_time_filter: bool,

    /// Filter out early morning periods (first N minutes of trading day)
    pub filter_morning_minutes: usize,

    /// Filter out lunch hour periods
    pub filter_lunch_hour: bool,

    /// Filter out late day periods (last N minutes of trading day)
    pub filter_late_day_minutes: usize,
}

impl Default for StrategyParams {
    fn default() -> Self {
        Self {
            ema_fast_period: 8,
            ema_slow_period: 21,
            rsi_period: 7,
            rsi_overbought: 70.0,
            rsi_oversold: 30.0,
            williams_r_period: 14,
            stoch_k_period: 14,
            stoch_d_period: 3,
            stoch_slowing: 3,
            psar_af_step: 0.02,
            psar_af_max: 0.2,
            atr_period: 14,
            atr_stop_multiplier: 2.0,
            atr_profit_multiplier: 3.0,
            bb_period: 20,
            bb_std_dev: 2.0,
            mfi_period: 14,
            cmf_period: 20,
            min_buy_signals: 3,
            min_sell_signals: 3,
            use_volume_filter: true,
            volume_threshold: 1.2,
            use_time_filter: true,
            filter_morning_minutes: 15,
            filter_lunch_hour: true,
            filter_late_day_minutes: 15,
        }
    }
}

/// Strategy signals with risk management
pub struct StrategySignals {
    /// Buy signals (1 for buy, 0 for no signal)
    pub buy_signals: Vec<i32>,

    /// Sell signals (1 for sell, 0 for no signal)
    pub sell_signals: Vec<i32>,

    /// Stop loss levels for each position
    pub stop_levels: Vec<f64>,

    /// Take profit levels for each position
    pub target_levels: Vec<f64>,

    /// DataFrame with all indicators and signals
    pub indicator_values: DataFrame,
}

/// Run the enhanced minute-based multi-indicator strategy
///
/// This function implements a comprehensive intraday trading strategy that
/// combines multiple indicators optimized for minute-level data processing.
/// It focuses on quick signal generation with effective risk management.
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLCV data with columns "open", "high", "low", "close", "volume", and optionally "time"
/// * `params` - Strategy parameters
///
/// # Returns
///
/// * `Result<StrategySignals, PolarsError>` - Strategy signals with risk management levels
pub fn run_strategy(
    df: &DataFrame,
    params: &StrategyParams,
) -> Result<StrategySignals, PolarsError> {
    // Calculate technical indicators
    let ema_fast = calculate_ema(df, "close", params.ema_fast_period)?;
    let ema_slow = calculate_ema(df, "close", params.ema_slow_period)?;
    let rsi = calculate_rsi(df, params.rsi_period, "close")?;
    let williams_r = calculate_williams_r(df, params.williams_r_period)?;
    let (stoch_k, stoch_d) = calculate_stochastic(
        df,
        params.stoch_k_period,
        params.stoch_d_period,
        params.stoch_slowing,
    )?;
    let psar = calculate_psar(df, params.psar_af_step, params.psar_af_max)?;
    let atr = calculate_atr(df, params.atr_period)?;
    let (bb_middle, bb_upper, bb_lower) =
        calculate_bollinger_bands(df, params.bb_period, params.bb_std_dev, "close")?;
    let mfi = calculate_mfi(df, params.mfi_period)?;
    let cmf = calculate_cmf(df, params.cmf_period)?;
    let obv = calculate_obv(df)?;

    // Calculate volume average for filtering
    let volume_sma = calculate_sma(df, "volume", 20)?;

    // Extract values for calculations
    let close = df.column("close")?.f64()?;
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let volume = df.column("volume")?.f64()?;

    // Clone series for fast access
    let ema_fast_cloned = ema_fast.clone();
    let ema_fast_vals = ema_fast_cloned.f64()?;

    let ema_slow_cloned = ema_slow.clone();
    let ema_slow_vals = ema_slow_cloned.f64()?;

    let rsi_cloned = rsi.clone();
    let rsi_vals = rsi_cloned.f64()?;

    let williams_r_cloned = williams_r.clone();
    let williams_r_vals = williams_r_cloned.f64()?;

    let stoch_k_cloned = stoch_k.clone();
    let stoch_k_vals = stoch_k_cloned.f64()?;

    let stoch_d_cloned = stoch_d.clone();
    let stoch_d_vals = stoch_d_cloned.f64()?;

    let psar_cloned = psar.clone();
    let psar_vals = psar_cloned.f64()?;

    let atr_cloned = atr.clone();
    let atr_vals = atr_cloned.f64()?;

    let bb_upper_cloned = bb_upper.clone();
    let bb_upper_vals = bb_upper_cloned.f64()?;

    let bb_lower_cloned = bb_lower.clone();
    let bb_lower_vals = bb_lower_cloned.f64()?;

    let bb_middle_cloned = bb_middle.clone();
    let bb_middle_vals = bb_middle_cloned.f64()?;

    let mfi_cloned = mfi.clone();
    let mfi_vals = mfi_cloned.f64()?;

    let cmf_cloned = cmf.clone();
    let cmf_vals = cmf_cloned.f64()?;

    let obv_cloned = obv.clone();
    let obv_vals = obv_cloned.f64()?;

    let volume_sma_cloned = volume_sma.clone();
    let volume_sma_vals = volume_sma_cloned.f64()?;

    // Create arrays for signals and levels
    let mut buy_signals = Vec::with_capacity(df.height());
    let mut sell_signals = Vec::with_capacity(df.height());
    let mut stop_levels = Vec::with_capacity(df.height());
    let mut target_levels = Vec::with_capacity(df.height());

    // Position tracking
    let mut in_position = false;
    let mut entry_price = 0.0;

    // Determine initial window to skip (need enough data for all indicators)
    let max_window = params
        .ema_slow_period
        .max(params.atr_period)
        .max(params.bb_period)
        .max(params.stoch_k_period + params.stoch_d_period + params.stoch_slowing)
        .max(params.mfi_period)
        .max(params.cmf_period)
        .max(20); // For volume SMA

    // Fill initial values
    for _ in 0..max_window {
        buy_signals.push(0);
        sell_signals.push(0);
        stop_levels.push(0.0);
        target_levels.push(0.0);
    }

    // Check for time column (for time-based filtering)
    let has_time_column = df.schema().contains("time");
    let time_series = if has_time_column {
        Some(df.column("time")?)
    } else {
        None
    };

    // Process each bar after the initial window
    for i in max_window..df.height() {
        // Extract current values
        let price = close.get(i).unwrap_or(f64::NAN);
        let high_val = high.get(i).unwrap_or(f64::NAN);
        let low_val = low.get(i).unwrap_or(f64::NAN);
        let vol = volume.get(i).unwrap_or(f64::NAN);

        // Skip if any essential value is NaN
        if price.is_nan() || high_val.is_nan() || low_val.is_nan() || vol.is_nan() {
            buy_signals.push(0);
            sell_signals.push(0);
            stop_levels.push(0.0);
            target_levels.push(0.0);
            continue;
        }

        // Extract indicator values
        let ema_fast_val = ema_fast_vals.get(i).unwrap_or(f64::NAN);
        let ema_slow_val = ema_slow_vals.get(i).unwrap_or(f64::NAN);
        let rsi_val = rsi_vals.get(i).unwrap_or(f64::NAN);
        let williams_r_val = williams_r_vals.get(i).unwrap_or(f64::NAN);
        let stoch_k_val = stoch_k_vals.get(i).unwrap_or(f64::NAN);
        let stoch_d_val = stoch_d_vals.get(i).unwrap_or(f64::NAN);
        let psar_val = psar_vals.get(i).unwrap_or(f64::NAN);
        let atr_val = atr_vals.get(i).unwrap_or(f64::NAN);
        let bb_upper_val = bb_upper_vals.get(i).unwrap_or(f64::NAN);
        let bb_lower_val = bb_lower_vals.get(i).unwrap_or(f64::NAN);
        let _bb_middle_val = bb_middle_vals.get(i).unwrap_or(f64::NAN);
        let mfi_val = mfi_vals.get(i).unwrap_or(f64::NAN);
        let cmf_val = cmf_vals.get(i).unwrap_or(f64::NAN);
        let obv_val = obv_vals.get(i).unwrap_or(f64::NAN);
        let volume_sma_val = volume_sma_vals.get(i).unwrap_or(f64::NAN);

        // Skip if any essential indicator is NaN
        if ema_fast_val.is_nan()
            || ema_slow_val.is_nan()
            || rsi_val.is_nan()
            || williams_r_val.is_nan()
            || stoch_k_val.is_nan()
            || stoch_d_val.is_nan()
            || psar_val.is_nan()
            || atr_val.is_nan()
            || bb_upper_val.is_nan()
            || bb_lower_val.is_nan()
            || mfi_val.is_nan()
            || cmf_val.is_nan()
        {
            buy_signals.push(0);
            sell_signals.push(0);
            stop_levels.push(0.0);
            target_levels.push(0.0);
            continue;
        }

        // Previous values for crossovers
        let prev_ema_fast = ema_fast_vals.get(i - 1).unwrap_or(f64::NAN);
        let prev_ema_slow = ema_slow_vals.get(i - 1).unwrap_or(f64::NAN);
        let prev_stoch_k = stoch_k_vals.get(i - 1).unwrap_or(f64::NAN);
        let prev_stoch_d = stoch_d_vals.get(i - 1).unwrap_or(f64::NAN);
        let prev_obv = obv_val;
        let prev_price = close.get(i - 1).unwrap_or(f64::NAN);

        // Volume filter
        let volume_ok =
            !params.use_volume_filter || vol >= volume_sma_val * params.volume_threshold;

        // Time filter
        let time_ok = if params.use_time_filter && has_time_column {
            let time_str = match time_series.as_ref().unwrap().dtype() {
                DataType::String => time_series
                    .as_ref()
                    .unwrap()
                    .str()?
                    .get(i)
                    .unwrap_or("")
                    .to_string(),
                DataType::Datetime(_, _) => {
                    let dt = time_series.as_ref().unwrap().datetime()?.get(i);
                    if let Some(datetime) = dt {
                        // Extract hours and minutes from nanosecond timestamp
                        let hour = (datetime / 3600000000000) % 24;
                        let minute = (datetime / 60000000000) % 60;
                        format!("{:02}:{:02}", hour, minute)
                    } else {
                        "".to_string()
                    }
                }
                _ => "".to_string(),
            };

            // Parse time components
            let parts: Vec<&str> = time_str.split(':').collect();
            if parts.len() >= 2 {
                let hour: i32 = parts[0].parse().unwrap_or(0);
                let minute: i32 = parts[1].parse().unwrap_or(0);
                let minutes_from_open = (hour - 9) * 60 + (minute - 30);
                let minutes_to_close = (16 - hour) * 60 - minute;

                // Apply time filters
                let morning_ok = !params.use_time_filter
                    || minutes_from_open >= params.filter_morning_minutes as i32;
                let lunch_ok = !params.filter_lunch_hour || !(12..13).contains(&hour);
                let late_day_ok = !params.use_time_filter
                    || minutes_to_close >= params.filter_late_day_minutes as i32;

                morning_ok && lunch_ok && late_day_ok
            } else {
                true
            }
        } else {
            true
        };

        // Check for buy signals
        let ema_cross_up = ema_fast_val > ema_slow_val && prev_ema_fast <= prev_ema_slow;
        let price_above_ema = price > ema_fast_val;
        let rsi_oversold = rsi_val < params.rsi_oversold;
        let rsi_rising = rsi_val > rsi_vals.get(i - 1).unwrap_or(100.0);
        let williams_r_bullish =
            williams_r_val < -80.0 && williams_r_val > williams_r_vals.get(i - 1).unwrap_or(-100.0);
        let stoch_cross_up = stoch_k_val > stoch_d_val && prev_stoch_k <= prev_stoch_d;
        let stoch_oversold = stoch_k_val < 20.0;
        let psar_bullish = price > psar_val;
        let price_at_bb_lower = price <= bb_lower_val;
        let mfi_oversold = mfi_val < 20.0;
        let cmf_positive = cmf_val > 0.05;
        let _obv_rising = obv_val > prev_obv;

        // Bullish divergence (price lower, indicators higher)
        let price_down = price < prev_price;
        let bullish_rsi_divergence = price_down && rsi_rising;

        // Check for sell signals or reversal conditions
        let ema_cross_down = ema_fast_val < ema_slow_val && prev_ema_fast >= prev_ema_slow;
        let price_below_ema = price < ema_fast_val;
        let rsi_overbought = rsi_val > params.rsi_overbought;
        let rsi_falling = rsi_val < rsi_vals.get(i - 1).unwrap_or(0.0);
        let williams_r_bearish =
            williams_r_val > -20.0 && williams_r_val < williams_r_vals.get(i - 1).unwrap_or(0.0);
        let stoch_cross_down = stoch_k_val < stoch_d_val && prev_stoch_k >= prev_stoch_d;
        let stoch_overbought = stoch_k_val > 80.0;
        let psar_bearish = price < psar_val;
        let price_at_bb_upper = price >= bb_upper_val;
        let mfi_overbought = mfi_val > 80.0;
        let cmf_negative = cmf_val < -0.05;

        // Count buy signals
        let mut buy_score = 0;
        if ema_cross_up {
            buy_score += 1;
        }
        if price_above_ema {
            buy_score += 1;
        }
        if rsi_oversold && rsi_rising {
            buy_score += 1;
        }
        if williams_r_bullish {
            buy_score += 1;
        }
        if stoch_cross_up || stoch_oversold {
            buy_score += 1;
        }
        if psar_bullish {
            buy_score += 1;
        }
        if price_at_bb_lower {
            buy_score += 1;
        }
        if mfi_oversold {
            buy_score += 1;
        }
        if cmf_positive {
            buy_score += 1;
        }
        if bullish_rsi_divergence {
            buy_score += 1;
        }

        // Count sell signals
        let mut sell_score = 0;
        if ema_cross_down {
            sell_score += 1;
        }
        if price_below_ema {
            sell_score += 1;
        }
        if rsi_overbought && rsi_falling {
            sell_score += 1;
        }
        if williams_r_bearish {
            sell_score += 1;
        }
        if stoch_cross_down || stoch_overbought {
            sell_score += 1;
        }
        if psar_bearish {
            sell_score += 1;
        }
        if price_at_bb_upper {
            sell_score += 1;
        }
        if mfi_overbought {
            sell_score += 1;
        }
        if cmf_negative {
            sell_score += 1;
        }

        // Risk management - for stop loss and take profit
        let stop_loss = if in_position {
            entry_price - (atr_val * params.atr_stop_multiplier)
        } else {
            price - (atr_val * params.atr_stop_multiplier)
        };

        let take_profit = if in_position {
            entry_price + (atr_val * params.atr_profit_multiplier)
        } else {
            price + (atr_val * params.atr_profit_multiplier)
        };

        // Check for stop or target hits
        let stop_hit = in_position && low_val <= stop_levels[i - 1] && stop_levels[i - 1] > 0.0;
        let target_hit =
            in_position && high_val >= target_levels[i - 1] && target_levels[i - 1] > 0.0;

        // Generate final signals
        let buy_signal =
            if !in_position && buy_score >= params.min_buy_signals as i32 && volume_ok && time_ok {
                1
            } else {
                0
            };

        let sell_signal = if in_position
            && (sell_score >= params.min_sell_signals as i32 || stop_hit || target_hit)
        {
            1
        } else {
            0
        };

        // Update tracking variables
        if buy_signal == 1 {
            in_position = true;
            entry_price = price;
        } else if sell_signal == 1 {
            in_position = false;
        }

        // Push results
        buy_signals.push(buy_signal);
        sell_signals.push(sell_signal);
        stop_levels.push(stop_loss);
        target_levels.push(take_profit);
    }

    // Create DataFrame with all indicators and signals
    let mut indicator_df = df.clone();

    // Add indicators to DataFrame
    indicator_df.with_column(ema_fast)?;
    indicator_df.with_column(ema_slow)?;
    indicator_df.with_column(rsi)?;
    indicator_df.with_column(williams_r)?;
    indicator_df.with_column(stoch_k)?;
    indicator_df.with_column(stoch_d)?;
    indicator_df.with_column(psar)?;
    indicator_df.with_column(atr)?;
    indicator_df.with_column(bb_middle)?;
    indicator_df.with_column(bb_upper)?;
    indicator_df.with_column(bb_lower)?;
    indicator_df.with_column(mfi)?;
    indicator_df.with_column(cmf)?;
    indicator_df.with_column(obv)?;

    // Add signals
    let buy_series = Series::new("buy_signal".into(), &buy_signals);
    let sell_series = Series::new("sell_signal".into(), &sell_signals);
    let stop_series = Series::new("stop_level".into(), &stop_levels);
    let target_series = Series::new("target_level".into(), &target_levels);

    indicator_df.with_column(buy_series)?;
    indicator_df.with_column(sell_series)?;
    indicator_df.with_column(stop_series)?;
    indicator_df.with_column(target_series)?;

    Ok(StrategySignals {
        buy_signals,
        sell_signals,
        stop_levels,
        target_levels,
        indicator_values: indicator_df,
    })
}

/// Calculate performance metrics for the enhanced minute-based strategy
///
/// This function calculates comprehensive performance metrics including
/// risk-adjusted returns.
///
/// # Arguments
///
/// * `close_prices` - Column of close prices
/// * `buy_signals` - Vector of buy signals (0 or 1)
/// * `sell_signals` - Vector of sell signals (0 or 1)
/// * `stop_levels` - Vector of stop loss levels
/// * `target_levels` - Vector of take profit levels
/// * `start_capital` - Starting capital amount
/// * `close_positions_eod` - Whether to close positions at end of day
///
/// # Returns
///
/// * `(final_value, total_return, num_trades, win_rate, max_drawdown, profit_factor, avg_profit_per_trade)`
pub fn calculate_performance(
    close_prices: &Column,
    buy_signals: &[i32],
    sell_signals: &[i32],
    stop_levels: &[f64],
    target_levels: &[f64],
    start_capital: f64,
    close_positions_eod: bool,
) -> (f64, f64, usize, f64, f64, f64, f64) {
    let close = close_prices.f64().unwrap();
    let mut capital = start_capital;
    let mut shares = 0.0;
    let mut trades = 0;
    let mut wins = 0;
    let mut _losses = 0;
    let mut buy_price = 0.0;
    let mut total_profit = 0.0;
    let mut total_loss = 0.0;
    let mut equity_curve = Vec::with_capacity(close.len());
    let mut max_equity = start_capital;
    let mut max_drawdown = 0.0;

    // Current day tracking for EOD closing
    let mut current_day = 0;

    // Initialize equity curve
    for _ in 0..close.len() {
        equity_curve.push(start_capital);
    }

    // Process signals
    for i in 0..close.len() {
        let price = close.get(i).unwrap_or(f64::NAN);

        if price.is_nan() {
            continue;
        }

        // Check for day change if closing positions EOD
        let day = i / 390; // Assuming 390 minutes in a trading day (6.5 hours)

        if close_positions_eod && day != current_day && shares > 0.0 {
            // Close position at end of day
            let position_value = shares * price;
            let trade_profit = position_value - (shares * buy_price);

            if trade_profit > 0.0 {
                wins += 1;
                total_profit += trade_profit;
            } else {
                _losses += 1;
                total_loss += trade_profit.abs();
            }

            capital += position_value;
            shares = 0.0;
            trades += 1;
        }

        current_day = day;

        // Check for buy signal
        if i < buy_signals.len() && buy_signals[i] == 1 && shares == 0.0 {
            shares = capital / price;
            capital = 0.0;
            buy_price = price;
            trades += 1;
        }
        // Check for sell signal or stop/target hit
        else if i < sell_signals.len()
            && shares > 0.0
            && (sell_signals[i] == 1
                || (i < stop_levels.len() && price <= stop_levels[i] && stop_levels[i] > 0.0)
                || (i < target_levels.len() && price >= target_levels[i] && target_levels[i] > 0.0))
        {
            let position_value = shares * price;
            let trade_profit = position_value - (shares * buy_price);

            if trade_profit > 0.0 {
                wins += 1;
                total_profit += trade_profit;
            } else {
                _losses += 1;
                total_loss += trade_profit.abs();
            }

            capital += position_value;
            shares = 0.0;
        }

        // Update equity curve
        let current_equity = capital + (shares * price);
        if i < equity_curve.len() {
            equity_curve[i] = current_equity;
        }

        // Update max equity and drawdown
        if current_equity > max_equity {
            max_equity = current_equity;
        } else {
            let drawdown = (max_equity - current_equity) / max_equity;
            if drawdown > max_drawdown {
                max_drawdown = drawdown;
            }
        }
    }

    // Close any open position at the end of the backtest
    if shares > 0.0 {
        let final_price = close.get(close.len() - 1).unwrap_or(0.0);
        capital += shares * final_price;
    }

    // Calculate final metrics
    let final_value = capital;
    let total_return = (final_value / start_capital - 1.0) * 100.0;
    let win_rate = if trades > 0 {
        (wins as f64 / trades as f64) * 100.0
    } else {
        0.0
    };
    let profit_factor = if total_loss > 0.0 {
        total_profit / total_loss
    } else if total_profit > 0.0 {
        f64::INFINITY
    } else {
        0.0
    };
    let avg_profit_per_trade = if trades > 0 {
        ((final_value / start_capital) - 1.0) / trades as f64 * 100.0
    } else {
        0.0
    };

    (
        final_value,
        total_return,
        trades,
        win_rate,
        max_drawdown,
        profit_factor,
        avg_profit_per_trade,
    )
}
