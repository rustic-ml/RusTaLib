use crate::indicators::{
    moving_averages::{calculate_ema, calculate_sma},
    oscillators::{calculate_macd, calculate_rsi},
    volatility::{calculate_atr, calculate_bollinger_bands},
    volume::calculate_obv,
};
use polars::prelude::*;

/// Strategy parameters for the adaptive trend-filtered strategy
#[derive(Clone)]
pub struct StrategyParams {
    // Trend detection
    pub ema_short_period: usize,
    pub ema_mid_period: usize,
    pub ema_long_period: usize,

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

    // Volume
    pub obv_ema_period: usize,
    pub volume_threshold: f64,

    // Signal thresholds
    pub min_signals_for_buy: usize,
    pub min_signals_for_sell: usize,

    // Risk management
    pub stop_loss_atr_multiple: f64,
    pub take_profit_atr_multiple: f64,
}

impl Default for StrategyParams {
    fn default() -> Self {
        Self {
            ema_short_period: 5,
            ema_mid_period: 21,
            ema_long_period: 50,
            rsi_period: 14,
            rsi_overbought: 70.0,
            rsi_oversold: 30.0,
            bb_period: 20,
            bb_std_dev: 2.0,
            atr_period: 14,
            atr_position_size_factor: 2.0,
            macd_fast: 12,
            macd_slow: 26,
            macd_signal: 9,
            obv_ema_period: 20,
            volume_threshold: 1.2,
            min_signals_for_buy: 3,
            min_signals_for_sell: 3,
            stop_loss_atr_multiple: 3.0,
            take_profit_atr_multiple: 4.0,
        }
    }
}

/// Multi-indicator strategy result with signals
pub struct StrategySignals {
    pub buy_signals: Vec<i32>,
    pub sell_signals: Vec<i32>,
    pub stop_signals: Vec<i32>,
    pub take_profit_signals: Vec<i32>,
    pub position_sizes: Vec<f64>,
    pub indicator_values: DataFrame,
}

/// Run the adaptive trend-filtered strategy on the given DataFrame
///
/// # Arguments
///
/// * `df` - DataFrame containing OHLCV data with columns "open", "high", "low", "close", "volume"
/// * `params` - Strategy parameters
///
/// # Returns
///
/// * `Result<StrategySignals, PolarsError>` - Buy/sell signals and indicator values
pub fn run_strategy(
    df: &DataFrame,
    params: &StrategyParams,
) -> Result<StrategySignals, PolarsError> {
    // Calculate technical indicators
    let ema_short = calculate_ema(df, "close", params.ema_short_period)?;
    let ema_mid = calculate_ema(df, "close", params.ema_mid_period)?;
    let ema_long = calculate_ema(df, "close", params.ema_long_period)?;
    let rsi = calculate_rsi(df, params.rsi_period, "close")?;
    let (bb_middle, bb_upper, bb_lower) =
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

    // Calculate OBV EMA for relative strength of volume
    let obv_df = DataFrame::new(vec![obv.clone().into()])?;
    let obv_ema = calculate_ema(&obv_df, "obv", params.obv_ema_period)?;

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

    // Calculate volume moving average for relative volume
    let volume_sma = calculate_sma(df, "volume", 20)?;
    let volume_sma_cloned = volume_sma.clone();
    let volume_sma_vals = volume_sma_cloned.f64()?;

    // Create arrays for signals
    let mut buy_signals = Vec::with_capacity(df.height());
    let mut sell_signals = Vec::with_capacity(df.height());
    let mut stop_signals = Vec::with_capacity(df.height());
    let mut take_profit_signals = Vec::with_capacity(df.height());
    let mut position_sizes = Vec::with_capacity(df.height());
    let mut is_in_position = false;
    let mut entry_price = 0.0;

    // The maximum window size needed
    let max_window = params
        .ema_long_period
        .max(params.macd_slow + params.macd_signal)
        .max(params.atr_period)
        .max(params.obv_ema_period)
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
        // Skip if we don't have valid values
        if ema_short_vals.get(i).is_none()
            || ema_mid_vals.get(i).is_none()
            || ema_long_vals.get(i).is_none()
            || rsi_vals.get(i).is_none()
            || bb_upper_vals.get(i).is_none()
            || macd_vals.get(i).is_none()
            || atr_vals.get(i).is_none()
            || obv_vals.get(i).is_none()
        {
            buy_signals.push(0);
            sell_signals.push(0);
            stop_signals.push(0);
            take_profit_signals.push(0);
            position_sizes.push(0.0);
            continue;
        }

        // Extract values
        let price = close.get(i).unwrap_or(0.0);
        let high_price = high.get(i).unwrap_or(0.0);
        let low_price = low.get(i).unwrap_or(0.0);
        let current_volume = volume.get(i).unwrap_or(0.0);
        let ema_short_val = ema_short_vals.get(i).unwrap_or(0.0);
        let ema_mid_val = ema_mid_vals.get(i).unwrap_or(0.0);
        let ema_long_val = ema_long_vals.get(i).unwrap_or(0.0);
        let rsi_val = rsi_vals.get(i).unwrap_or(0.0);
        let bb_upper_val = bb_upper_vals.get(i).unwrap_or(0.0);
        let bb_lower_val = bb_lower_vals.get(i).unwrap_or(0.0);
        let _bb_middle_val = bb_middle_vals.get(i).unwrap_or(0.0);
        let macd_val = macd_vals.get(i).unwrap_or(0.0);
        let macd_signal_val = macd_signal_vals.get(i).unwrap_or(0.0);
        let atr_val = atr_vals.get(i).unwrap_or(0.0);
        let obv_val = obv_vals.get(i).unwrap_or(0.0);
        let obv_ema_val = obv_ema_vals.get(i).unwrap_or(0.0);
        let avg_volume = volume_sma_vals.get(i).unwrap_or(1.0);

        // Previous values
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
        let prev_price = if i > 0 {
            close.get(i - 1).unwrap_or(price)
        } else {
            price
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
        let prev_obv = if i > 1 {
            obv_vals.get(i - 1).unwrap_or(0.0)
        } else {
            0.0
        };

        // Trend detection
        let bullish_trend = ema_short_val > ema_mid_val && ema_mid_val > ema_long_val;
        let bearish_trend = ema_short_val < ema_mid_val && ema_mid_val < ema_long_val;

        // Adaptive trend strength - stronger when EMAs are farther apart
        let trend_strength = (ema_short_val - ema_long_val).abs() / ema_long_val * 100.0;
        let strong_trend = trend_strength > 2.0; // 2% difference between short and long EMAs

        // Volatility conditions
        let high_volatility = atr_val > (price * 0.015); // ATR more than 1.5% of price
        let price_momentum = (price - prev_price) / prev_price * 100.0;
        let strong_momentum = price_momentum.abs() > 1.0; // More than 1% price change

        // Volume conditions
        let high_relative_volume = current_volume > (avg_volume * params.volume_threshold);
        let obv_rising = obv_val > obv_ema_val && obv_val > prev_obv;
        let obv_falling = obv_val < obv_ema_val && obv_val < prev_obv;

        // Divergence detection
        let bullish_div = price < prev_price && macd_val > prev_macd;
        let bearish_div = price > prev_price && macd_val < prev_macd;

        // Mean reversion components
        let oversold = rsi_val < params.rsi_oversold;
        let overbought = rsi_val > params.rsi_overbought;
        let rsi_rising = rsi_val > prev_rsi;
        let rsi_falling = rsi_val < prev_rsi;
        let price_at_bb_lower = price <= bb_lower_val;
        let price_at_bb_upper = price >= bb_upper_val;

        // Trend reversal detection
        let ema_short_cross_above_mid =
            ema_short_val > ema_mid_val && prev_ema_short <= prev_ema_mid;
        let ema_short_cross_below_mid =
            ema_short_val < ema_mid_val && prev_ema_short >= prev_ema_mid;

        // MACD crossover
        let macd_cross_up = macd_val > macd_signal_val && prev_macd <= prev_macd_signal;
        let macd_cross_down = macd_val < macd_signal_val && prev_macd >= prev_macd_signal;

        // Check for stop loss and take profit if in position
        let mut stop_loss_hit = false;
        let mut take_profit_hit = false;

        if is_in_position {
            // Calculate stop loss and take profit levels
            let stop_loss_level = entry_price - (params.stop_loss_atr_multiple * atr_val);
            let take_profit_level = entry_price + (params.take_profit_atr_multiple * atr_val);

            // Check if stop loss or take profit hit
            stop_loss_hit = low_price <= stop_loss_level;
            take_profit_hit = high_price >= take_profit_level;
        }

        // Combined signal logic with adaptive weights based on market conditions
        // In strong trends, we emphasize momentum; in choppy conditions, we emphasize mean reversion
        let mut buy_score = 0;
        let mut sell_score = 0;

        // Base signals
        if ema_short_cross_above_mid {
            buy_score += 1;
        }
        if bullish_trend && strong_trend {
            buy_score += 1;
        }
        if oversold && rsi_rising {
            buy_score += 1;
        }
        if price_at_bb_lower && bullish_trend {
            buy_score += 1;
        }
        if macd_cross_up {
            buy_score += 1;
        }
        if obv_rising && high_relative_volume {
            buy_score += 1;
        }
        if bullish_div {
            buy_score += 1;
        }

        if ema_short_cross_below_mid {
            sell_score += 1;
        }
        if bearish_trend && strong_trend {
            sell_score += 1;
        }
        if overbought && rsi_falling {
            sell_score += 1;
        }
        if price_at_bb_upper && bearish_trend {
            sell_score += 1;
        }
        if macd_cross_down {
            sell_score += 1;
        }
        if obv_falling && high_relative_volume {
            sell_score += 1;
        }
        if bearish_div {
            sell_score += 1;
        }

        // Adjust signals based on adaptive conditions
        if high_volatility && strong_momentum {
            if price_momentum > 0.0 {
                buy_score += 1;
            }
            if price_momentum < 0.0 {
                sell_score += 1;
            }
        }

        // Position size based on ATR (lower position size for higher volatility)
        let position_size = if atr_val > 0.0 {
            1.0 / (params.atr_position_size_factor * atr_val / price)
        } else {
            1.0
        };

        // Final decision using configurable thresholds
        let buy_signal = if !is_in_position && buy_score >= params.min_signals_for_buy {
            1
        } else {
            0
        };
        let sell_signal = if is_in_position
            && (sell_score >= params.min_signals_for_sell || stop_loss_hit || take_profit_hit)
        {
            1
        } else {
            0
        };
        let stop_signal = if is_in_position && stop_loss_hit {
            1
        } else {
            0
        };
        let take_profit_signal = if is_in_position && take_profit_hit {
            1
        } else {
            0
        };

        buy_signals.push(buy_signal);
        sell_signals.push(sell_signal);
        stop_signals.push(stop_signal);
        take_profit_signals.push(take_profit_signal);
        position_sizes.push(position_size);

        // Update position status
        if buy_signal == 1 {
            is_in_position = true;
            entry_price = price;
        } else if sell_signal == 1 {
            is_in_position = false;
        }
    }

    // Create a new DataFrame with all indicators
    let mut indicator_df = df.clone();

    // Add indicators to the DataFrame
    let _ = indicator_df.with_column(ema_short.with_name("ema_short".into()));
    let _ = indicator_df.with_column(ema_mid.with_name("ema_mid".into()));
    let _ = indicator_df.with_column(ema_long.with_name("ema_long".into()));
    let _ = indicator_df.with_column(rsi.with_name("rsi".into()));
    let _ = indicator_df.with_column(bb_middle.with_name("bb_middle".into()));
    let _ = indicator_df.with_column(bb_upper.with_name("bb_upper".into()));
    let _ = indicator_df.with_column(bb_lower.with_name("bb_lower".into()));
    let _ = indicator_df.with_column(macd.with_name("macd".into()));
    let _ = indicator_df.with_column(macd_signal.with_name("macd_signal".into()));
    let _ = indicator_df.with_column(atr.with_name("atr".into()));
    let _ = indicator_df.with_column(obv.with_name("obv".into()));
    let _ = indicator_df.with_column(obv_ema.with_name("obv_ema".into()));
    let _ = indicator_df.with_column(volume_sma.with_name("volume_sma".into()));

    // Add buy and sell signals
    let buy_series = Series::new("buy_signal".into(), &buy_signals);
    let sell_series = Series::new("sell_signal".into(), &sell_signals);
    let stop_series = Series::new("stop_signal".into(), &stop_signals);
    let take_profit_series = Series::new("take_profit_signal".into(), &take_profit_signals);
    let position_size_series = Series::new("position_size".into(), &position_sizes);

    let _ = indicator_df.with_column(buy_series);
    let _ = indicator_df.with_column(sell_series);
    let _ = indicator_df.with_column(stop_series);
    let _ = indicator_df.with_column(take_profit_series);
    let _ = indicator_df.with_column(position_size_series);

    Ok(StrategySignals {
        buy_signals,
        sell_signals,
        stop_signals,
        take_profit_signals,
        position_sizes,
        indicator_values: indicator_df,
    })
}

/// Calculate performance metrics based on buy/sell signals with position sizing
///
/// # Arguments
///
/// * `close_prices` - Column of close prices
/// * `buy_signals` - Vector of buy signals (0 or 1)
/// * `sell_signals` - Vector of sell signals (0 or 1)
/// * `position_sizes` - Vector of position sizes
/// * `start_capital` - Starting capital amount
///
/// # Returns
///
/// * `(final_value, total_return, num_trades, win_rate, max_drawdown, profit_factor)`
pub fn calculate_performance(
    close_prices: &Column,
    buy_signals: &[i32],
    sell_signals: &[i32],
    position_sizes: &[f64],
    start_capital: f64,
) -> (f64, f64, usize, f64, f64, f64) {
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

    // Determine starting point with valid signals
    let start_idx = buy_signals
        .iter()
        .position(|&x| x == 1)
        .unwrap_or(0)
        .saturating_sub(1);

    // Initialize equity curve
    for _ in 0..close.len() {
        equity_curve.push(start_capital);
    }

    // Process signals
    for i in start_idx..close.len() {
        let price = close.get(i).unwrap_or(0.0);
        let buy_signal = buy_signals[i];
        let sell_signal = sell_signals[i];
        let position_size = position_sizes[i].min(1.0).max(0.1); // Ensure position size is between 0.1 and 1.0

        if buy_signal == 1 {
            // Use position sizing
            let amount_to_invest = capital * position_size;
            shares = amount_to_invest / price;
            capital -= amount_to_invest;
            buy_price = price;
            trades += 1;
        } else if sell_signal == 1 {
            let sale_value = shares * price;
            capital += sale_value;
            let trade_profit = sale_value - (shares * buy_price);

            if trade_profit > 0.0 {
                wins += 1;
                total_profit += trade_profit;
            } else {
                _losses += 1;
                total_loss += trade_profit.abs();
            }

            shares = 0.0;
        }

        // Update equity curve
        let current_equity = capital + (shares * price);
        equity_curve[i] = current_equity;

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

    // Final calculations
    let final_price = close.get(close.len() - 1).unwrap_or(0.0);
    let final_value = capital + (shares * final_price);
    let total_return = (final_value / start_capital - 1.0) * 100.0;
    let win_rate = if trades > 0 {
        (wins as f64 / trades as f64) * 100.0
    } else {
        0.0
    };
    let profit_factor = if total_loss > 0.0 {
        total_profit / total_loss
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
    )
}
