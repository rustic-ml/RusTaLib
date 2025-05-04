use crate::indicators::{
    moving_averages::calculate_sma,
    oscillators::{calculate_macd, calculate_rsi},
    volatility::{calculate_atr, calculate_bollinger_bands},
    volume::calculate_obv,
};
use polars::prelude::*;

/// Strategy parameters for the volatility-focused multi-indicator strategy
#[derive(Clone)]
pub struct StrategyParams {
    pub sma_short_period: usize,
    pub sma_long_period: usize,
    pub rsi_period: usize,
    pub rsi_overbought: f64,
    pub rsi_oversold: f64,
    pub bb_period: usize,
    pub bb_std_dev: f64,
    pub macd_fast: usize,
    pub macd_slow: usize,
    pub macd_signal: usize,
    pub atr_period: usize,
    pub atr_multiplier: f64,
    pub volume_threshold: f64,
    pub min_signals_for_buy: usize,
    pub min_signals_for_sell: usize,
}

impl Default for StrategyParams {
    fn default() -> Self {
        Self {
            sma_short_period: 5,
            sma_long_period: 20,
            rsi_period: 7,
            rsi_overbought: 75.0,
            rsi_oversold: 25.0,
            bb_period: 14,
            bb_std_dev: 2.5,
            macd_fast: 8,
            macd_slow: 21,
            macd_signal: 5,
            atr_period: 14,
            atr_multiplier: 3.0,
            volume_threshold: 1.5,
            min_signals_for_buy: 3,
            min_signals_for_sell: 3,
        }
    }
}

/// Multi-indicator strategy result with signals
pub struct StrategySignals {
    pub buy_signals: Vec<i32>,
    pub sell_signals: Vec<i32>,
    pub indicator_values: DataFrame,
}

/// Run the multi-indicator strategy on the given DataFrame
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
    let sma_short = calculate_sma(df, "close", params.sma_short_period)?;
    let sma_long = calculate_sma(df, "close", params.sma_long_period)?;
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

    // Extract values for calculations
    let close = df.column("close")?.f64()?;
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let volume = df.column("volume")?.f64()?;

    // Fix temporary value dropped while borrowed errors
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

    // Calculate volume moving average for relative volume
    let volume_sma = calculate_sma(df, "volume", 20)?;
    let volume_sma_cloned = volume_sma.clone();
    let volume_sma_vals = volume_sma_cloned.f64()?;

    // Create arrays for buy/sell signals
    let mut buy_signals = Vec::with_capacity(df.height());
    let mut sell_signals = Vec::with_capacity(df.height());
    let mut is_in_position = false;

    // The maximum window size needed
    let max_window = params
        .sma_long_period
        .max(params.macd_slow + params.macd_signal)
        .max(params.atr_period)
        .max(20); // For volume SMA

    // Fill the first max_window elements with 0
    for _ in 0..max_window {
        buy_signals.push(0);
        sell_signals.push(0);
    }

    // Main strategy logic
    for i in max_window..df.height() {
        // Skip if we don't have valid values
        if sma_short_vals.get(i).is_none()
            || sma_long_vals.get(i).is_none()
            || rsi_vals.get(i).is_none()
            || bb_upper_vals.get(i).is_none()
            || bb_lower_vals.get(i).is_none()
            || macd_vals.get(i).is_none()
            || macd_signal_vals.get(i).is_none()
            || atr_vals.get(i).is_none()
            || obv_vals.get(i).is_none()
            || volume_sma_vals.get(i).is_none()
        {
            buy_signals.push(0);
            sell_signals.push(0);
            continue;
        }

        // Extract values
        let price = close.get(i).unwrap_or(0.0);
        let high_price = high.get(i).unwrap_or(0.0);
        let low_price = low.get(i).unwrap_or(0.0);
        let current_volume = volume.get(i).unwrap_or(0.0);
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
        let avg_volume = volume_sma_vals.get(i).unwrap_or(1.0);

        // Previous values
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
        let prev_price = if i > 0 {
            close.get(i - 1).unwrap_or(price)
        } else {
            price
        };

        // Volatility and momentum conditions
        let high_volatility = atr_val > (price * 0.02); // ATR more than 2% of price
        let price_momentum = (price - prev_price) / prev_price * 100.0;
        let strong_momentum = price_momentum.abs() > 1.0; // More than 1% price change
        let high_relative_volume = current_volume > (avg_volume * params.volume_threshold);

        // Bullish and bearish OBV divergence
        let obv_increasing = obv_val > prev_obv;
        let price_decreasing = price < prev_price;
        let bullish_obv_divergence = obv_increasing && price_decreasing;

        let obv_decreasing = obv_val < prev_obv;
        let price_increasing = price > prev_price;
        let bearish_obv_divergence = obv_decreasing && price_increasing;

        // Check for buy signals
        let sma_cross_up = sma_short_val > sma_long_val
            && (i > 0
                && sma_short_vals.get(i - 1).unwrap_or(0.0)
                    <= sma_long_vals.get(i - 1).unwrap_or(0.0));
        let rsi_oversold = rsi_val < params.rsi_oversold;
        let rsi_rising = rsi_val > prev_rsi;
        let price_at_bb_lower = price <= bb_lower_val;
        let macd_cross_up = macd_val > macd_signal_val && prev_macd <= prev_macd_signal;
        let volatility_breakout = high_price > (bb_middle_val + atr_val * params.atr_multiplier);

        // Check for sell signals
        let sma_cross_down = sma_short_val < sma_long_val
            && (i > 0
                && sma_short_vals.get(i - 1).unwrap_or(0.0)
                    >= sma_long_vals.get(i - 1).unwrap_or(0.0));
        let rsi_overbought = rsi_val > params.rsi_overbought;
        let rsi_falling = rsi_val < prev_rsi;
        let price_at_bb_upper = price >= bb_upper_val;
        let macd_cross_down = macd_val < macd_signal_val && prev_macd >= prev_macd_signal;
        let volatility_breakdown = low_price < (bb_middle_val - atr_val * params.atr_multiplier);

        // Combined signal logic with more weight on volatility and volume
        let buy_score = (if sma_cross_up { 1 } else { 0 })
            + (if rsi_oversold { 1 } else { 0 })
            + (if rsi_rising { 1 } else { 0 })
            + (if price_at_bb_lower { 1 } else { 0 })
            + (if macd_cross_up { 1 } else { 0 })
            + (if volatility_breakout && high_relative_volume {
                2
            } else {
                0
            })
            + (if bullish_obv_divergence { 1 } else { 0 })
            + (if high_volatility && strong_momentum && price_momentum > 0.0 {
                1
            } else {
                0
            });

        let sell_score = (if sma_cross_down { 1 } else { 0 })
            + (if rsi_overbought { 1 } else { 0 })
            + (if rsi_falling { 1 } else { 0 })
            + (if price_at_bb_upper { 1 } else { 0 })
            + (if macd_cross_down { 1 } else { 0 })
            + (if volatility_breakdown && high_relative_volume {
                2
            } else {
                0
            })
            + (if bearish_obv_divergence { 1 } else { 0 })
            + (if high_volatility && strong_momentum && price_momentum < 0.0 {
                1
            } else {
                0
            });

        // Final decision using configurable thresholds
        let buy_signal = if !is_in_position && buy_score >= params.min_signals_for_buy {
            1
        } else {
            0
        };
        let sell_signal = if is_in_position && sell_score >= params.min_signals_for_sell {
            1
        } else {
            0
        };

        buy_signals.push(buy_signal);
        sell_signals.push(sell_signal);

        // Update position status
        if buy_signal == 1 {
            is_in_position = true;
        } else if sell_signal == 1 {
            is_in_position = false;
        }
    }

    // Create a new DataFrame with all indicators
    let mut indicator_df = df.clone();

    // Add indicators to the DataFrame
    let _ = indicator_df.with_column(sma_short.with_name("sma_short".into()));
    let _ = indicator_df.with_column(sma_long.with_name("sma_long".into()));
    let _ = indicator_df.with_column(rsi.with_name("rsi".into()));
    let _ = indicator_df.with_column(bb_middle.with_name("bb_middle".into()));
    let _ = indicator_df.with_column(bb_upper.with_name("bb_upper".into()));
    let _ = indicator_df.with_column(bb_lower.with_name("bb_lower".into()));
    let _ = indicator_df.with_column(macd.with_name("macd".into()));
    let _ = indicator_df.with_column(macd_signal.with_name("macd_signal".into()));
    let _ = indicator_df.with_column(atr.with_name("atr".into()));
    let _ = indicator_df.with_column(obv.with_name("obv".into()));
    let _ = indicator_df.with_column(volume_sma.with_name("volume_sma".into()));

    // Add buy and sell signals
    let buy_series = Series::new("buy_signal".into(), &buy_signals);
    let sell_series = Series::new("sell_signal".into(), &sell_signals);
    let _ = indicator_df.with_column(buy_series);
    let _ = indicator_df.with_column(sell_series);

    Ok(StrategySignals {
        buy_signals,
        sell_signals,
        indicator_values: indicator_df,
    })
}

/// Calculate performance metrics based on buy/sell signals
///
/// # Arguments
///
/// * `close_prices` - Column of close prices
/// * `buy_signals` - Vector of buy signals (0 or 1)
/// * `sell_signals` - Vector of sell signals (0 or 1)
/// * `start_capital` - Starting capital amount
///
/// # Returns
///
/// * `(final_value, total_return, num_trades, win_rate, max_drawdown, profit_factor)`
pub fn calculate_performance(
    close_prices: &Column,
    buy_signals: &[i32],
    sell_signals: &[i32],
    start_capital: f64,
) -> (f64, f64, usize, f64, f64, f64) {
    let close = close_prices.f64().unwrap();
    let mut capital = start_capital;
    let mut shares = 0.0;
    let mut trades = 0;
    let mut wins = 0;
    let mut losses = 0;
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

        if buy_signal == 1 {
            shares = capital / price;
            capital = 0.0;
            buy_price = price;
            trades += 1;
        } else if sell_signal == 1 {
            capital = shares * price;
            let trade_profit = capital - (shares * buy_price);

            if trade_profit > 0.0 {
                wins += 1;
                total_profit += trade_profit;
            } else {
                losses += 1;
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
