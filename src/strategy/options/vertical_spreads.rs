//! # Vertical Spread Options Strategy
//! 
//! This module implements bull and bear vertical spread strategies for options trading.
//! It includes both call and put vertical spreads with dynamic entry/exit rules based
//! on implied volatility, technical analysis, and spread pricing.

use crate::indicators::{
    oscillators::calculate_rsi,
    moving_averages::calculate_ema,
};
// TODO: Uncomment when trade module is available
// use crate::trade::options::spreads::calculate_vertical_spread_metrics;
use polars::prelude::*;
use std::collections::HashMap;

/// Parameters for configuring the vertical spread strategy
#[derive(Clone)]
pub struct StrategyParams {
    /// Type of spread: "bull_call", "bear_call", "bull_put", "bear_put"
    pub spread_type: String,
    
    /// Days to expiration range for option selection
    pub min_days_to_expiry: usize,
    pub max_days_to_expiry: usize,
    
    /// Delta target for short option in the spread
    pub short_option_delta_target: f64,
    
    /// Width between short and long strikes
    pub strike_width: f64,
    
    /// Maximum percentage of capital to risk per trade
    pub max_risk_pct: f64,
    
    /// Profit target (percentage of max loss)
    pub profit_target_pct: f64,
    
    /// Stop loss (percentage of max loss)
    pub stop_loss_pct: f64,
    
    /// Entry criteria based on RSI
    pub use_rsi_filter: bool,
    pub rsi_period: usize,
    pub rsi_oversold: f64,  // For bull spreads
    pub rsi_overbought: f64, // For bear spreads
    
    /// Entry criteria based on implied volatility
    pub use_iv_filter: bool,
    pub iv_percentile_threshold: f64,
    
    /// Entry criteria based on trend (using EMA)
    pub use_trend_filter: bool,
    pub ema_short_period: usize,
    pub ema_long_period: usize,
    
    /// Days before expiration to close regardless of P/L
    pub days_to_close_before_expiry: usize,
    
    /// Maximum number of concurrent spreads
    pub max_concurrent_spreads: usize,
}

impl Default for StrategyParams {
    /// Creates default parameters for a bull put spread strategy
    fn default() -> Self {
        Self {
            spread_type: "bull_put".to_string(),
            min_days_to_expiry: 30,
            max_days_to_expiry: 45,
            short_option_delta_target: 0.30,
            strike_width: 5.0,  // $5 wide spread
            max_risk_pct: 5.0,   // 5% of capital risked per trade
            profit_target_pct: 50.0, // Close at 50% of max profit
            stop_loss_pct: 200.0,    // Close at 2x max loss
            use_rsi_filter: true,
            rsi_period: 14,
            rsi_oversold: 30.0,
            rsi_overbought: 70.0,
            use_iv_filter: true,
            iv_percentile_threshold: 60.0, // IV above 60th percentile for put selling
            use_trend_filter: true,
            ema_short_period: 8,
            ema_long_period: 21,
            days_to_close_before_expiry: 7,
            max_concurrent_spreads: 4,
        }
    }
}

/// Strategy signals and metrics
pub struct StrategySignals {
    /// Vector of entry dates/times
    pub entry_signals: Vec<i32>,
    
    /// Vector of exit dates/times
    pub exit_signals: Vec<i32>,
    
    /// Profit/loss values per trade
    pub pnl_values: Vec<f64>,
    
    /// DataFrame containing all price, indicator, and spread metrics
    pub indicator_values: DataFrame,
    
    /// Details of each trade executed
    pub trade_details: Vec<TradeDetails>,
}

/// Details of a vertical spread trade
pub struct TradeDetails {
    /// Entry timestamp
    pub entry_date: String,
    
    /// Exit timestamp
    pub exit_date: String,
    
    /// Type of spread
    pub spread_type: String,
    
    /// Short strike price
    pub short_strike: f64,
    
    /// Long strike price
    pub long_strike: f64,
    
    /// Days to expiration at entry
    pub days_to_expiry: usize,
    
    /// Credit received (for credit spreads)
    pub credit_received: f64,
    
    /// Debit paid (for debit spreads)
    pub debit_paid: f64,
    
    /// Maximum profit possible
    pub max_profit: f64,
    
    /// Maximum loss possible
    pub max_loss: f64,
    
    /// Return on risk
    pub return_on_risk: f64,
    
    /// Profit/loss amount
    pub pnl: f64,
    
    /// Profit/loss percentage
    pub pnl_pct: f64,
    
    /// Reason for exit (target, stop, expiry, or signal)
    pub exit_reason: String,
}

/// Run the vertical spread strategy on the given DataFrames
///
/// This function analyzes both underlying price data and options chain data
/// to generate entry and exit signals for vertical spread trades based on the
/// provided parameters.
///
/// # Arguments
///
/// * `price_df` - DataFrame with underlying price data (OHLCV)
/// * `options_df` - DataFrame with options chain data
/// * `params` - Strategy parameters
///
/// # Returns
///
/// * `Result<StrategySignals, PolarsError>` - Entry/exit signals and trade details
pub fn run_strategy(
    price_df: &DataFrame,
    options_df: &DataFrame,
    params: &StrategyParams,
) -> Result<StrategySignals, PolarsError> {
    // Calculate technical indicators on the underlying price
    let rsi = if params.use_rsi_filter {
        Some(calculate_rsi(price_df, params.rsi_period, "close")?)
    } else {
        None
    };
    
    let ema_short = if params.use_trend_filter {
        Some(calculate_ema(price_df, "close", params.ema_short_period)?)
    } else {
        None
    };
    
    let ema_long = if params.use_trend_filter {
        Some(calculate_ema(price_df, "close", params.ema_long_period)?)
    } else {
        None
    };
    
    // Extract date/time and close price from price DataFrame
    let dates = price_df.column("date")?;
    let close = price_df.column("close")?.f64()?;
    
    // Prepare containers for signals and trades
    let mut entry_signals = vec![0; price_df.height()];
    let mut exit_signals = vec![0; price_df.height()];
    let mut pnl_values = vec![0.0; price_df.height()];
    let mut trade_details = Vec::new();
    
    // Get technical indicator values for signal generation
    let rsi_vals = if let Some(rsi_ref) = &rsi {
        // Collect values into a Vec to avoid borrowing issues
        let rsi_vec = match rsi_ref.clone().f64() {
            Ok(chunked) => {
                let mut values = Vec::with_capacity(chunked.len());
                for i in 0..chunked.len() {
                    values.push(chunked.get(i).unwrap_or(f64::NAN));
                }
                Some(values)
            },
            Err(_) => None
        };
        rsi_vec
    } else {
        None
    };
    
    let ema_short_vals = if let Some(ema_ref) = &ema_short {
        // Collect values into a Vec to avoid borrowing issues
        let ema_vec = match ema_ref.clone().f64() {
            Ok(chunked) => {
                let mut values = Vec::with_capacity(chunked.len());
                for i in 0..chunked.len() {
                    values.push(chunked.get(i).unwrap_or(f64::NAN));
                }
                Some(values)
            },
            Err(_) => None
        };
        ema_vec
    } else {
        None
    };
    
    let ema_long_vals = if let Some(ema_ref) = &ema_long {
        // Collect values into a Vec to avoid borrowing issues
        let ema_vec = match ema_ref.clone().f64() {
            Ok(chunked) => {
                let mut values = Vec::with_capacity(chunked.len());
                for i in 0..chunked.len() {
                    values.push(chunked.get(i).unwrap_or(f64::NAN));
                }
                Some(values)
            },
            Err(_) => None
        };
        ema_vec
    } else {
        None
    };
    
    // Track active trades
    let mut active_trades: HashMap<usize, TradeDetails> = HashMap::new();
    
    // Loop through each date and determine entry/exit signals
    for i in params.ema_long_period.max(params.rsi_period)..price_df.height() {
        let current_date = dates.get(i).unwrap().to_string();
        let current_price = close.get(i).unwrap_or(f64::NAN);
        
        // Skip if missing price data
        if current_price.is_nan() {
            continue;
        }
        
        // Check if we should enter a new spread
        if active_trades.len() < params.max_concurrent_spreads {
            let mut entry_conditions_met = true;
            
            // Check RSI condition if enabled
            if let Some(rsi_series) = &rsi_vals {
                let current_rsi = if i < rsi_series.len() { rsi_series[i] } else { f64::NAN };
                if !current_rsi.is_nan() {
                    if params.spread_type.contains("bull") && current_rsi > params.rsi_oversold {
                        entry_conditions_met = false;
                    } else if params.spread_type.contains("bear") && current_rsi < params.rsi_overbought {
                        entry_conditions_met = false;
                    }
                }
            }
            
            // Check trend condition if enabled
            if let (Some(short_series), Some(long_series)) = (&ema_short_vals, &ema_long_vals) {
                let short_ema = if i < short_series.len() { short_series[i] } else { f64::NAN };
                let long_ema = if i < long_series.len() { long_series[i] } else { f64::NAN };
                
                if !short_ema.is_nan() && !long_ema.is_nan() {
                    if params.spread_type.contains("bull") && short_ema < long_ema {
                        entry_conditions_met = false;
                    } else if params.spread_type.contains("bear") && short_ema > long_ema {
                        entry_conditions_met = false;
                    }
                }
            }
            
            // Entry signal encountered
            if entry_conditions_met {
                // Here we would select appropriate strikes from the options_df
                // For this example, we'll simulate finding appropriate options
                
                // Create a simulated trade
                let new_trade = simulate_vertical_spread_trade(
                    &current_date,
                    current_price,
                    params,
                );
                
                // Record the entry
                entry_signals[i] = 1;
                active_trades.insert(i, new_trade);
            }
        }
        
        // Check if we should exit any of the active trades
        let mut trades_to_remove = Vec::new();
        
        for (&entry_idx, trade) in active_trades.iter_mut() {
            // Simulate P/L for the current trade
            let days_held = i - entry_idx;
            let pnl_pct = simulate_trade_pnl_progression(days_held, params);
            
            // Determine if we should exit
            let mut should_exit = false;
            let mut exit_reason = String::new();
            
            // Check profit target
            if pnl_pct >= params.profit_target_pct {
                should_exit = true;
                exit_reason = "target".to_string();
            }
            
            // Check stop loss
            else if pnl_pct <= -params.stop_loss_pct {
                should_exit = true;
                exit_reason = "stop".to_string();
            }
            
            // Check days to expiry threshold
            // (In reality, we would check the actual days remaining)
            else if days_held >= 30 - params.days_to_close_before_expiry {
                should_exit = true;
                exit_reason = "expiry".to_string();
            }
            
            // Exit if conditions met
            if should_exit {
                exit_signals[i] = 1;
                
                // Update trade details
                trade.exit_date = current_date.clone();
                trade.pnl_pct = pnl_pct;
                
                // For credit spreads, profit is credit received minus cost to close
                if trade.spread_type.contains("bull_put") || trade.spread_type.contains("bear_call") {
                    trade.pnl = trade.credit_received * pnl_pct / 100.0;
                } 
                // For debit spreads, profit is selling price minus debit paid
                else {
                    trade.pnl = trade.debit_paid * pnl_pct / 100.0;
                }
                
                trade.exit_reason = exit_reason;
                
                // Record P/L
                pnl_values[i] = trade.pnl;
                
                // Schedule trade for removal
                trades_to_remove.push(entry_idx);
                
                // Add to completed trades list
                trade_details.push(trade.clone());
            }
        }
        
        // Remove exited trades
        for entry_idx in trades_to_remove {
            active_trades.remove(&entry_idx);
        }
    }
    
    // Create indicator DataFrame
    let mut indicator_df = price_df.clone();
    
    // Add technical indicators
    if let Some(rsi_series) = rsi {
        indicator_df.with_column(rsi_series)?;
    }
    if let Some(ema_short_series) = ema_short {
        indicator_df.with_column(ema_short_series)?;
    }
    if let Some(ema_long_series) = ema_long {
        indicator_df.with_column(ema_long_series)?;
    }
    
    // Add entry/exit signals
    let entry_series = Series::new("entry_signals".into(), &entry_signals);
    let exit_series = Series::new("exit_signals".into(), &exit_signals);
    let pnl_series = Series::new("pnl".into(), &pnl_values);
    
    indicator_df.with_column(entry_series)?;
    indicator_df.with_column(exit_series)?;
    indicator_df.with_column(pnl_series)?;
    
    Ok(StrategySignals {
        entry_signals,
        exit_signals,
        pnl_values,
        indicator_values: indicator_df,
        trade_details,
    })
}

/// Simulate a vertical spread trade (helper function for demonstration)
fn simulate_vertical_spread_trade(
    date: &str,
    current_price: f64,
    params: &StrategyParams,
) -> TradeDetails {
    // Simulate a trade based on the spread type
    let is_credit_spread = params.spread_type == "bull_put" || params.spread_type == "bear_call";
    
    // Determine strikes based on the strategy parameters
    let short_strike = if params.spread_type == "bull_put" || params.spread_type == "bull_call" {
        current_price * (1.0 - params.short_option_delta_target * 0.1)
    } else {
        current_price * (1.0 + params.short_option_delta_target * 0.1)
    };
    
    let long_strike = if params.spread_type == "bull_put" || params.spread_type == "bear_call" {
        short_strike - params.strike_width
    } else {
        short_strike + params.strike_width
    };
    
    // Simulate option prices and credit/debit
    let simulated_premium = current_price * 0.05 * params.short_option_delta_target;
    
    // Calculate credit/debit and max profit/loss
    let (credit_received, debit_paid, max_profit, max_loss) = if is_credit_spread {
        let credit = simulated_premium * 0.7; // Long option costs less than short
        let max_profit_val = credit;
        let max_loss_val = params.strike_width * 100.0 - credit * 100.0;
        (credit, 0.0, max_profit_val, max_loss_val)
    } else {
        let debit = simulated_premium * 0.3; // Pay net debit
        let max_profit_val = params.strike_width * 100.0 - debit * 100.0;
        let max_loss_val = debit * 100.0;
        (0.0, debit, max_profit_val, max_loss_val)
    };
    
    TradeDetails {
        entry_date: date.to_string(),
        exit_date: String::new(), // To be filled at exit
        spread_type: params.spread_type.clone(),
        short_strike,
        long_strike,
        days_to_expiry: params.min_days_to_expiry,
        credit_received: credit_received * 100.0, // Per share to per contract
        debit_paid: debit_paid * 100.0,
        max_profit: max_profit,
        max_loss: max_loss,
        return_on_risk: if max_loss > 0.0 { max_profit / max_loss * 100.0 } else { 0.0 },
        pnl: 0.0, // To be filled at exit
        pnl_pct: 0.0, // To be filled at exit
        exit_reason: String::new(), // To be filled at exit
    }
}

/// Simulate P/L progression of a trade over time (for demonstration)
fn simulate_trade_pnl_progression(days_held: usize, params: &StrategyParams) -> f64 {
    // This is a simplified model of how options spreads decay
    // In reality, this would depend on price movement, IV changes, and theta decay
    
    // Assume a maximum holding period of 30 days
    let max_days = 30;
    let progress = (days_held as f64).min(max_days as f64) / max_days as f64;
    
    // Calculate profit/loss percentage based on time held
    // Theta decay accelerates as expiration approaches
    let decay_factor = 1.0 - (1.0 - progress).powi(2);
    
    // Add some randomness to simulate price movement
    let price_factor = (((days_held as f64) * 0.1).sin() - 0.5) * 30.0;
    
    // Credit spreads tend to profit from time decay
    if params.spread_type == "bull_put" || params.spread_type == "bear_call" {
        decay_factor * 100.0 + price_factor
    } 
    // Debit spreads need price movement to profit
    else {
        price_factor * 2.0 - decay_factor * 20.0
    }
}

/// Calculate performance metrics for the strategy
///
/// # Arguments
///
/// * `trade_details` - Vector of completed trades
/// * `starting_capital` - Initial capital amount
///
/// # Returns
///
/// * Tuple containing:
///   * final_capital: Final capital after all trades
///   * total_return_pct: Total return percentage
///   * num_trades: Number of trades
///   * win_rate: Percentage of winning trades
///   * avg_win: Average profit on winning trades
///   * avg_loss: Average loss on losing trades
///   * profit_factor: Ratio of gross profit to gross loss
pub fn calculate_performance(
    trade_details: &[TradeDetails],
    starting_capital: f64,
) -> (f64, f64, usize, f64, f64, f64, f64) {
    if trade_details.is_empty() {
        return (starting_capital, 0.0, 0, 0.0, 0.0, 0.0, 0.0);
    }
    
    let mut capital = starting_capital;
    let mut winning_trades = 0;
    let mut losing_trades = 0;
    let mut total_wins = 0.0;
    let mut total_losses = 0.0;
    
    for trade in trade_details {
        capital += trade.pnl;
        
        if trade.pnl > 0.0 {
            winning_trades += 1;
            total_wins += trade.pnl;
        } else if trade.pnl < 0.0 {
            losing_trades += 1;
            total_losses += trade.pnl.abs();
        }
    }
    
    let num_trades = trade_details.len();
    let win_rate = (winning_trades as f64) / (num_trades as f64) * 100.0;
    
    let avg_win = if winning_trades > 0 {
        total_wins / (winning_trades as f64)
    } else {
        0.0
    };
    
    let avg_loss = if losing_trades > 0 {
        total_losses / (losing_trades as f64)
    } else {
        0.0
    };
    
    let profit_factor = if total_losses > 0.0 {
        total_wins / total_losses
    } else {
        if total_wins > 0.0 { f64::INFINITY } else { 0.0 }
    };
    
    let total_return_pct = (capital - starting_capital) / starting_capital * 100.0;
    
    (
        capital,
        total_return_pct,
        num_trades,
        win_rate,
        avg_win,
        avg_loss,
        profit_factor,
    )
}

/// Implement Clone for TradeDetails
impl Clone for TradeDetails {
    fn clone(&self) -> Self {
        Self {
            entry_date: self.entry_date.clone(),
            exit_date: self.exit_date.clone(),
            spread_type: self.spread_type.clone(),
            short_strike: self.short_strike,
            long_strike: self.long_strike,
            days_to_expiry: self.days_to_expiry,
            credit_received: self.credit_received,
            debit_paid: self.debit_paid,
            max_profit: self.max_profit,
            max_loss: self.max_loss,
            return_on_risk: self.return_on_risk,
            pnl: self.pnl,
            pnl_pct: self.pnl_pct,
            exit_reason: self.exit_reason.clone(),
        }
    }
} 