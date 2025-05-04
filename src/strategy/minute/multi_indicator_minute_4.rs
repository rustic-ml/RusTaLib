use crate::indicators::{
    moving_averages::calculate_ema, oscillators::calculate_macd,
    volatility::calculate_bollinger_bands,
};
use polars::prelude::*;
use serde::{Deserialize, Serialize};

// Define trading types and structures directly in this file
pub trait TradingStrategy {
    type Params;

    fn name(&self) -> String;
    fn timeframe(&self) -> String;
    fn prepare_data(&self, df: &DataFrame) -> PolarsResult<DataFrame>;
    fn generate_signals(&self, df: &DataFrame) -> PolarsResult<Vec<TradeRecord>>;
    fn backtest(&self, df: &DataFrame, params: &DataFetchParams) -> PolarsResult<BacktestSummary>;
    fn set_params(&mut self, params: Self::Params);
    fn get_params(&self) -> Self::Params;
}

#[derive(Debug, Clone)]
pub enum TradeDirection {
    Long,
    Short,
}

#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub symbol: String,
    pub entry_time: String,
    pub entry_price: f64,
    pub exit_time: String,
    pub exit_price: f64,
    pub direction: TradeDirection,
    pub pnl: f64,
    pub exit_reason: String,
}

#[derive(Debug, Clone)]
pub struct TradePosition {
    pub entry_price: f64,
    pub entry_time: String,
    pub entry_index: usize,
    pub direction: TradeDirection,
}

#[derive(Debug, Clone)]
pub struct DataFetchParams {
    pub symbol: String,
    pub start_date: String,
    pub end_date: String,
    pub timeframe: String,
}

#[derive(Debug, Clone)]
pub struct BacktestSummary {
    pub strategy_name: String,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub win_rate: f64,
    pub average_pnl: f64,
    pub total_pnl: f64,
    pub trade_records: Vec<TradeRecord>,
}

// Helper function to process data with indicators
pub fn process_data_with_indicators<F>(df: &DataFrame, processor: F) -> PolarsResult<DataFrame>
where
    F: FnOnce(&DataFrame) -> PolarsResult<DataFrame>,
{
    // Ensure we have required columns
    if !df.schema().iter().any(|(name, _)| name == "close") {
        return Err(PolarsError::ComputeError(
            "DataFrame must contain a 'close' column".into(),
        ));
    }

    // Process the data
    processor(df)
}

/// Parameters for the Multi-Indicator Minute Strategy 4
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MultiIndicatorMinute4Params {
    pub ema_short_period: usize,
    pub ema_long_period: usize,
    pub macd_fast_period: usize,
    pub macd_slow_period: usize,
    pub macd_signal_period: usize,
    pub bb_period: usize,
    pub bb_std_dev: f64,
    pub stop_loss_pct: f64,
    pub take_profit_pct: f64,
    pub max_holding_period: usize,
}

impl Default for MultiIndicatorMinute4Params {
    fn default() -> Self {
        Self {
            ema_short_period: 9,
            ema_long_period: 21,
            macd_fast_period: 12,
            macd_slow_period: 26,
            macd_signal_period: 9,
            bb_period: 20,
            bb_std_dev: 2.0,
            stop_loss_pct: 1.0,
            take_profit_pct: 2.0,
            max_holding_period: 60,
        }
    }
}

pub struct MultiIndicatorMinute4Strategy {
    params: MultiIndicatorMinute4Params,
}

impl MultiIndicatorMinute4Strategy {
    pub fn new(params: MultiIndicatorMinute4Params) -> Self {
        Self { params }
    }
}

impl TradingStrategy for MultiIndicatorMinute4Strategy {
    type Params = MultiIndicatorMinute4Params;

    fn name(&self) -> String {
        "Multi-Indicator Minute Strategy 4".to_string()
    }

    fn timeframe(&self) -> String {
        "minute".to_string()
    }

    fn prepare_data(&self, df: &DataFrame) -> PolarsResult<DataFrame> {
        process_data_with_indicators(df, |processed_df| {
            // Calculate EMA
            let ema_short = calculate_ema(processed_df, "close", self.params.ema_short_period)?;
            let ema_long = calculate_ema(processed_df, "close", self.params.ema_long_period)?;

            // Calculate MACD
            let (macd, macd_signal) = calculate_macd(
                processed_df,
                self.params.macd_fast_period,
                self.params.macd_slow_period,
                self.params.macd_signal_period,
                "close",
            )?;

            // Calculate Bollinger Bands
            let (bb_middle, bb_upper, bb_lower) = calculate_bollinger_bands(
                processed_df,
                self.params.bb_period,
                self.params.bb_std_dev,
                "close",
            )?;

            // Add all indicators to the DataFrame
            let mut result = processed_df.clone();
            let ema_short = ema_short.with_name("ema_short".into());
            let ema_long = ema_long.with_name("ema_long".into());
            let macd = macd.with_name("macd".into());
            let macd_signal = macd_signal.with_name("macd_signal".into());

            result.with_column(ema_short)?;
            result.with_column(ema_long)?;
            result.with_column(macd)?;
            result.with_column(macd_signal)?;
            result.with_column(bb_middle)?;
            result.with_column(bb_upper)?;
            result.with_column(bb_lower)?;

            Ok(result)
        })
    }

    fn generate_signals(&self, df: &DataFrame) -> PolarsResult<Vec<TradeRecord>> {
        let mut trade_records = Vec::new();
        let price = df.column("close")?.f64()?;
        let ema_short = df.column("ema_short")?.f64()?;
        let ema_long = df.column("ema_long")?.f64()?;
        let macd = df.column("macd")?.f64()?;
        let macd_signal = df.column("macd_signal")?.f64()?;
        let bb_upper = df.column("bb_upper")?.f64()?;
        let bb_lower = df.column("bb_lower")?.f64()?;
        let _bb_middle = df.column("bb_middle")?.f64()?;
        let datetime = df.column("datetime")?;

        let mut position: Option<TradePosition> = None;

        for i in self.params.ema_long_period..price.len() {
            let current_price = price.get(i).unwrap_or(f64::NAN);
            if current_price.is_nan() {
                continue;
            }

            // Check if we have an open position
            if let Some(pos) = &position {
                let bars_held = i - pos.entry_index;
                let price_change_pct = (current_price - pos.entry_price) / pos.entry_price * 100.0;

                // Exit conditions
                let stop_loss_triggered = match pos.direction {
                    TradeDirection::Long => price_change_pct <= -self.params.stop_loss_pct,
                    TradeDirection::Short => price_change_pct >= self.params.stop_loss_pct,
                };

                let take_profit_triggered = match pos.direction {
                    TradeDirection::Long => price_change_pct >= self.params.take_profit_pct,
                    TradeDirection::Short => price_change_pct <= -self.params.take_profit_pct,
                };

                let max_holding_time_reached = bars_held >= self.params.max_holding_period;

                // Trend reversal exit
                let trend_reversal = match pos.direction {
                    TradeDirection::Long => {
                        ema_short.get(i).unwrap_or(0.0) < ema_long.get(i).unwrap_or(0.0)
                    }
                    TradeDirection::Short => {
                        ema_short.get(i).unwrap_or(0.0) > ema_long.get(i).unwrap_or(0.0)
                    }
                };

                // Exit position if any exit condition is met
                if stop_loss_triggered
                    || take_profit_triggered
                    || max_holding_time_reached
                    || trend_reversal
                {
                    let exit_reason = if stop_loss_triggered {
                        "Stop Loss"
                    } else if take_profit_triggered {
                        "Take Profit"
                    } else if max_holding_time_reached {
                        "Max Holding Time"
                    } else {
                        "Trend Reversal"
                    };

                    let trade_record = TradeRecord {
                        symbol: "".to_string(), // Will be filled by the backtest engine
                        entry_time: pos.entry_time.clone(),
                        entry_price: pos.entry_price,
                        exit_time: datetime.get(i).unwrap().to_string(),
                        exit_price: current_price,
                        direction: pos.direction.clone(),
                        pnl: match pos.direction {
                            TradeDirection::Long => {
                                (current_price - pos.entry_price) / pos.entry_price * 100.0
                            }
                            TradeDirection::Short => {
                                (pos.entry_price - current_price) / pos.entry_price * 100.0
                            }
                        },
                        exit_reason: exit_reason.to_string(),
                    };

                    trade_records.push(trade_record);
                    position = None;
                }
            } else {
                // Entry conditions for a new position

                // Condition 1: EMA Crossover
                let ema_crossover_bullish = ema_short.get(i).unwrap_or(0.0)
                    > ema_long.get(i).unwrap_or(0.0)
                    && ema_short.get(i - 1).unwrap_or(0.0) <= ema_long.get(i - 1).unwrap_or(0.0);

                let ema_crossover_bearish = ema_short.get(i).unwrap_or(0.0)
                    < ema_long.get(i).unwrap_or(0.0)
                    && ema_short.get(i - 1).unwrap_or(0.0) >= ema_long.get(i - 1).unwrap_or(0.0);

                // Condition 2: MACD Crossover
                let macd_crossover_bullish = macd.get(i).unwrap_or(0.0)
                    > macd_signal.get(i).unwrap_or(0.0)
                    && macd.get(i - 1).unwrap_or(0.0) <= macd_signal.get(i - 1).unwrap_or(0.0);

                let macd_crossover_bearish = macd.get(i).unwrap_or(0.0)
                    < macd_signal.get(i).unwrap_or(0.0)
                    && macd.get(i - 1).unwrap_or(0.0) >= macd_signal.get(i - 1).unwrap_or(0.0);

                // Condition 3: Bollinger Band touch
                let price_near_lower_band = current_price < bb_lower.get(i).unwrap_or(f64::MIN);
                let price_near_upper_band = current_price > bb_upper.get(i).unwrap_or(f64::MAX);

                // Entry signals
                let long_signal =
                    ema_crossover_bullish && macd_crossover_bullish && price_near_lower_band;
                let short_signal =
                    ema_crossover_bearish && macd_crossover_bearish && price_near_upper_band;

                if long_signal {
                    position = Some(TradePosition {
                        entry_price: current_price,
                        entry_time: datetime.get(i).unwrap().to_string(),
                        entry_index: i,
                        direction: TradeDirection::Long,
                    });
                } else if short_signal {
                    position = Some(TradePosition {
                        entry_price: current_price,
                        entry_time: datetime.get(i).unwrap().to_string(),
                        entry_index: i,
                        direction: TradeDirection::Short,
                    });
                }
            }
        }

        Ok(trade_records)
    }

    fn backtest(&self, df: &DataFrame, _params: &DataFetchParams) -> PolarsResult<BacktestSummary> {
        let prepared_data = self.prepare_data(df)?;
        let trade_records = self.generate_signals(&prepared_data)?;

        // Basic statistics
        let mut wins = 0;
        let mut losses = 0;
        let mut total_pnl = 0.0;

        for record in &trade_records {
            if record.pnl > 0.0 {
                wins += 1;
            } else if record.pnl < 0.0 {
                losses += 1;
            }
            total_pnl += record.pnl;
        }

        let win_rate = if !trade_records.is_empty() {
            wins as f64 / trade_records.len() as f64 * 100.0
        } else {
            0.0
        };

        let avg_pnl = if !trade_records.is_empty() {
            total_pnl / trade_records.len() as f64
        } else {
            0.0
        };

        Ok(BacktestSummary {
            strategy_name: self.name(),
            total_trades: trade_records.len(),
            winning_trades: wins,
            losing_trades: losses,
            win_rate,
            average_pnl: avg_pnl,
            total_pnl,
            trade_records,
        })
    }

    fn set_params(&mut self, params: Self::Params) {
        self.params = params;
    }

    fn get_params(&self) -> Self::Params {
        self.params.clone()
    }
}

/// Run the multi-indicator minute 4 strategy on the given DataFrame
///
/// This function is the public entry point for using this strategy.
///
/// # Arguments
///
/// * `df` - DataFrame containing the price data
/// * `params` - Parameters for the strategy
///
/// # Returns
///
/// * `PolarsResult<BacktestSummary>` - Summary of the backtest results
pub fn run_strategy(
    df: &DataFrame,
    params: &MultiIndicatorMinute4Params,
) -> PolarsResult<BacktestSummary> {
    let data_params = DataFetchParams {
        symbol: "".to_string(),
        start_date: "".to_string(),
        end_date: "".to_string(),
        timeframe: "minute".to_string(),
    };

    let strategy = MultiIndicatorMinute4Strategy::new(params.clone());
    strategy.backtest(df, &data_params)
}

/// Type alias for the strategy parameters, for use with the module re-export
pub type StrategyParams = MultiIndicatorMinute4Params;
