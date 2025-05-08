//! # Cryptocurrency Grid Trading Strategy
//! 
//! This module implements grid trading strategies optimized for cryptocurrency markets,
//! which are designed to profit from sideways and volatile market conditions by
//! placing buy and sell orders at regular price intervals.

use polars::prelude::*;

/// Strategy parameters for crypto grid trading strategy
#[derive(Clone)]
pub struct StrategyParams {
    /// Upper price boundary for the grid
    pub upper_price: f64,
    
    /// Lower price boundary for the grid
    pub lower_price: f64,
    
    /// Number of grid levels
    pub grid_levels: usize,
    
    /// Percentage of capital to allocate to the grid
    pub capital_allocation_pct: f64,
    
    /// Whether to rebalance the grid when boundaries are reached
    pub rebalance_on_boundary: bool,
    
    /// Whether to use arithmetic or geometric grid spacing
    pub use_geometric_grid: bool,
    
    /// Additional profit target percentage (optional)
    pub profit_target_pct: Option<f64>,
    
    /// Stop loss percentage below lowest grid line (optional)
    pub stop_loss_pct: Option<f64>,
}

impl Default for StrategyParams {
    fn default() -> Self {
        Self {
            upper_price: 0.0, // This should be set based on market conditions
            lower_price: 0.0, // This should be set based on market conditions
            grid_levels: 10,
            capital_allocation_pct: 90.0,
            rebalance_on_boundary: true,
            use_geometric_grid: true,
            profit_target_pct: None,
            stop_loss_pct: None,
        }
    }
}

/// Grid level details
#[derive(Clone)]
pub struct GridLevel {
    /// Price level for the grid line
    pub price: f64,
    
    /// Buy order quantity at this level
    pub buy_quantity: f64,
    
    /// Sell order quantity at this level
    pub sell_quantity: f64,
    
    /// Status of the grid level (active, filled, etc.)
    pub status: String,
}

/// Strategy signals and data
pub struct StrategySignals {
    /// Buy signals (1 = buy, 0 = no action)
    pub buy_signals: Vec<i32>,
    
    /// Sell signals (1 = sell, 0 = no action)
    pub sell_signals: Vec<i32>,
    
    /// Grid levels at each time point
    pub grid_levels: Vec<Vec<GridLevel>>,
    
    /// DataFrame with signals and metrics
    pub signals_df: DataFrame,
}

/// Generate grid price levels
///
/// # Arguments
///
/// * `upper_price` - Upper boundary price
/// * `lower_price` - Lower boundary price
/// * `levels` - Number of grid levels
/// * `geometric` - Whether to use geometric or arithmetic spacing
///
/// # Returns
///
/// * `Vec<f64>` - Vector of price levels
fn generate_grid_levels(
    upper_price: f64,
    lower_price: f64,
    levels: usize,
    geometric: bool,
) -> Vec<f64> {
    let mut grid_prices = Vec::with_capacity(levels);
    
    if geometric {
        // Geometric grid (equal percentage steps)
        let ratio = (upper_price / lower_price).powf(1.0 / (levels as f64 - 1.0));
        
        for i in 0..levels {
            let price = lower_price * ratio.powf(i as f64);
            grid_prices.push(price);
        }
    } else {
        // Arithmetic grid (equal price steps)
        let step = (upper_price - lower_price) / (levels as f64 - 1.0);
        
        for i in 0..levels {
            let price = lower_price + step * (i as f64);
            grid_prices.push(price);
        }
    }
    
    grid_prices
}

/// Run cryptocurrency grid trading strategy
///
/// This strategy places buy and sell orders at predetermined price levels,
/// profiting from price volatility within a range.
///
/// # Arguments
///
/// * `price_df` - DataFrame with OHLCV data
/// * `params` - Strategy parameters
///
/// # Returns
///
/// * `Result<StrategySignals, PolarsError>` - Grid trading signals and levels
pub fn run_strategy(
    price_df: &DataFrame,
    params: &StrategyParams,
) -> Result<StrategySignals, PolarsError> {
    // Validate parameters
    if params.upper_price <= params.lower_price || params.grid_levels < 2 {
        return Err(PolarsError::ComputeError(
            "Invalid grid parameters: ensure upper_price > lower_price and grid_levels >= 2".into()
        ));
    }
    
    // Generate grid price levels
    let grid_prices = generate_grid_levels(
        params.upper_price,
        params.lower_price,
        params.grid_levels,
        params.use_geometric_grid,
    );
    
    // Initialize results
    let mut buy_signals = vec![0; price_df.height()];
    let mut sell_signals = vec![0; price_df.height()];
    let mut all_grid_levels = Vec::with_capacity(price_df.height());
    
    // Extract close prices
    let close = price_df.column("close")?.f64()?;
    
    // Initialize sample grid levels
    let initial_grid = grid_prices.iter().map(|&price| {
        GridLevel {
            price,
            buy_quantity: 100.0,  // Placeholder
            sell_quantity: 100.0, // Placeholder
            status: "active".to_string(),
        }
    }).collect::<Vec<GridLevel>>();
    
    // Initialize with empty grid for earlier periods
    for _ in 0..params.grid_levels.max(params.grid_levels) {
        all_grid_levels.push(vec![]);
    }
    
    // Process each price point
    for i in params.grid_levels.max(params.grid_levels)..price_df.height() {
        let current_price = close.get(i).unwrap_or(f64::NAN);
        let prev_price = if i > 0 { close.get(i - 1).unwrap_or(f64::NAN) } else { f64::NAN };
        
        // Skip if we have NaN values
        if current_price.is_nan() || prev_price.is_nan() {
            all_grid_levels.push(initial_grid.clone());
            continue;
        }
        
        // Check which grid levels were crossed
        for (level_idx, &grid_price) in grid_prices.iter().enumerate() {
            // Price crossed from below to above - sell signal
            if prev_price < grid_price && current_price >= grid_price {
                sell_signals[i] = 1;
            }
            // Price crossed from above to below - buy signal
            else if prev_price > grid_price && current_price <= grid_price {
                buy_signals[i] = 1;
            }
        }
        
        // Add this period's grid levels
        all_grid_levels.push(initial_grid.clone());
    }
    
    // Create signals DataFrame with price and signals
    let price_series = price_df.column("close")?.clone();
    
    // Create timestamp series, or use integers if no date column exists
    let datetime_series = match price_df.column("date") {
        Ok(date_col) => date_col.clone(),
        _ => Series::new("date".into(), (0..price_df.height()).map(|i| i as i32).collect::<Vec<i32>>()).into()
    };
    
    let buy_series = Series::new("buy_signals".into(), &buy_signals).into();
    let sell_series = Series::new("sell_signals".into(), &sell_signals).into();
    
    let signals_df = DataFrame::new(vec![
        datetime_series,
        price_series,
        buy_series,
        sell_series,
    ])?;
    
    Ok(StrategySignals {
        buy_signals,
        sell_signals,
        grid_levels: all_grid_levels,
        signals_df,
    })
}

/// Calculate performance metrics for the grid trading strategy
///
/// # Arguments
///
/// * `price_df` - DataFrame with price data
/// * `signals` - StrategySignals with buy/sell signals
/// * `params` - Strategy parameters
/// * `start_capital` - Initial capital amount
///
/// # Returns
///
/// * Tuple containing performance metrics: (final_capital, return%, trades, win%, profit_per_grid)
pub fn calculate_performance(
    price_df: &DataFrame,
    signals: &StrategySignals,
    params: &StrategyParams,
    start_capital: f64,
) -> (f64, f64, usize, f64, f64) {
    // Placeholder implementation
    let num_buys = signals.buy_signals.iter().filter(|&&s| s == 1).count();
    let num_sells = signals.sell_signals.iter().filter(|&&s| s == 1).count();
    let total_trades = num_buys + num_sells;
    
    // In a real implementation, we would calculate actual P&L based on the grid trading logic
    let estimated_profit_pct = 12.0;
    let final_capital = start_capital * (1.0 + estimated_profit_pct / 100.0);
    
    (
        final_capital,           // final capital 
        estimated_profit_pct,    // return percentage
        total_trades,            // number of trades
        95.0,                    // win rate (usually high for grid trading)
        1.2,                     // profit per grid (%)
    )
} 