//! # Options Trading Module
//! 
//! This module provides tools and utilities specifically for options markets.
//! It handles options-specific trading concepts including:
//! 
//! - Implied Volatility (IV) calculations and analysis
//! - Options chain handling and filtering
//! - Options strategy evaluation (spreads, straddles, condors, etc.)
//! - Options Greeks calculation and visualization
//! - Probability analysis and expected value calculations

use polars::prelude::*;
use std::collections::HashMap;

/// Basic functions for options trading
pub mod options_trading {
    use super::*;

    /// Simple Black-Scholes model for option pricing
    struct BlackScholes {
        price: f64,
        strike: f64,
        time_to_expiry: f64,  // in years
        risk_free_rate: f64,
        volatility: f64,
    }

    impl BlackScholes {
        /// Calculate d1 in the Black-Scholes formula
        fn d1(&self) -> f64 {
            let numerator = (self.price / self.strike).ln() + 
                (self.risk_free_rate + 0.5 * self.volatility.powi(2)) * self.time_to_expiry;
            let denominator = self.volatility * self.time_to_expiry.sqrt();
            numerator / denominator
        }

        /// Calculate d2 in the Black-Scholes formula
        fn d2(&self) -> f64 {
            self.d1() - self.volatility * self.time_to_expiry.sqrt()
        }

        /// Normal cumulative distribution function approximation
        fn norm_cdf(x: f64) -> f64 {
            // Simple approximation of the normal CDF
            if x > 6.0 {
                1.0
            } else if x < -6.0 {
                0.0
            } else {
                let b1 = 0.31938153;
                let b2 = -0.356563782;
                let b3 = 1.781477937;
                let b4 = -1.821255978;
                let b5 = 1.330274429;
                let p = 0.2316419;
                let c = 0.39894228;
                
                let t = 1.0 / (1.0 + p * x.abs());
                let poly = t * (b1 + t * (b2 + t * (b3 + t * (b4 + t * b5))));
                
                if x >= 0.0 {
                    1.0 - c * (-x * x / 2.0).exp() * poly
                } else {
                    c * (-x * x / 2.0).exp() * poly
                }
            }
        }

        /// Calculate call option price
        pub fn call_price(&self) -> f64 {
            let d1 = self.d1();
            let d2 = self.d2();
            self.price * Self::norm_cdf(d1) - 
                self.strike * (-self.risk_free_rate * self.time_to_expiry).exp() * Self::norm_cdf(d2)
        }

        /// Calculate put option price
        pub fn put_price(&self) -> f64 {
            let d1 = self.d1();
            let d2 = self.d2();
            self.strike * (-self.risk_free_rate * self.time_to_expiry).exp() * Self::norm_cdf(-d2) - 
                self.price * Self::norm_cdf(-d1)
        }
    }

    /// Calculate implied volatility from option price
    /// 
    /// Uses bisection method to find the volatility that matches the market price
    /// 
    /// # Arguments
    /// 
    /// * `price` - Underlying price
    /// * `strike` - Option strike price
    /// * `time_to_expiry` - Time to expiration in years
    /// * `risk_free_rate` - Risk-free interest rate
    /// * `option_price` - Market price of the option
    /// * `is_call` - Whether this is a call option
    /// 
    /// # Returns
    /// 
    /// Implied volatility as a decimal (e.g., 0.25 for 25%)
    pub fn calculate_implied_volatility(
        price: f64,
        strike: f64,
        time_to_expiry: f64,
        risk_free_rate: f64,
        option_price: f64,
        is_call: bool,
    ) -> f64 {
        // Use bisection method to find IV
        let mut low = 0.001;
        let mut high = 4.0; // 400% volatility as upper bound
        let mut mid;
        let accuracy = 0.0001;
        let max_iterations = 100;
        
        for _ in 0..max_iterations {
            mid = (low + high) / 2.0;
            
            let model = BlackScholes {
                price,
                strike,
                time_to_expiry,
                risk_free_rate,
                volatility: mid,
            };
            
            let model_price = if is_call {
                model.call_price()
            } else {
                model.put_price()
            };
            
            let price_diff = model_price - option_price;
            
            if price_diff.abs() < accuracy {
                return mid;
            }
            
            if price_diff > 0.0 {
                high = mid;
            } else {
                low = mid;
            }
        }
        
        // Return best estimate after max iterations
        (low + high) / 2.0
    }

    /// Analyze options chain for a security
    /// 
    /// Processes a complete options chain to calculate Greeks,
    /// implied volatility, and identify potential trading opportunities.
    /// 
    /// # Arguments
    /// 
    /// * `underlying_price` - Current price of the underlying
    /// * `options_data` - DataFrame containing options chain data
    /// 
    /// # Returns
    /// 
    /// DataFrame with enhanced options analysis
    pub fn analyze_options_chain(
        underlying_price: f64,
        options_data: &DataFrame,
    ) -> PolarsResult<DataFrame> {
        // This is a placeholder for options chain analysis
        // A full implementation would:
        // 1. Calculate IV for each option
        // 2. Compute all Greeks
        // 3. Find mispriced options
        // 4. Identify optimal strike selections
        
        Ok(options_data.clone())
    }

    /// Evaluate an options strategy
    /// 
    /// Analyzes the risk/reward profile of an options strategy
    /// like vertical spreads, iron condors, etc.
    /// 
    /// # Arguments
    /// 
    /// * `underlying_price` - Current price of the underlying
    /// * `strategy_legs` - Vector of option positions that make up the strategy
    /// * `price_range` - Range of prices to analyze for the underlying
    /// 
    /// # Returns
    /// 
    /// DataFrame with profit/loss at different price levels and dates
    pub fn evaluate_options_strategy(
        underlying_price: f64,
        strategy_legs: Vec<(f64, f64, bool, i32)>, // (strike, expiry, is_call, quantity)
        price_range: (f64, f64)
    ) -> PolarsResult<DataFrame> {
        // This is a placeholder for options strategy evaluation
        // A full implementation would:
        // 1. Generate a price grid
        // 2. Calculate P/L for each leg at each price point
        // 3. Combine the legs to get strategy P/L
        // 4. Calculate key metrics like max profit, max loss, breakevens
        
        // Create a simple DataFrame with results
        let price_points: Vec<f64> = (0..21)
            .map(|i| price_range.0 + i as f64 * (price_range.1 - price_range.0) / 20.0)
            .collect();
            
        let mut pnl_values = Vec::new();
        
        // Simple calculation (placeholder)
        for price in &price_points {
            let mut strategy_pnl = 0.0;
            
            for &(strike, _expiry, is_call, quantity) in &strategy_legs {
                let intrinsic = if is_call {
                    (*price - strike).max(0.0)
                } else {
                    (strike - *price).max(0.0)
                };
                
                strategy_pnl += intrinsic * quantity as f64;
            }
            
            pnl_values.push(strategy_pnl);
        }
        
        // Create DataFrame with results
        let df = DataFrame::new(vec![
            Series::new("price", price_points),
            Series::new("pnl", pnl_values)
        ])?;
        
        Ok(df)
    }
}

//! # Options Trading Indicators
//! 
//! This module provides specialized technical indicators optimized for
//! options trading across different strategies and timeframes.
//!
//! ## Included Indicators
//!
//! * Implied Volatility Analysis - IV-based indicators for price movement prediction
//! * Greeks Analysis - Option Greeks indicators and their rate of change
//! * Spread Analysis - Multi-leg option strategy indicators
//! * Volume Analysis - Volume and open interest based indicators for options
//! * Skew Analysis - Indicators based on volatility skew across strikes

mod volatility_analysis;
mod greeks;
mod spreads;
mod volume_analysis;
mod skew_analysis;

// Re-export the public functions
pub use volatility_analysis::*;
pub use greeks::*;
pub use spreads::*;
pub use volume_analysis::*;
pub use skew_analysis::*;

/// Calculate common options trading indicators
///
/// Adds a suite of indicators specifically optimized for options trading
/// analysis and strategy development.
///
/// # Arguments
///
/// * `df` - DataFrame with options data (must include columns for price, strike, expiry, etc.)
///
/// # Returns
///
/// * `PolarsResult<DataFrame>` - DataFrame with added options indicators
pub fn add_options_indicators(df: &DataFrame) -> PolarsResult<DataFrame> {
    let mut result = df.clone();
    
    // Add implied volatility analysis
    volatility_analysis::add_volatility_indicators(&mut result)?;
    
    // Add Greeks analysis
    greeks::add_greeks_indicators(&mut result)?;
    
    // Add spread analysis
    spreads::add_spread_indicators(&mut result)?;
    
    // Add volume analysis
    volume_analysis::add_volume_indicators(&mut result)?;
    
    // Add skew analysis
    skew_analysis::add_skew_indicators(&mut result)?;
    
    Ok(result)
}

/// Generate combined options trading signals
///
/// This function combines signals from various indicators to generate
/// more robust entry and exit points for options trading strategies.
///
/// # Arguments
///
/// * `df` - DataFrame with previously calculated options indicators
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with combined signals (2: strong buy, 1: moderate buy,
///                           0: neutral, -1: moderate sell, -2: strong sell)
pub fn generate_options_trading_signals(df: &DataFrame) -> PolarsResult<Series> {
    // Check if necessary indicators exist
    let required_indicators = [
        "iv_percentile", "iv_forecast", "delta", 
        "gamma_exposure", "volume_oi_ratio"
    ];
    
    for indicator in required_indicators {
        if !df.schema().contains(indicator) {
            return Err(PolarsError::ComputeError(
                format!("Required indicator '{}' not found", indicator).into(),
            ));
        }
    }
    
    // Extract indicator values
    let iv_percentile = df.column("iv_percentile")?.f64()?;
    let iv_forecast = df.column("iv_forecast")?.f64()?;
    let delta = df.column("delta")?.f64()?;
    let gamma_exposure = df.column("gamma_exposure")?.f64()?;
    let volume_oi_ratio = df.column("volume_oi_ratio")?.f64()?;
    
    // Get volatility skew if available
    let has_skew = df.schema().contains("volatility_skew");
    let vol_skew = if has_skew {
        Some(df.column("volatility_skew")?.f64()?)
    } else {
        None
    };
    
    let mut combined_signals = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let iv_pct = iv_percentile.get(i).unwrap_or(f64::NAN);
        let iv_fcst = iv_forecast.get(i).unwrap_or(f64::NAN);
        let delta_val = delta.get(i).unwrap_or(f64::NAN);
        let gamma_val = gamma_exposure.get(i).unwrap_or(f64::NAN);
        let vol_oi = volume_oi_ratio.get(i).unwrap_or(f64::NAN);
        
        // Skip if we don't have valid data
        if iv_pct.is_nan() || iv_fcst.is_nan() || delta_val.is_nan() || gamma_val.is_nan() || vol_oi.is_nan() {
            combined_signals.push(0);
            continue;
        }
        
        // Count bullish and bearish signals
        let mut bullish_count = 0;
        let mut bearish_count = 0;
        
        // IV percentile (high = potentially bullish for selling premium)
        if iv_pct > 80.0 { bearish_count += 1; } // IV is high - good for selling premium
        if iv_pct < 20.0 { bullish_count += 1; } // IV is low - good for buying premium
        
        // IV forecast (higher than current = potentially bullish)
        if iv_fcst > iv_pct * 1.1 { bullish_count += 1; } // IV expected to increase
        if iv_fcst < iv_pct * 0.9 { bearish_count += 1; } // IV expected to decrease
        
        // Delta exposure (absolute, adjusted for direction)
        let abs_delta = delta_val.abs();
        if abs_delta > 0.6 { bullish_count += 1; } // Strong directional exposure
        if abs_delta < 0.3 { bearish_count += 1; } // Weak directional exposure
        
        // Gamma exposure (high gamma = higher risk/reward)
        if gamma_val > 0.05 { bullish_count += 1; } // High gamma, potential for acceleration
        if gamma_val < 0.01 { bearish_count += 1; } // Low gamma, less leverage
        
        // Volume/OI ratio (high = increased activity)
        if vol_oi > 1.5 { bullish_count += 1; } // Increasing volume relative to OI
        if vol_oi < 0.5 { bearish_count += 1; } // Decreasing volume relative to OI
        
        // Factor in volatility skew if available
        if let Some(skew) = &vol_skew {
            let skew_val = skew.get(i).unwrap_or(f64::NAN);
            if !skew_val.is_nan() {
                if skew_val > 0.1 { bearish_count += 1; } // High put skew - market expecting downside
                if skew_val < -0.1 { bullish_count += 1; } // Low put skew - market expecting upside
            }
        }
        
        // Generate combined signal
        if bullish_count >= 3 && bearish_count == 0 {
            combined_signals.push(2); // Strong buy
        } else if bullish_count > bearish_count {
            combined_signals.push(1); // Moderate buy
        } else if bearish_count >= 3 && bullish_count == 0 {
            combined_signals.push(-2); // Strong sell
        } else if bearish_count > bullish_count {
            combined_signals.push(-1); // Moderate sell
        } else {
            combined_signals.push(0); // Neutral
        }
    }
    
    Ok(Series::new("options_trading_signal", combined_signals))
}

/// Calculate optimal position sizing for options trades
///
/// This function suggests appropriate position sizes based on
/// risk metrics specific to options trading.
///
/// # Arguments
///
/// * `df` - DataFrame with options indicators
/// * `risk_capital` - Amount of capital willing to risk per trade
/// * `max_loss_pct` - Maximum percentage loss willing to accept
///
/// # Returns
///
/// * `PolarsResult<Series>` - Series with suggested position sizes (in contracts)
pub fn calculate_options_position_sizing(
    df: &DataFrame,
    risk_capital: f64,
    max_loss_pct: f64,
) -> PolarsResult<Series> {
    // Check if necessary columns exist
    for col in ["option_price", "max_loss"].iter() {
        if !df.schema().contains(col) {
            return Err(PolarsError::ComputeError(
                format!("Required column '{}' not found", col).into(),
            ));
        }
    }
    
    let price = df.column("option_price")?.f64()?;
    let max_loss = df.column("max_loss")?.f64()?;
    
    // Get Greeks if available for adjusting position sizing
    let has_theta = df.schema().contains("theta");
    let theta = if has_theta {
        Some(df.column("theta")?.f64()?)
    } else {
        None
    };
    
    let has_gamma = df.schema().contains("gamma");
    let gamma = if has_gamma {
        Some(df.column("gamma")?.f64()?)
    } else {
        None
    };
    
    let mut position_sizes = Vec::with_capacity(df.height());
    
    for i in 0..df.height() {
        let option_price = price.get(i).unwrap_or(f64::NAN);
        let option_max_loss = max_loss.get(i).unwrap_or(f64::NAN);
        
        if option_price.is_nan() || option_max_loss.is_nan() || option_price <= 0.0 || option_max_loss <= 0.0 {
            position_sizes.push(0);
            continue;
        }
        
        // Base calculation using risk capital and max loss
        let max_risk = risk_capital * max_loss_pct / 100.0;
        let mut contracts = (max_risk / option_max_loss).floor();
        
        // Adjust for theta decay if available
        if let Some(theta_vals) = &theta {
            let theta_val = theta_vals.get(i).unwrap_or(f64::NAN);
            if !theta_val.is_nan() && theta_val < 0.0 {
                // Reduce position size if theta decay is high
                let theta_factor = (1.0 + (theta_val.abs() / option_price).min(0.5));
                contracts = (contracts / theta_factor).floor();
            }
        }
        
        // Adjust for gamma risk if available
        if let Some(gamma_vals) = &gamma {
            let gamma_val = gamma_vals.get(i).unwrap_or(f64::NAN);
            if !gamma_val.is_nan() && gamma_val > 0.05 {
                // High gamma means higher risk, reduce position size
                let gamma_factor = 1.0 + (gamma_val * 10.0).min(1.0);
                contracts = (contracts / gamma_factor).floor();
            }
        }
        
        // Ensure at least 1 contract if any position is suggested
        if contracts > 0.0 && contracts < 1.0 {
            contracts = 1.0;
        }
        
        position_sizes.push(contracts as i32);
    }
    
    Ok(Series::new("suggested_contracts", position_sizes))
} 