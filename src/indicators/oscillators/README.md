# Oscillators

Oscillators are technical indicators that fluctuate within a bounded range, helping traders identify overbought or oversold conditions and potential reversal points in the market. Unlike trending indicators, oscillators are most useful in non-trending, range-bound markets.

## Types of Oscillators

This library implements several common oscillators:

### Relative Strength Index (RSI)

RSI measures the speed and change of price movements on a scale from 0 to 100. It helps identify overbought and oversold conditions.

```rust
let rsi = calculate_rsi(&dataframe, 14, "close")?;
```

**Parameters:**
- `dataframe`: The price data
- `window`: Lookback period (typically 14)
- `column_name`: The column to calculate RSI on (typically "close")

**Interpretation:**
- RSI > 70: Potentially overbought condition (sell signal)
- RSI < 30: Potentially oversold condition (buy signal)
- RSI = 50: Neutral market
- Divergence with price: Potential trend reversal
- RSI trending above 50: Bullish momentum
- RSI trending below 50: Bearish momentum

**Common parameters:**
- Standard: 14 periods
- More sensitive: 9-11 periods
- Less sensitive: 21-25 periods

### Moving Average Convergence Divergence (MACD)

MACD is a trend-following momentum indicator that shows the relationship between two moving averages of a security's price.

```rust
let (macd, signal) = calculate_macd(&dataframe, 12, 26, 9, "close")?;
```

**Parameters:**
- `dataframe`: The price data
- `fast_period`: Fast EMA period (typically 12)
- `slow_period`: Slow EMA period (typically 26)
- `signal_period`: Signal line EMA period (typically 9)
- `column_name`: The column to calculate MACD on (typically "close")

**Interpretation:**
- MACD crossing above signal line: Bullish signal
- MACD crossing below signal line: Bearish signal
- MACD above zero line: Bullish trend
- MACD below zero line: Bearish trend
- MACD histogram increasing: Increasing momentum
- MACD histogram decreasing: Decreasing momentum
- Divergence with price: Potential trend reversal

**Common parameters:**
- Standard: Fast=12, Slow=26, Signal=9
- Faster: Fast=8, Slow=17, Signal=9
- Weekly charts: Fast=19, Slow=39, Signal=9

## Trading Strategies with Oscillators

### RSI Trading Strategies

1. **Overbought/Oversold Strategy**
   - Buy when RSI falls below 30 and then rises back above 30
   - Sell when RSI rises above 70 and then falls back below 70

2. **Divergence Strategy**
   - Bullish divergence: Price makes lower lows while RSI makes higher lows
   - Bearish divergence: Price makes higher highs while RSI makes lower highs

3. **Centerline (50) Strategy**
   - Buy when RSI crosses above 50 in an uptrend
   - Sell when RSI crosses below 50 in a downtrend

### MACD Trading Strategies

1. **Signal Line Crossover**
   - Buy when MACD crosses above the signal line
   - Sell when MACD crosses below the signal line

2. **Zero Line Crossover**
   - Buy when MACD crosses above the zero line
   - Sell when MACD crosses below the zero line

3. **Divergence Strategy**
   - Bullish divergence: Price makes lower lows while MACD makes higher lows
   - Bearish divergence: Price makes higher highs while MACD makes lower highs

4. **Histogram Analysis**
   - Buy when histogram turns positive and increasing
   - Sell when histogram turns negative and decreasing

## Limitations

- **False signals**: Oscillators can generate false signals during strong trends
- **Late signals**: By the time extreme readings occur, the price move may be nearly complete
- **Subjective interpretation**: Divergences and other patterns can be subjective
- **Parameter sensitivity**: Results can vary significantly with different period settings

## Best Practices

- Use oscillators primarily in range-bound markets rather than trending markets
- Combine oscillators with trend indicators for confirmation
- Look for divergences between price and oscillator readings
- Adjust parameters based on the volatility of the asset and your trading timeframe
- Be cautious with overbought/oversold signals during strong trends
- Consider using multiple timeframes for confirmation 