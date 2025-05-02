# Volatility Indicators

Volatility indicators measure the magnitude of price movements or market fluctuations over time. They help traders assess market conditions, identify potential breakouts, and manage risk by quantifying the degree of price variation.

## Types of Volatility Indicators

This library implements several volatility indicators:

### Bollinger Bands

Bollinger Bands consist of three lines: a middle band (simple moving average) and two outer bands that are standard deviations away from the middle band. They expand and contract based on market volatility.

```rust
let (middle, upper, lower) = calculate_bollinger_bands(&dataframe, 20, 2.0, "close")?;
```

**Parameters:**
- `dataframe`: The price data
- `window`: Period for the middle band SMA (typically 20)
- `num_std_dev`: Number of standard deviations for the bands (typically 2.0)
- `column_name`: The column to calculate bands on (typically "close")

**Interpretation:**
- Price touching upper band: Potentially overbought
- Price touching lower band: Potentially oversold
- Bands widening: Increasing volatility
- Bands narrowing (squeeze): Decreasing volatility, potential breakout coming
- Price bouncing between bands: Range-bound market
- Price breaking outside bands: Strong trend or potential reversal

**Common parameters:**
- Standard: 20 periods, 2 standard deviations
- More sensitive: 10 periods, 1.5 standard deviations
- Less sensitive: 50 periods, 2.5 standard deviations

### Bollinger Band %B

%B indicates where the current price is in relation to the Bollinger Bands. It ranges from 0 to 1, where 0 indicates price at the lower band and 1 indicates price at the upper band.

```rust
let band_b = calculate_bollinger_band_b(&dataframe, 20, 2.0, "close")?;
```

**Parameters:**
- Same as Bollinger Bands

**Interpretation:**
- %B > 1.0: Price above upper band (strongly overbought)
- %B = 1.0: Price at upper band (overbought)
- %B = 0.5: Price at middle band (neutral)
- %B = 0.0: Price at lower band (oversold)
- %B < 0.0: Price below lower band (strongly oversold)

### Average True Range (ATR)

ATR measures market volatility by calculating the average range between high and low prices, adjusted for gaps.

```rust
let atr = calculate_atr(&dataframe, 14)?;
```

**Parameters:**
- `dataframe`: The price data with high, low, and close columns
- `window`: Period for ATR calculation (typically 14)

**Interpretation:**
- Higher ATR: Higher volatility
- Lower ATR: Lower volatility
- ATR increasing: Volatility increasing (often during trends)
- ATR decreasing: Volatility decreasing (often during consolidation)

**Common parameters:**
- Short-term: 7-10 periods
- Standard: 14 periods
- Long-term: 20-30 periods

### Garman-Klass Volatility

Garman-Klass volatility uses open, high, low, and close prices to estimate historical volatility more efficiently than traditional methods.

```rust
let gk_vol = calculate_gk_volatility(&dataframe, 20)?;
```

**Parameters:**
- `dataframe`: The price data with open, high, low, and close columns
- `window`: Period for volatility calculation

**Interpretation:**
- Similar to other volatility measures but potentially more accurate
- Used mainly for volatility analysis rather than direct trading signals

## Trading Strategies with Volatility Indicators

### Bollinger Band Strategies

1. **Mean Reversion Strategy**
   - Buy when price touches lower band and starts moving up
   - Sell when price touches upper band and starts moving down

2. **Bollinger Band Squeeze**
   - Identify when bands narrow significantly (low volatility)
   - Enter trade in direction of the breakout from the squeeze

3. **Bollinger Band Trend Strategy**
   - In strong uptrends, buy when price pulls back to middle band
   - In strong downtrends, sell when price rallies to middle band

### ATR-Based Position Sizing

ATR can be used to determine position size and place stops:

```rust
let stop_loss_distance = atr_value * 2.0; // 2 times ATR
let position_size = risk_amount / stop_loss_distance;
```

### Volatility Breakout Strategy

1. Identify periods of low volatility (low ATR or narrow Bollinger Bands)
2. Set entry points just outside recent price range
3. Enter trade when price breaks out, as volatility often expands after contraction

## Limitations

- **Lagging nature**: Volatility indicators are based on historical price data
- **False signals**: Temporary volatility spikes can cause misleading readings
- **Parameter sensitivity**: Results can vary significantly with different period settings
- **Market regime dependency**: What works in high volatility might not work in low volatility

## Best Practices

- Adjust parameters based on the typical volatility profile of the asset
- Use longer periods for less volatile instruments, shorter for more volatile ones
- Combine volatility indicators with trend and momentum indicators
- Be aware of upcoming news events that might cause volatility spikes
- Consider volatility cycles and mean reversion in your analysis
- Use ATR for stop placement and position sizing to manage risk 