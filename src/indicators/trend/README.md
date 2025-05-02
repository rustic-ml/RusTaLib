# Trend Indicators

Trend indicators help traders identify the direction and strength of market trends. They are designed to signal when a market is trending up, down, or moving sideways. These indicators are most useful in trending markets and can help traders avoid false signals in range-bound conditions.

## Types of Trend Indicators

This library implements several trend indicators:

### Average Directional Index (ADX)

ADX measures the strength of a trend, regardless of whether it's up or down. It doesn't indicate trend direction, only strength.

```rust
let adx = calculate_adx(&dataframe, 14)?;
```

**Parameters:**
- `dataframe`: The price data with high, low, and close columns
- `window`: Period for ADX calculation (typically 14)

**Interpretation:**
- ADX < 20: Weak trend or no trend (ranging market)
- ADX > 25: Strong trend present
- ADX > 40: Very strong trend
- ADX > 60: Extremely strong trend
- Rising ADX: Trend strengthening
- Falling ADX: Trend weakening

**Common parameters:**
- Standard: 14 periods
- More responsive: 7-10 periods
- Less responsive: 20-30 periods

### ADX Rating (ADXR)

ADXR is the average of the current ADX and the ADX from n periods ago. It provides a smoother reading of trend strength.

```rust
let adxr = calculate_adxr(&dataframe, 14)?;
```

**Parameters:**
- Similar to ADX

**Interpretation:**
- Similar to ADX but with smoother signals
- Changes in ADXR can provide early indications of trend reversals

### Directional Movement Index (DMI)

DMI consists of two indicators: +DI (Plus Directional Indicator) and -DI (Minus Directional Indicator). These help determine trend direction along with ADX.

```rust
let plus_di = calculate_plus_di(&dataframe, 14)?;
let minus_di = calculate_minus_di(&dataframe, 14)?;
```

**Parameters:**
- `dataframe`: The price data with high, low, and close columns
- `window`: Period for calculation (typically 14)

**Interpretation:**
- +DI > -DI: Bullish trend
- -DI > +DI: Bearish trend
- +DI crossing above -DI: Potential buy signal
- -DI crossing above +DI: Potential sell signal
- Wide separation between +DI and -DI: Strong trend

### Directional Movement (DM)

+DM and -DM are the building blocks of the DMI system, measuring upward and downward price movements.

```rust
let plus_dm = calculate_plus_dm(&dataframe, 14)?;
let minus_dm = calculate_minus_dm(&dataframe, 14)?;
```

**Parameters:**
- Similar to DMI

**Interpretation:**
- Used primarily as components for other indicators like ADX and DMI

### Aroon Indicator

Aroon consists of Aroon Up and Aroon Down lines, which measure the time since the highest high and lowest low within a given period. It helps identify trend changes and consolidations.

```rust
let (aroon_up, aroon_down) = calculate_aroon(&dataframe, 25)?;
```

**Parameters:**
- `dataframe`: The price data with high and low columns
- `window`: Period for calculation (typically 25)

**Interpretation:**
- Aroon Up > 70 and Aroon Down < 30: Strong uptrend
- Aroon Down > 70 and Aroon Up < 30: Strong downtrend
- Both indicators moving lower (below 50): Consolidation period
- Crossovers: Potential trend changes

**Common parameters:**
- Standard: 25 periods
- Short-term: 10-14 periods

### Aroon Oscillator

The Aroon Oscillator is simply Aroon Up minus Aroon Down, creating a single line oscillator that fluctuates between -100 and +100.

```rust
let aroon_osc = calculate_aroon_oscillator(&dataframe, 25)?;
```

**Parameters:**
- Same as Aroon

**Interpretation:**
- Values above zero: Bullish
- Values below zero: Bearish
- Moving toward +100: Strengthening uptrend
- Moving toward -100: Strengthening downtrend
- Oscillating around zero: No clear trend or consolidation

## Trading Strategies with Trend Indicators

### ADX-DMI Trading System

1. **Trend Strength Filter**
   - Only take trades when ADX > 25 (strong trend)
   - For stronger filtering, only trade when ADX is rising

2. **Directional Trading Strategy**
   - Buy when +DI crosses above -DI and ADX > 25
   - Sell when -DI crosses above +DI and ADX > 25

3. **Trend Exhaustion Strategy**
   - Look for ADX > 45-50 and starting to decline
   - Consider taking profits or preparing for potential reversal

### Aroon Trading Strategies

1. **Aroon Crossover Strategy**
   - Buy when Aroon Up crosses above Aroon Down
   - Sell when Aroon Down crosses above Aroon Up

2. **Strong Trend Strategy**
   - Buy when Aroon Up > 80 and Aroon Down < 20
   - Sell when Aroon Down > 80 and Aroon Up < 20

3. **Consolidation Breakout Strategy**
   - Identify when both Aroon indicators are below 50 (consolidation)
   - Enter position when one of the indicators moves above 70 (breakout)

## Limitations

- **Lag**: All trend indicators have some degree of lag and may miss the beginning of trends
- **False signals**: In choppy markets, trend indicators can generate many false signals
- **Volatility sensitivity**: Sharp price movements can cause temporary distortions
- **Sensitivity to period settings**: Results can vary significantly with different period settings

## Best Practices

- Use trend indicators in combination with each other for confirmation
- Adjust parameters based on the time frame and volatility of the asset
- Always consider the broader market context when interpreting signals
- Use trend indicators to stay in alignment with the major trend
- Combine with momentum indicators or oscillators for entry/exit timing
- Be patient - trend following requires discipline to let profitable trends develop 