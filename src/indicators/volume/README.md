# Volume Indicators

Volume indicators incorporate trading volume data to provide insights into the strength of price movements and market trends. Volume is a critical component of market analysis, as it helps confirm price trends, identify potential reversals, and gauge market participation.

## Types of Volume Indicators

This library implements several volume-based indicators:

### On-Balance Volume (OBV)

OBV is a cumulative indicator that adds volume on up days and subtracts volume on down days, creating a running total. It helps confirm price trends and identify potential divergences.

```rust
let obv = calculate_obv(&dataframe)?;
```

**Parameters:**
- `dataframe`: The price data with close and volume columns

**Interpretation:**
- OBV rising with price: Strong uptrend confirmation
- OBV falling with price: Strong downtrend confirmation
- OBV rising while price falling: Potential bullish divergence (reversal up)
- OBV falling while price rising: Potential bearish divergence (reversal down)
- OBV breakouts: Can precede price breakouts

### Chaikin Money Flow (CMF)

CMF measures the Money Flow Volume over a specified period. It indicates buying and selling pressure by analyzing both price and volume.

```rust
let cmf = calculate_cmf(&dataframe, 20)?;
```

**Parameters:**
- `dataframe`: The price data with high, low, close, and volume columns
- `window`: Period for calculation (typically 20)

**Interpretation:**
- CMF > 0: Accumulation (buying pressure)
- CMF < 0: Distribution (selling pressure)
- CMF > 0.25: Strong buying pressure
- CMF < -0.25: Strong selling pressure
- CMF trend changes: Potential shift in market sentiment

**Common parameters:**
- Short-term: 10-15 periods
- Standard: 20-21 periods

## Trading Strategies with Volume Indicators

### OBV Trading Strategies

1. **Trend Confirmation Strategy**
   - Buy when price and OBV are both rising
   - Sell when price and OBV are both falling

2. **Divergence Strategy**
   - Bullish divergence: Buy when OBV makes higher lows while price makes lower lows
   - Bearish divergence: Sell when OBV makes lower highs while price makes higher highs

3. **OBV Breakout Strategy**
   - Enter trades when OBV breaks out of a consolidation pattern
   - Use price action to confirm the breakout

### CMF Trading Strategies

1. **Zero Line Cross Strategy**
   - Buy when CMF crosses above zero (accumulation begins)
   - Sell when CMF crosses below zero (distribution begins)

2. **Extreme Readings Strategy**
   - Buy when CMF rebounds from extreme negative readings (-0.25 or lower)
   - Sell when CMF drops from extreme positive readings (0.25 or higher)

3. **Trend Confirmation Strategy**
   - In uptrends, buy when CMF is positive and rising
   - In downtrends, sell when CMF is negative and falling

## Volume Analysis Principles

1. **Volume Precedes Price**: Often, volume changes occur before price movements
2. **Volume Confirms Trends**: Healthy trends show increasing volume in the direction of the trend
3. **Volume at Key Levels**: High volume at support/resistance levels indicates significance
4. **Volume Climax**: Extremely high volume can indicate exhaustion and potential reversal
5. **Low Volume Pullbacks**: Low volume during retracements suggests weak counter-trend movement

## Limitations

- **No Standard Values**: Volume indicators are relative and have no standard values across all assets
- **Delayed Data**: Volume data can sometimes be reported with delays
- **Market Structure Changes**: Changes in market structure or trading patterns can affect interpretation
- **Exchange Differences**: Volume may be reported differently across exchanges

## Best Practices

- Compare current volume to recent average volume for context
- Look for volume confirmation during breakouts and trend changes
- Use volume indicators alongside price-based indicators
- Be wary of price movements on very low volume
- Consider time of day, as volume typically varies throughout trading sessions
- Adjust interpretations based on the specific market (stocks, forex, crypto, etc.)
- Compare volume indicators across multiple timeframes for confirmation 