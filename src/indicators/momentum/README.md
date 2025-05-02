# Momentum Indicators

Momentum indicators measure the rate of change in price movements. They help traders identify the speed or strength of a price trend and potential reversals when momentum starts to weaken. These indicators are valuable for timing entries and exits in trending markets.

## Types of Momentum Indicators

This library implements several momentum indicators:

### Rate of Change (ROC)

ROC calculates the percentage change in price over a specified period, providing a direct measure of momentum.

```rust
let roc = calculate_roc(&dataframe, 14, "close")?;
```

**Parameters:**
- `dataframe`: The price data
- `window`: Period for ROC calculation (typically 9-14)
- `column_name`: The column to calculate ROC on (typically "close")

**Interpretation:**
- Positive ROC: Upward momentum
- Negative ROC: Downward momentum
- ROC crossing zero: Potential trend change
- Divergence with price: Potential reversal signal
- Extreme readings: Potential overbought/oversold conditions

**Common parameters:**
- Short-term: 5-10 periods
- Medium-term: 10-20 periods
- Long-term: 20-30 periods

## Trading Strategies with Momentum Indicators

### ROC Trading Strategies

1. **Zero-Line Crossover Strategy**
   - Buy when ROC crosses above zero (upward momentum starting)
   - Sell when ROC crosses below zero (downward momentum starting)

2. **Divergence Strategy**
   - Bullish divergence: Price makes lower lows while ROC makes higher lows
   - Bearish divergence: Price makes higher highs while ROC makes lower highs

3. **Momentum Confirmation Strategy**
   - In an uptrend, buy when ROC pulls back and then starts rising again
   - In a downtrend, sell when ROC rallies and then starts falling again

4. **Overbought/Oversold Strategy**
   - Look for extreme ROC readings relative to historical values
   - Consider contrarian trades when momentum reaches unsustainable levels

## Momentum Trading Principles

1. **Momentum Precedes Price**: Strong momentum often continues in the same direction
2. **Momentum Leads Reversals**: Weakening momentum often precedes price reversals
3. **Momentum Divergence**: Divergence between price and momentum indicates potential trend weakness
4. **Momentum Oscillation**: Momentum naturally oscillates, even in strong trends
5. **Momentum Extremes**: Extreme momentum readings often mean reversion is approaching

## Limitations

- **False signals**: Momentum indicators can give false signals in choppy or sideways markets
- **Lagging components**: Some momentum calculations include moving averages, introducing lag
- **Sensitivity to volatility**: Market volatility can distort momentum readings
- **No absolute levels**: What constitutes "high" or "low" momentum varies between assets

## Best Practices

- Use momentum indicators with trend indicators for confirmation
- Be aware of the general market context when interpreting momentum signals
- Look for momentum divergences at potential market tops and bottoms
- Adjust lookback periods based on the asset's typical volatility
- Compare momentum across multiple timeframes
- Use percentage-based metrics like ROC for comparing momentum across different assets
- Consider combining multiple momentum indicators for more robust signals 