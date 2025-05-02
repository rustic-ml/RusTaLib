# Moving Averages

Moving averages are among the most widely used technical indicators in stock trading. They smooth out price data to create a single flowing line, making it easier to identify the direction of the trend.

## Types of Moving Averages

This library implements several types of moving averages:

### Simple Moving Average (SMA)

The SMA is calculated by adding the closing prices for a specific number of periods and then dividing by the number of periods.

```rust
let sma = calculate_sma(&dataframe, "close", 20)?;
```

**Parameters:**
- `dataframe`: The price data
- `column_name`: The column to calculate the SMA on (typically "close")
- `window`: Number of periods to average (e.g., 20, 50, 200)

**Interpretation:**
- Price above SMA: Bullish
- Price below SMA: Bearish
- Price crossing above SMA: Buy signal
- Price crossing below SMA: Sell signal
- SMA slope: Indicates trend direction (up=bullish, down=bearish)

**Common parameters:**
- Short-term: 5-20 periods
- Medium-term: 20-60 periods
- Long-term: 100-200 periods

### Exponential Moving Average (EMA)

The EMA gives more weight to recent prices, making it more responsive to new information than the SMA.

```rust
let ema = calculate_ema(&dataframe, "close", 12)?;
```

**Parameters:**
- `dataframe`: The price data
- `column_name`: The column to calculate the EMA on (typically "close")
- `window`: Number of periods to average (e.g., 12, 26)

**Interpretation:**
- Similar to SMA but reacts faster to price changes
- Often used in combination (e.g., 12 and 26-period EMAs for MACD)
- Crossovers between fast and slow EMAs signal trend changes

**Common parameters:**
- Fast EMA: 8-12 periods
- Slow EMA: 21-26 periods

### Weighted Moving Average (WMA)

The WMA assigns weights to each data point, with more weight given to more recent data.

```rust
let wma = calculate_wma(&dataframe, "close", 20)?;
```

**Parameters:**
- `dataframe`: The price data
- `column_name`: The column to calculate the WMA on (typically "close")
- `window`: Number of periods to average

**Interpretation:**
- More responsive than SMA but less than EMA
- Reduces lag but maintains some smoothing
- Interpretation follows the same principles as other moving averages

## Trading Strategies with Moving Averages

### Moving Average Crossovers

A common strategy is to use two moving averages of different lengths and generate signals when they cross each other:

- **Golden Cross**: Short-term MA crosses above long-term MA (bullish)
- **Death Cross**: Short-term MA crosses below long-term MA (bearish)

Example: 50-day MA crossing the 200-day MA is a widely watched signal.

### Moving Average Envelopes

Bands placed above and below a moving average at a fixed percentage distance:

```rust
let upper_band = sma_values * 1.05; // 5% above
let lower_band = sma_values * 0.95; // 5% below
```

**Interpretation:**
- Price near upper band: Potentially overbought
- Price near lower band: Potentially oversold
- Price breaking outside bands: Strong trend

### Multiple Moving Averages

Using three or more moving averages of different lengths to determine overall trend direction:

- All MAs aligned and sloping up: Strong uptrend
- All MAs aligned and sloping down: Strong downtrend
- Mixed alignment: Consolidation or trend change

## Limitations

- **Lagging indicator**: Moving averages follow the price and confirm trends only after they have been established
- **False signals**: In choppy or sideways markets, moving averages can generate many false signals
- **Parameter sensitivity**: Results can vary significantly with different period settings

## Best Practices

- Use longer periods in trending markets and shorter periods in ranging markets
- Adjust MA period lengths based on the volatility of the asset and your trading timeframe
- Combine with other indicators for confirmation (e.g., RSI, volume)
- Consider using multiple types of moving averages for different purposes 