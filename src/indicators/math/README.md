# Mathematical Utility Functions

This module contains mathematical utility functions used by various technical indicators in the library. These functions provide the underlying calculations needed to implement the more complex indicators.

## Utility Functions

The following mathematical utilities are provided:

### Simple Moving Average (SMA)

Basic arithmetic mean of a series over a specified window.

```rust
let sma_values = simple_moving_average(&series, window);
```

**Used by:**
- SMA indicator
- Bollinger Bands (middle band)
- Many other indicators that require smoothing

### Exponential Moving Average (EMA)

Weighted average that gives more importance to recent data points.

```rust
let ema_values = exponential_moving_average(&series, window);
```

**Used by:**
- EMA indicator
- MACD (both components)
- RSI (optionally for smoothing)

### Weighted Moving Average (WMA)

Weighted average where weights decrease linearly from recent to older data points.

```rust
let wma_values = weighted_moving_average(&series, window);
```

**Used by:**
- WMA indicator
- Custom indicators that need linear weighting

### Standard Deviation

Measures the amount of variation or dispersion in a series.

```rust
let std_dev = standard_deviation(&series, window);
```

**Used by:**
- Bollinger Bands (for band width)
- Historical Volatility indicators

### True Range (TR)

Measures the true price range accounting for gaps between trading sessions.

```rust
let tr = true_range(&high, &low, &close);
```

**Used by:**
- Average True Range (ATR)
- Directional Movement indicators
- Many volatility indicators

### Directional Movement (DM)

Functions that measure the upward and downward price movement.

```rust
let plus_dm = plus_directional_movement(&high, &low);
let minus_dm = minus_directional_movement(&high, &low);
```

**Used by:**
- Directional Movement Index (DMI)
- Average Directional Index (ADX)

### Rate of Change (ROC)

Simple function to calculate percentage change over a period.

```rust
let roc_values = rate_of_change(&series, window);
```

**Used by:**
- ROC indicator
- Momentum indicators
- Custom indicators requiring momentum measurement

## Implementation Notes

- These functions operate primarily on Series from the Polars library
- Many functions include special handling for the initial periods where full window calculations aren't possible
- NaN values are handled appropriately to prevent errors in calculations
- Most functions operate on floating-point data for precision
- Performance optimizations are incorporated where appropriate

## Usage Guidelines

When implementing custom indicators:

1. **Reuse existing functions** where possible rather than reimplementing basic calculations
2. **Consider edge cases** like handling of first n periods, NaN values, etc.
3. **Check for numerical stability** - some calculations can become unstable with extreme values
4. **Maintain consistency** with existing implementation patterns
5. **Document parameter meaning** clearly, especially for less intuitive parameters
6. **Consider optimization** for performance-critical applications

## Mathematical Reference

For detailed mathematical formulas for each function, please refer to the function documentation in the code. The implementations closely follow standard technical analysis formulas with adjustments made for practical computation.

These utility functions are internal to the library and would typically not be called directly by end users, who would instead use the higher-level indicator functions. 