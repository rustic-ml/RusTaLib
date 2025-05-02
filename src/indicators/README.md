# Technical Indicators

This directory contains various technical indicators used in financial market analysis. Technical indicators are mathematical calculations based on historic price, volume, or open interest information that aim to forecast future price movements.

## Categories of Indicators

The indicators are organized into the following categories:

- **Moving Averages**: Trend-following indicators that smooth price data
- **Oscillators**: Indicators that fluctuate within a bounded range
- **Volatility**: Indicators that measure the rate of price movement
- **Volume**: Indicators based on trading volume
- **Trend**: Indicators designed to identify market direction
- **Momentum**: Indicators that measure the rate of price change
- **Cycle**: Indicators that identify cyclical patterns in price
- **Pattern Recognition**: Indicators that identify chart patterns
- **Price Transform**: Indicators that transform price data
- **Stats**: Statistical indicators
- **Math**: Mathematical utility functions

## General Usage

Most indicators follow a similar usage pattern:

```rust
use technical_indicators::indicators::{category_name::indicator_function};

// Calculate the indicator
let result = indicator_function(&dataframe, parameters)?;

// Add to existing DataFrame
dataframe.with_column(result)?;
```

## Choosing Indicators

When selecting indicators for analysis, consider:

1. **Market conditions**: Different indicators work better in trending vs ranging markets
2. **Time frame**: Indicators may provide different signals based on your trading timeframe
3. **Asset class**: Some indicators work better for certain asset classes
4. **Confirmation**: Use multiple indicators to confirm signals
5. **Avoid redundancy**: Using multiple indicators of the same type can lead to false confidence

## Best Practices

- Don't rely on a single indicator for trading decisions
- Understand the calculation and limitations of each indicator
- Backtest your strategy before trading with real money
- Be aware of lagging vs. leading indicators
- Combine indicators with price action analysis and fundamental analysis
- Adjust indicator parameters based on the specific asset and timeframe

Refer to the README in each category folder for specific information about the indicators in that group. 