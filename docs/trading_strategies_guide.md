# RusTaLib Technical Indicators: Usage and Trading Strategies

This document provides an overview of the technical indicators available in the RusTaLib library and common trading strategies associated with them.

The library offers a comprehensive suite of indicators, categorized for ease of use:
- Moving Averages
- Oscillators
- Momentum Indicators
- Volatility Indicators
- Volume Indicators
- Trend Indicators
- Cycle Indicators
- Pattern Recognition
- Price Transformation
- Statistical Tools
- Stock-Specific Indicators
- Options-Specific Indicators

For each indicator, we will provide:
- The function name for calculation (as available in the library).
- A brief description of what the indicator measures.
- Common trading strategies or interpretations.

**Disclaimer:** *Technical analysis and trading involve risk. This document is for educational purposes only and should not be considered financial advice. Always do your own research and consider your risk tolerance before making any trading decisions.*

---

## Moving Averages

Moving averages smooth out price data to help identify trend direction. They are lagging indicators, meaning they react to past price movements. Common strategies involving moving averages include trend identification, using them as dynamic support and resistance levels, and trading price crossovers or crossovers between different moving averages (e.g., Golden Cross, Death Cross).

### Simple Moving Average (SMA)
- **Calculation Function**: `calculate_sma`
- **Description**: The SMA calculates the average price of an asset over a specified number of periods. It gives equal weight to each price in the period, providing a smoothed trend line.
- **Trading Strategies**:
    - **Trend Identification**: A rising SMA suggests an uptrend, while a falling SMA indicates a downtrend. The longer the SMA period (e.g., 50-day, 200-day), the more significant the trend it represents.
    - **Support and Resistance**: SMAs can act as dynamic support levels in an uptrend (price bounces off the SMA) or resistance levels in a downtrend (price struggles to break above the SMA). Traders often look for price to respect these levels and may use a bounce off the SMA as an entry point in the direction of the trend, especially when confirmed by other signals (e.g., candlestick patterns).
    - **Price Crossovers**:
        - *Bullish Signal*: Price crosses above the SMA. This can indicate the beginning of an uptrend or a buy signal. Some traders wait for a pullback to the SMA after the initial cross, looking for it to hold as support before entering.
        - *Bearish Signal*: Price crosses below the SMA. This can indicate the beginning of a downtrend or a sell signal. Similarly, some traders wait for a pullback to the SMA after the initial cross, looking for it to hold as resistance before entering.
    - **SMA Crossovers (Multiple SMAs)**:
        - *Golden Cross*: A shorter-term SMA (e.g., 50-day) crosses above a longer-term SMA (e.g., 200-day). This is often interpreted as a strong bullish signal.
        - *Death Cross*: A shorter-term SMA (e.g., 50-day) crosses below a longer-term SMA (e.g., 200-day). This is often interpreted as a strong bearish signal.
- **Considerations**: SMAs are lagging indicators, meaning they react to past price movements and may not predict future movements. They are less responsive to rapid price changes compared to other MAs like EMA. They tend to work best in trending markets and can give false signals in sideways or choppy markets.

### Exponential Moving Average (EMA)
- **Calculation Function**: `calculate_ema`
- **Description**: The EMA is a type of moving average that gives more weight to recent prices, making it more responsive to new information and price changes than an SMA. The weighting applied to the most recent price depends on the selected period of the EMA.
- **Trading Strategies**:
    - **Trend Identification**: Similar to SMA, a rising EMA suggests an uptrend and a falling EMA a downtrend. Due to its responsiveness, it reflects trend changes more quickly than an SMA.
    - **Dynamic Support and Resistance**: EMAs are frequently used as dynamic support and resistance levels. Traders watch for price to respect these levels, often looking for entry opportunities on pullbacks to the EMA that hold support/resistance in the direction of the prevailing trend, especially when confirmed by other signals.
    - **Price Crossovers**:
        - *Bullish Signal*: Price crosses above the EMA. This is often seen as a buy signal, especially in an uptrend or when a new uptrend might be starting. Some traders look for a pullback to the EMA where it acts as support before entering.
        - *Bearish Signal*: Price crosses below the EMA. This is often seen as a sell signal, especially in a downtrend or when a new downtrend might be starting. Similarly, a pullback to the EMA where it acts as resistance can be an entry point.
    - **EMA Crossovers (e.g., 9-EMA & 21-EMA, or 12-EMA & 26-EMA, or 50-EMA & 200-EMA)**:
        - *Bullish Crossover*: A shorter-period EMA crosses above a longer-period EMA. This indicates increasing bullish momentum (e.g., 12-EMA crossing above 26-EMA is a common MACD component).
        - *Bearish Crossover*: A shorter-period EMA crosses below a longer-period EMA. This indicates increasing bearish momentum.
        - **Confirmation with Other Indicators**: EMAs are often used in conjunction with other indicators like RSI or MACD to confirm signals and filter out false positives.
- **Considerations**: While its responsiveness is an advantage for earlier signals, it can also lead to more false signals in volatile or non-trending markets compared to SMAs. The choice of EMA period (e.g., 9, 12, 20, 26, 50, 100, 200) depends heavily on the trader's strategy and the market's characteristics (e.g., shorter periods for day trading, longer for swing or position trading).

### Weighted Moving Average (WMA)
- **Calculation Function**: `calculate_wma`
- **Description**: The WMA assigns a heavier weighting to more recent data points and a lighter weighting to older data points. This makes it more responsive to recent price changes than a Simple Moving Average (SMA) but typically less responsive than an Exponential Moving Average (EMA).
- **Trading Strategies**:
    - **Trend Identification**: A rising WMA suggests an uptrend, while a falling WMA indicates a downtrend. Its greater sensitivity to recent prices (compared to SMA) can provide earlier trend signals.
    - **Dynamic Support and Resistance**: The WMA can act as a dynamic support level in an uptrend or a resistance level in a downtrend. Traders may look for price to pull back to and bounce off the WMA as potential entry points in the direction of the trend.
    - **Price Crossovers**:
        - *Bullish Signal*: Price crosses above the WMA, potentially indicating upward momentum or the start of an uptrend.
        - *Bearish Signal*: Price crosses below the WMA, potentially indicating downward momentum or the start of a downtrend.
    - **WMA Crossovers** (using two WMAs with different periods, e.g., a shorter-term and a longer-term WMA):
        - *Bullish Signal*: The shorter-term WMA crosses above the longer-term WMA. This can indicate a stronger uptrend.
        - *Bearish Signal*: The shorter-term WMA crosses below the longer-term WMA. This can indicate a stronger downtrend.
        - **Confirmation with Other Indicators**: WMAs are often used in conjunction with other technical analysis tools for confirmation.
- **Considerations**: WMA provides a balance between the SMA's smoothness and the EMA's responsiveness. However, like all moving averages, it's a lagging indicator. Its sensitivity to recent prices means it can be prone to whipsaws in choppy or sideways markets, though often less so than an EMA of the same period. The choice of period is crucial and depends on the trading style and asset.

### Volume Weighted Average Price (VWAP)
- **Calculation Function**: `calculate_vwap`
- **Description**: VWAP represents the average price of a security throughout the day, weighted by the volume traded at each price point. It is calculated by summing the dollars traded for every transaction (price multiplied by volume) and then dividing by the total shares traded for the day. VWAP is primarily an intraday indicator as it resets at the start of each trading day.
- **Trading Strategies**:
    - **Intraday Trend and Value Assessment**: 
        - If the price is trading above VWAP, it may be considered bullish for the session, suggesting buyers are in control and traders might look for buying opportunities, especially on pullbacks to the VWAP if it holds as support.
        - If the price is trading below VWAP, it may be considered bearish, suggesting sellers are in control and traders might look for shorting opportunities, especially on rallies to the VWAP if it holds as resistance.
    - **Entry/Exit Points (Mean Reversion)**: Some traders look for prices to revert to VWAP. They might consider buying when the price dips significantly below VWAP (potentially near a lower VWAP band) and then starts to move back towards it, or selling when the price rallies far above VWAP (potentially near an upper VWAP band) and shows signs of returning.
    - **Support and Resistance**: For intraday trading, VWAP can act as a dynamic level of support or resistance. Traders watch how price reacts around the VWAP line. A break and hold above VWAP can be bullish, while a break and hold below can be bearish. Rejections from VWAP can also signal entries in the opposite direction.
    - **Institutional Benchmark**: Large institutions often use VWAP to execute large orders to minimize market impact. They might aim to buy below VWAP or sell above VWAP. Retail traders can use this knowledge to anticipate potential areas of institutional interest and subsequent price reactions around the VWAP.
    - **Confirmation of Breakouts**: A breakout above or below a key horizontal support/resistance level, when accompanied by the price also crossing VWAP in the same direction, can add confirmation to the breakout's validity and strength.
- **Considerations**: 
    - VWAP is a lagging indicator as it's based on past price and volume data.
    - Its usefulness is primarily for intraday timeframes; it's not typically used for longer-term analysis because it resets daily.
    - In low-liquidity stocks, VWAP might be less reliable due to erratic volume.
    - Some traders use VWAP bands (similar to Bollinger Bands, plotted at standard deviations above and below the VWAP line) to identify overbought or oversold conditions relative to the VWAP.

## Momentum Indicators

Momentum indicators help traders assess the speed or rate of price changes in an asset. They are often used to identify overbought or oversold conditions, signal potential trend reversals or continuations, and gauge the strength of price movements. Common strategies involve looking for crossovers, divergences, and extreme readings.

### Relative Strength Index (RSI)
- **Calculation Function**: `calculate_rsi`
- **Description**: The Relative Strength Index (RSI) is a momentum oscillator, developed by J. Welles Wilder, that measures the speed and change of price movements. RSI oscillates between zero and 100. Traditionally, RSI is considered overbought when above 70 and oversold when below 30. It can also be used to identify the general trend.
- **Trading Strategies**:
    - **Overbought/Oversold Levels**:
        - *Sell Signal*: RSI moves above 70 (overbought) and then crosses back below 70. This suggests that buying momentum may be exhausted.
        - *Buy Signal*: RSI moves below 30 (oversold) and then crosses back above 30. This suggests that selling momentum may be exhausted.
        - These levels can be adjusted (e.g., 80/20) based on asset volatility or market conditions.
    - **Divergence**:
        - *Bullish Divergence*: Price makes a new low, but RSI makes a higher low. This indicates weakening selling momentum and can signal a potential price upturn.
        - *Bearish Divergence*: Price makes a new high, but RSI makes a lower high. This indicates weakening buying momentum and can signal a potential price downturn.
    - **Centerline (50 Level) Crossovers**:
        - *Bullish Signal*: RSI crosses above the 50 level, suggesting that bullish momentum is gaining control. This can be an entry signal or confirm an uptrend.
        - *Bearish Signal*: RSI crosses below the 50 level, suggesting that bearish momentum is gaining control. This can be an entry signal or confirm a downtrend.
    - **Failure Swings (More Advanced)**:
        - *Bullish Failure Swing (Top Failure Swing during oversold)*: RSI dips below 30 (oversold), bounces, pulls back but stays above the previous low (fails to make a new low in oversold territory), and then breaks its previous reaction high. This is a confirmation of a bottom.
        - *Bearish Failure Swing (Bottom Failure Swing during overbought)*: RSI rises above 70 (overbought), declines, rallies but stays below the previous high (fails to make a new high in overbought territory), and then breaks its previous reaction low. This is a confirmation of a top.
    - **Trendline Analysis on RSI**: Trendlines can be drawn on the RSI chart itself. A break of an RSI trendline can often precede a break of a trendline on the price chart, providing an early signal.
- **Considerations**:
    - **Period**: The most common period for RSI is 14. Shorter periods make it more sensitive, while longer periods make it smoother.
    - **Strong Trends**: In a strong uptrend, RSI can remain in the overbought territory for extended periods, and vice-versa in strong downtrends. Therefore, overbought/oversold signals alone are not always reliable for reversals in strong trends; they might just indicate strength.
    - **Confirmation**: It's generally recommended to use RSI signals in conjunction with other indicators (e.g., price action, trendlines, moving averages) for confirmation.

### Moving Average Convergence Divergence (MACD)
- **Calculation Function**: `calculate_macd`
- **Description**: The MACD is a trend-following momentum indicator that shows the relationship between two exponential moving averages (EMAs) of a security's price. It consists of the MACD line (typically the 12-period EMA minus the 26-period EMA), a signal line (typically a 9-period EMA of the MACD line), and a histogram (which represents the difference between the MACD line and the signal line).
- **Trading Strategies**:
    - **Signal Line Crossovers**:
        - *Bullish Crossover*: The MACD line crosses above the signal line. This is often interpreted as a buy signal, especially if it occurs when both lines are below the zero line.
        - *Bearish Crossover*: The MACD line crosses below the signal line. This is often interpreted as a sell signal, especially if it occurs when both lines are above the zero line.
    - **Zero Line (Centerline) Crossovers**:
        - *Bullish Crossover*: The MACD line crosses above the zero line, moving from negative to positive territory. This indicates increasing bullish momentum.
        - *Bearish Crossover*: The MACD line crosses below the zero line, moving from positive to negative territory. This indicates increasing bearish momentum.
    - **Divergence**:
        - *Bullish Divergence*: Price makes new lows, but the MACD line (or histogram) makes higher lows. This suggests that bearish momentum is weakening and a potential upturn in price may occur.
        - *Bearish Divergence*: Price makes new highs, but the MACD line (or histogram) makes lower highs. This suggests that bullish momentum is weakening and a potential downturn in price may occur.
    - **Histogram Analysis**:
        - *Increasing Bullish Momentum*: The histogram bars are positive and increasing in height.
        - *Decreasing Bullish Momentum*: The histogram bars are positive but decreasing in height (can be an early warning of a potential bearish crossover).
        - *Increasing Bearish Momentum*: The histogram bars are negative and increasing in depth (length below zero).
        - *Decreasing Bearish Momentum*: The histogram bars are negative but decreasing in depth (can be an early warning of a potential bullish crossover).
        - Divergences between the histogram and price can also be used.
    - **Rapid Rises or Falls**: A sharp increase or decrease in the MACD line, pulling away from the signal line, can indicate that the security is overbought or oversold in the short term and might be due for a correction.
- **Considerations**:
    - Standard MACD parameters are 12, 26, and 9, but traders may adjust these to suit different assets or timeframes.
    - MACD is a lagging indicator, so it confirms trends rather than predicts them. This can lead to late entries or exits.
    - It can produce false signals in sideways or choppy markets. Combining MACD with other indicators (like RSI, moving averages, or trend-identifying tools like ADX) and price action analysis is often recommended.
    - The interpretation of MACD can be subjective, especially regarding divergences.

### Rate of Change (ROC)
- **Calculation Function**: `calculate_roc`
- **Description**: The ROC is a pure momentum oscillator that measures the percentage change in price between the current price and the price a certain number of periods ago. It oscillates above and below a zero line. Positive values indicate upward momentum, while negative values indicate downward momentum.
- **Trading Strategies**:
    - **Zero Line Crossovers**:
        - *Bullish Signal*: ROC crosses above the zero line. This suggests that upward momentum is increasing and can be a buy signal, especially if confirming an existing uptrend.
        - *Bearish Signal*: ROC crosses below the zero line. This suggests that downward momentum is increasing and can be a sell signal, especially if confirming an existing downtrend.
    - **Overbought/Oversold Levels**:
        - Unlike RSI, ROC does not have standardized overbought/oversold levels (e.g., 70/30). These levels are subjective and vary depending on the asset's volatility and the chosen ROC period. Traders identify historical extreme highs and lows in the ROC to define potential overbought (sell) and oversold (buy) zones for a specific asset.
        - *Sell Signal*: ROC reaches a historically significant high level (overbought) and starts to turn down.
        - *Buy Signal*: ROC reaches a historically significant low level (oversold) and starts to turn up.
    - **Divergence**:
        - *Bullish Divergence*: Price makes new lows, but ROC makes higher lows. This can signal weakening bearish momentum and a potential price upturn.
        - *Bearish Divergence*: Price makes new highs, but ROC makes lower highs. This can signal weakening bullish momentum and a potential price downturn.
    - **Trend Confirmation**: 
        - In a strong uptrend, ROC will generally remain positive.
        - In a strong downtrend, ROC will generally remain negative.
        - A move towards and across the zero line can signal a potential trend change.
- **Considerations**:
    - The choice of the lookback period (n-periods) is crucial. Shorter periods (e.g., 9-14) make the ROC more sensitive to short-term price changes, leading to more signals (and potentially more false signals). Longer periods (e.g., 20-50) make it smoother and better for identifying longer-term momentum, but signals will be less frequent and more lagging.
    - ROC can be very choppy, especially with shorter lookback periods. It's often used in conjunction with other indicators (e.g., moving averages to define the trend) or chart patterns for confirmation.
    - Extreme ROC readings do not always mean an immediate reversal, especially in strongly trending markets.
    - Because ROC is not normalized (like RSI), comparing its absolute values across different securities or significantly different time periods can be misleading.

### Stochastic Oscillator
- **Calculation Function**: `calculate_stochastic_oscillator`
- **Description**: The Stochastic Oscillator is a momentum indicator that compares a particular closing price of a security to a range of its prices over a certain period of time. It is range-bound, oscillating between 0 and 100. The indicator consists of two lines: the %K line (the main stochastic line) and the %D line (a moving average of %K). It's designed to show when an asset is overbought or oversold.
- **Trading Strategies**:
    - **Overbought/Oversold Conditions**:
        - *Sell Signal*: The oscillator moves above 80 (overbought territory). A sell is often considered when %K crosses below %D while above 80, or when both lines cross back below 80.
        - *Buy Signal*: The oscillator moves below 20 (oversold territory). A buy is often considered when %K crosses above %D while below 20, or when both lines cross back above 20.
    - **%K and %D Line Crossovers**:
        - *Bullish Crossover*: The %K line crosses above the %D line, especially when occurring in the oversold region (below 20). This can be an early signal of upward momentum.
        - *Bearish Crossover*: The %K line crosses below the %D line, especially when occurring in the overbought region (above 80). This can be an early signal of downward momentum.
        - Crossovers near the 50 level can also be significant, indicating a potential shift in momentum.
    - **Divergence**:
        - *Bullish Divergence*: Price makes a new low, but the Stochastic Oscillator makes a higher low. This suggests that the downward momentum is weakening and a price rise might be imminent.
        - *Bearish Divergence*: Price makes a new high, but the Stochastic Oscillator makes a lower high. This suggests that the upward momentum is weakening and a price fall might be imminent.
    - **Trend Confirmation (using the 50 level)**:
        - In an uptrend, the Stochastic Oscillator tends to stay above the 50 level, with dips into oversold territory offering buying opportunities.
        - In a downtrend, the Stochastic Oscillator tends to stay below the 50 level, with rallies into overbought territory offering selling opportunities.
- **Considerations**:
    - **Settings**: Common settings are (14, 3, 3) - 14 periods for %K, 3-period SMA for %D, and a 3-period slowing for %K. "Fast" stochastics use raw %K, while "slow" stochastics (more common) apply a smoothing to %K before calculating %D. Adjusting these periods can make the indicator more or less sensitive.
    - **Whipsaws**: The Stochastic Oscillator can generate false signals (whipsaws), especially in choppy or non-trending markets. It's generally more reliable in ranging markets or when used to identify pullbacks within a strong trend.
    - **Prolonged Extremes**: In strong trending markets, the oscillator can remain in overbought or oversold territory for extended periods, making these signals less reliable for immediate reversals. In such cases, it might indicate trend strength rather than an imminent reversal.
    - **Confirmation**: It's highly recommended to use the Stochastic Oscillator in conjunction with other indicators (like trend lines, moving averages, or RSI) and price action analysis for confirmation of signals.

### Triple Exponential Average (TRIX)
- **Calculation Function**: `calculate_trix`
- **Description**: TRIX is a momentum oscillator that displays the percentage rate of change of a triple exponentially smoothed moving average of the price. Its primary advantage is its ability to filter out insignificant price movements (market noise), making it effective for identifying trends. It oscillates around a zero line.
- **Trading Strategies**:
    - **Zero Line Crossovers**:
        - *Bullish Signal*: TRIX crosses above the zero line. This suggests that momentum is shifting upwards and can be a buy signal, indicating a potential new uptrend.
        - *Bearish Signal*: TRIX crosses below the zero line. This suggests that momentum is shifting downwards and can be a sell signal, indicating a potential new downtrend.
    - **Signal Line Crossovers** (A signal line, typically a 9-period EMA of the TRIX itself, is often used):
        - *Bullish Signal*: TRIX line crosses above its signal line, especially when both are below the zero line or when TRIX is already rising.
        - *Bearish Signal*: TRIX line crosses below its signal line, especially when both are above the zero line or when TRIX is already falling.
    - **Divergence**:
        - *Bullish Divergence*: Price makes new lows, but TRIX makes higher lows. This indicates that the selling momentum is weakening and a price bottom might be forming.
        - *Bearish Divergence*: Price makes new highs, but TRIX makes lower highs. This indicates that the buying momentum is weakening and a price top might be forming.
    - **Trend Confirmation**:
        - A TRIX value consistently above zero can confirm an uptrend.
        - A TRIX value consistently below zero can confirm a downtrend.
        - The slope of the TRIX line can also indicate the strength of the momentum.
- **Considerations**:
    - **Lag**: Due to the triple smoothing, TRIX can be a lagging indicator. While this helps filter noise, it might also mean signals appear after a significant portion of the price move has already occurred.
    - **Period Selection**: The period used for TRIX calculation (commonly 14 or 15 periods) significantly impacts its sensitivity. Shorter periods make it more responsive but prone to more false signals. Longer periods make it smoother but increase lag.
    - **Whipsaws**: Like many oscillators, TRIX can produce false signals in sideways or choppy markets. It tends to perform better in trending markets.
    - **Confirmation**: It's advisable to use TRIX in conjunction with other indicators, such as trend-following tools (e.g., moving averages) or price action analysis, to confirm signals and improve trading decisions.
    - **Not for Overbought/Oversold**: While TRIX is an oscillator, it's not typically used to identify overbought or oversold levels in the same way as RSI or Stochastics because its values are not strictly bounded in a way that defines clear extreme zones for all assets.

### Ultimate Oscillator
- **Calculation Function**: `calculate_ultimate_oscillator`
- **Description**: The Ultimate Oscillator, developed by Larry Williams, is a momentum oscillator designed to capture momentum across three different timeframes (typically short, medium, and long term - e.g., 7, 14, and 28 periods). It calculates a weighted average of these three timeframes to reduce the false signals that can occur with single-timeframe oscillators, especially during strong trends. It ranges from 0 to 100.
- **Trading Strategies**:
    - **Divergence (Primary Signal)**:
        - *Bullish Divergence*: This is the main buy signal. It occurs when:
            1. Price makes a lower low, but the Ultimate Oscillator makes a higher low (indicating weakening selling momentum).
            2. The first low of the divergence in the oscillator occurred below a certain threshold (e.g., 30 or 35), suggesting an oversold condition.
            3. The oscillator then rises above the high point it reached *between* the two lows of the divergence.
        - *Bearish Divergence*: This is the main sell signal. It occurs when:
            1. Price makes a higher high, but the Ultimate Oscillator makes a lower high (indicating weakening buying momentum).
            2. The first high of the divergence in the oscillator occurred above a certain threshold (e.g., 70 or 65), suggesting an overbought condition.
            3. The oscillator then falls below the low point it reached *between* the two highs of the divergence.
    - **Overbought/Oversold Levels**:
        - While divergence is key, levels like 70 (overbought) and 30 (oversold) are often used as prerequisites for valid divergence signals. 
        - Some traders might look for exits when the oscillator reaches extreme levels (e.g., sell if long and oscillator hits 70; buy to cover if short and oscillator hits 30), though Williams emphasized divergence for entries.
    - **Centerline Crossover (50 Level)**:
        - A move above 50 can be seen as bullish momentum building.
        - A move below 50 can be seen as bearish momentum building.
        - This can be used as a filter or for confirmation alongside divergence signals.
- **Considerations**:
    - **Timeframes**: Default periods are typically 7, 14, and 28. These can be adjusted based on the asset's volatility and the trading timeframe. Shorter periods increase sensitivity, while longer periods smooth it out.
    - **False Signals**: Like all oscillators, the Ultimate Oscillator can produce false signals, especially in non-trending or very choppy markets. The multi-timeframe nature aims to reduce these compared to single-timeframe oscillators.
    - **Confirmation**: It is strongly recommended to use the Ultimate Oscillator in conjunction with other technical analysis tools, such as trend analysis (trendlines, moving averages), chart patterns, or volume analysis, to confirm signals.
    - **Complexity**: The calculation is more complex than some other oscillators, but most charting platforms handle this automatically.
    - **Signal Interpretation**: The divergence rules, especially the confirmation by breaking the divergence's internal peak/trough, are crucial for Williams' intended use.

## Volatility Indicators

Volatility indicators measure the rate and magnitude of price fluctuations in an asset. High volatility often signifies a market with large price swings and potential for significant gains or losses, while low volatility indicates smaller price changes and a more stable market. These indicators help traders assess risk, identify potential breakouts, and adapt their strategies to changing market conditions.

### Bollinger Bands®
- **Calculation Function**: `calculate_bollinger_bands`
- **Description**: Developed by John Bollinger, Bollinger Bands® are a volatility indicator consisting of a middle band (typically a 20-period Simple Moving Average) and two outer bands (typically set at 2 standard deviations above and below the middle band). The bands widen when volatility increases and narrow when volatility decreases.
- **Trading Strategies**:
    - **Volatility Assessment (The Squeeze)**:
        - *Widening Bands*: Indicate increased market volatility.
        - *Narrowing Bands (Squeeze)*: Indicate decreased volatility. A "squeeze" often precedes a significant price breakout. Traders look for the bands to narrow significantly, then position for a breakout when the price moves decisively outside one of the bands, ideally with increased volume.
    - **Overbought/Oversold (Mean Reversion)**:
        - *Price at Upper Band*: May suggest an overbought condition, with potential for price to revert towards the middle band (SMA). This is often considered a sell signal, especially if confirmed by bearish price action or other indicators.
        - *Price at Lower Band*: May suggest an oversold condition, with potential for price to revert towards the middle band. This is often considered a buy signal, especially if confirmed by bullish price action.
        - This strategy works best in ranging or sideways markets. In strong trends, price can "walk the bands" (stay near an outer band for extended periods).
    - **Trend Following / Breakouts**:
        - *Walking the Bands*: In a strong uptrend, prices may consistently touch or run along the upper band. In a strong downtrend, prices may consistently touch or run along thelower band. This indicates trend strength, not necessarily an immediate reversal.
        - *Breakout from Squeeze*: As mentioned above, a price break outside the bands after a squeeze can signal the start of a new trend.
    - **"W" Bottoms and "M" Tops (Advanced Patterns)**:
        - *"W" Bottom (Bullish)*: Price forms a low, rallies slightly, retests the low (often near or slightly below the first low but above the lower band), and then rallies again, often breaking above the middle band. The second low holding above the lower band while the first low touched or breached it can be a sign of waning selling pressure.
        - *"M" Top (Bearish)*: Price forms a high, declines, retests the high (often near or slightly below the first high but below the upper band), and then declines again, often breaking below the middle band. The second high failing to decisively break the upper band can signal waning buying pressure.
- **Considerations**:
    - **Standard Deviation and Period**: The most common settings are a 20-period SMA for the middle band and 2 standard deviations for the outer bands. These can be adjusted (e.g., 2.5 or 3 standard deviations for wider bands, or different SMA periods).
    - **Not Standalone Signals**: Touches of the bands are not by themselves buy/sell signals. Price action, confirmation from other indicators (e.g., RSI for divergence, volume), and market context are crucial.
    - **Volatility Indication**: Bollinger Bands primarily indicate volatility. They do not inherently predict price direction.
    - **Non-Normal Distribution**: Stock prices don't always follow a normal distribution, so the statistical significance of the standard deviation bands can vary.

### Average True Range (ATR)
- **Calculation Function**: `calculate_atr`
- **Description**: Developed by J. Welles Wilder Jr., the Average True Range (ATR) is an indicator that measures market volatility. It calculates the average of "true ranges" over a specified period. The true range for each period is the greatest of: the current period's high minus low; the absolute value of the current high minus the previous close; or the absolute value of the current low minus the previous close. ATR does not indicate price direction, only the degree of price volatility.
- **Trading Strategies**:
    - **Volatility Assessment**: 
        - A rising ATR indicates increasing price volatility.
        - A falling ATR indicates decreasing price volatility.
        - Periods of low ATR (low volatility) can sometimes precede significant price breakouts (high volatility).
    - **Stop-Loss Placement (Primary Use)**:
        - *Fixed Multiple Trailing Stop*: Set a stop-loss a certain multiple of ATR away from the entry price or the current price. For a long position, this might be `Entry Price - (N * ATR)` or `Current Price - (N * ATR)`. For a short position, `Entry Price + (N * ATR)` or `Current Price + (N * ATR)`. Common multiples (N) are 1.5, 2, or 3.
        - *Chandelier Exit*: A popular trailing stop method where the stop-loss is placed a multiple of ATR below the highest high achieved since entering a long trade, or a multiple of ATR above the lowest low achieved since entering a short trade. This allows the stop to trail the price as it moves favorably while adjusting for volatility.
    - **Position Sizing**: Adjust trade size based on the asset's ATR. For more volatile assets (higher ATR), a smaller position size can be used to maintain consistent risk per trade, and vice-versa for less volatile assets.
        - Example: `Position Size = Account Risk / (ATR * Multiplier * Price per Point)`
    - **Breakout Confirmation / Entry Filter**: Although ATR is non-directional, a sudden increase in ATR can confirm the strength of a breakout. Some strategies might require ATR to be above a certain level or to have increased by a certain percentage to validate a breakout signal.
    - **Identifying Potential for Range Expansion/Contraction**: Observing ATR levels can help anticipate shifts in market behavior. Sustained low ATR might suggest an impending breakout, while extremely high ATR might indicate unsustainable volatility that could lead to consolidation.
- **Considerations**:
    - **Period Selection**: The most common period for ATR is 14, but this can be adjusted. Shorter periods make ATR more responsive to recent volatility changes, while longer periods provide a smoother, longer-term average.
    - **Not Directional**: ATR only measures volatility, not trend direction or momentum. A high ATR can occur in a strong uptrend, a strong downtrend, or even a choppy sideways market with large swings.
    - **Subjective Interpretation**: What constitutes "high" or "low" ATR is relative to the specific asset and its historical ATR values.
    - **Lagging Indicator**: ATR is based on past price data, so it will lag in reflecting instantaneous changes in volatility.
    - **Use in Conjunction**: It's best used with other indicators and analysis techniques to provide context for trading decisions (e.g., trend indicators to determine direction, price patterns for entry signals).

### Ease of Movement (EOM / EMV)
- **Calculation Function**: `calculate_ease_of_movement`
- **Description**: Developed by Richard W. Arms, Jr., the Ease of Movement (EOM or EMV) indicator is an oscillator that relates an asset's price change to its volume. It's designed to show how easily prices are moving. High EOM values suggest that prices are rising with relative ease (i.e., on light volume), while low EOM values suggest prices are falling with relative ease (again, on light volume). If it takes a lot of volume to move prices, the EOM value will be close to zero. The indicator typically uses a 14-period Simple Moving Average of the 1-period EOM values.
- **Trading Strategies**:
    - **Trend Identification & Confirmation**:
        - *Positive EOM*: When the EOM is above the zero line and rising, it suggests that prices are advancing with ease. This can confirm an uptrend.
        - *Negative EOM*: When the EOM is below the zero line and falling, it suggests that prices are declining with ease. This can confirm a downtrend.
        - *EOM near Zero*: If the EOM is hovering around the zero line, it indicates that it's taking significant volume to move prices, which might suggest a consolidating market or a lack of clear directional pressure.
    - **Zero Line Crossovers**:
        - *Bullish Crossover*: EOM crosses above the zero line, indicating it's becoming easier for prices to move up. This can be an early signal of a potential uptrend or strengthening bullish momentum.
        - *Bearish Crossover*: EOM crosses below the zero line, indicating it's becoming easier for prices to move down. This can be an early signal of a potential downtrend or strengthening bearish momentum.
    - **Divergence**:
        - *Bullish Divergence*: Price makes new lows, but EOM makes higher lows or fails to make a new low. This can suggest that the downward price movement is occurring with less ease (i.e., it's taking more effort/volume to push prices down), potentially signaling a weakening downtrend.
        - *Bearish Divergence*: Price makes new highs, but EOM makes lower highs or fails to make a new high. This can suggest that the upward price movement is occurring with less ease, potentially signaling a weakening uptrend.
    - **Confirmation with Moving Averages**: Some traders apply a moving average to the EOM line itself. Crossovers of the EOM line and its moving average can be used as buy or sell signals, similar to MACD signal line crossovers.
- **Considerations**:
    - **Volume is Key**: The EOM heavily relies on volume data. Its interpretation can be skewed in markets with very low or erratic volume.
    - **Period Setting**: The default period for the EOM's moving average is often 14. Shorter periods make it more sensitive, while longer periods make it smoother.
    - **Not a Standalone Indicator**: EOM is generally not recommended as a standalone indicator. It's best used to confirm signals from other indicators (like price action analysis, trendlines, or other oscillators) because its primary strength is in assessing how much volume is driving price changes.
    - **Interpretation Context**: 
        - High positive EOM: Prices rising on low volume (ease of upward movement).
        - High negative EOM: Prices falling on low volume (ease of downward movement).
        - EOM near zero: Prices moving with difficulty, requiring high volume for small changes, or a very quiet market.
    - **Use in Conjunction**: It's best used with other indicators and analysis techniques to provide context for trading decisions (e.g., trend indicators to determine direction, price patterns for entry signals).

### Volume Price Trend (VPT) / Price Volume Trend (PVT)
- **Calculation Function**: `calculate_volume_price_trend`
- **Description**: The VPT is a momentum-based indicator that measures the relationship between an asset's price change and its trading volume. It's a cumulative indicator where a portion of the volume (based on the percentage price change) is added to or subtracted from the previous VPT value. If the price closes up, the volume multiplied by the percentage price increase is added. If the price closes down, the volume multiplied by the percentage price decrease is subtracted. This makes VPT sensitive to both price changes and the volume behind them.
- **Trading Strategies**:
    - **Trend Confirmation**:
        - *Rising VPT & Rising Price*: Confirms a healthy uptrend, as increasing prices are supported by volume.
        - *Falling VPT & Falling Price*: Confirms a healthy downtrend, as decreasing prices are supported by volume.
        - The slope and direction of the VPT line should generally follow the price trend.
    - **Divergence**:
        - *Bullish Divergence*: Price makes a new low, but VPT fails to make a new low or makes a higher low. This suggests that selling pressure is diminishing, and the downtrend might be losing momentum, potentially leading to a reversal.
        - *Bearish Divergence*: Price makes a new high, but VPT fails to make a new high or makes a lower high. This suggests that buying pressure is weakening, and the uptrend might be losing momentum, potentially leading to a reversal.
    - **Signal Line Crossovers**: Some traders apply a moving average (e.g., 9-period SMA or EMA) to the VPT line and use crossovers for signals:
        - *Bullish Signal*: VPT crosses above its moving average.
        - *Bearish Signal*: VPT crosses below its moving average.
    - **Relationship with On-Balance Volume (OBV)**: VPT is similar to OBV, but while OBV adds or subtracts the total volume based purely on whether the price closed up or down, VPT incorporates the *magnitude* (percentage) of the price change. This can make VPT more sensitive to the strength of price moves relative to volume.
- **Considerations**:
    - **Cumulative Nature**: Like OBV, VPT is a cumulative total. The absolute value of the VPT is not as important as its direction and its relationship with price.
    - **Lagging Indicator**: As it uses past price and volume data, it can lag behind price movements.
    - **Interpretation of Divergence**: Divergences can persist for a long time and don't always result in an immediate reversal. They indicate a potential change in momentum, not a guaranteed reversal.
    - **Confirmation**: It's generally recommended to use VPT signals in conjunction with other technical analysis tools (e.g., chart patterns, other oscillators, or price action) to confirm trading decisions.
    - **Choppy Markets**: In sideways or choppy markets, VPT may produce frequent and unreliable signals. It tends to perform better in trending markets.

### On-Balance Volume (OBV)
- **Calculation Function**: `calculate_obv`
- **Description**: Developed by Joseph Granville, On-Balance Volume (OBV) is a momentum indicator that uses volume flow to predict changes in stock price. It is a cumulative total of volume. If the current closing price is higher than the previous close, the current day's volume is added to the previous OBV. If the current closing price is lower than the previous close, the current day's volume is subtracted. If the closing prices are the same, the OBV remains unchanged. The absolute value of OBV is generally not important; its direction and relationship with price are key.
- **Trading Strategies**:
    - **Trend Confirmation**:
        - *Uptrend Confirmation*: If price is making higher highs and higher lows, OBV should also be making higher highs and higher lows. This indicates that volume is supporting the uptrend.
        - *Downtrend Confirmation*: If price is making lower highs and lower lows, OBV should also be making lower highs and lower lows. This indicates that volume is supporting the downtrend.
    - **Divergence (Primary Signal)**:
        - *Bullish Divergence*: Occurs when price makes a new low (or a series of lower lows) but OBV fails to make a new low, instead forming a higher low. This suggests that selling pressure is diminishing and accumulation may be occurring, potentially leading to a price reversal upwards.
        - *Bearish Divergence*: Occurs when price makes a new high (or a series of higher highs) but OBV fails to make a new high, instead forming a lower high. This suggests that buying pressure is weakening and distribution may be occurring, potentially leading to a price reversal downwards.
    - **Breakout Confirmation**:
        - When price breaks out above a resistance level or below a support level, a corresponding breakout in the OBV (moving to new highs or lows along with the price) can confirm the strength and validity of the price breakout.
        - Sometimes OBV may break its own trendline or a key OBV level before the price breaks out, acting as a leading indicator for a potential price move.
    - **Identifying Accumulation/Distribution**: Sharp increases in OBV without a correspondingly large price increase can suggest accumulation (smart money buying). Conversely, sharp decreases in OBV without a correspondingly large price decrease can suggest distribution (smart money selling).
- **Considerations**:
    - **Leading vs. Lagging**: While divergences can offer leading signals, simple trend confirmation is more of a lagging characteristic.
    - **Sensitivity to Large Volume Spikes**: A single day with exceptionally high volume (e.g., due to news, an earnings report, or an index rebalancing) can significantly impact the OBV and potentially distort its interpretation for a period afterward. Some smoothing or context is needed.
    - **Not for Overbought/Oversold**: OBV is not typically used to identify overbought or oversold levels directly as it is not range-bound.
    - **Use with Other Indicators**: It's most effective when used in conjunction with other technical analysis tools such as price patterns, moving averages, and other oscillators for confirmation of signals.
    - **Starting Point Sensitivity**: Since OBV is cumulative, its absolute value depends on the starting point of the calculation. However, this is generally not an issue as analysis focuses on the OBV line's direction, slope, and patterns relative to price.

## Trend Indicators

Trend indicators are designed to identify the direction and strength of a market trend. They help traders determine whether a market is in an uptrend (prices are generally rising), a downtrend (prices are generally falling), or a sideways trend (prices are consolidating). Understanding the prevailing trend is crucial for making informed trading decisions, as trading with the trend is often a core principle of many strategies. These indicators can also help in identifying potential trend reversals.

### Average Directional Index (ADX), +DI, and -DI
- **Calculation Functions**:
    - ADX: `calculate_adx`
    - Plus Directional Indicator (+DI): `calculate_plus_di`
    - Minus Directional Indicator (-DI): `calculate_minus_di`
    - Smoothed ADX (ADXR): `calculate_adxr`
    - Underlying Plus Directional Movement (+DM): `calculate_plus_dm`
    - Underlying Minus Directional Movement (-DM): `calculate_minus_dm`
- **Description**: Developed by J. Welles Wilder, the ADX system is designed to measure both the strength and direction of a trend. It consists of three lines:
    - **ADX Line**: Measures the strength of the trend, regardless of whether it's an uptrend or downtrend. It ranges from 0 to 100. A rising ADX indicates a strengthening trend, while a falling ADX suggests a weakening trend or a non-trending market.
    - **Plus Directional Indicator (+DI Line)**: Measures the strength of the upward price movement.
    - **Minus Directional Indicator (-DI Line)**: Measures the strength of the downward price movement.
    - +DI and -DI lines are often plotted alongside the ADX line.
- **Trading Strategies & Interpretation**:
    - **Trend Strength**:
        - ADX below 20-25: Weak trend or non-trending market. Trading trend-following strategies can be risky.
        - ADX rising above 20-25: Trend is strengthening.
        - ADX above 40-50: Very strong trend.
        - ADX falling from high levels: Trend may be losing momentum or ending.
        - *Note*: ADX itself is non-directional; it only indicates trend strength.
    - **Trend Direction (using +DI and -DI Crossovers)**:
        - *Bullish Signal*: +DI crosses above -DI. This suggests the start of an uptrend.
        - *Bearish Signal*: -DI crosses above +DI. This suggests the start of a downtrend.
        - These crossovers are often used in conjunction with ADX rising above 20-25 to confirm trend strength.
    - **Entry/Exit Signals**:
        - *Buy*: +DI crosses above -DI, and ADX is above 20-25 (or rising).
        - *Sell/Short*: -DI crosses above +DI, and ADX is above 20-25 (or rising).
        - Exits can be triggered when ADX starts to fall significantly, or when +DI/-DI lines cross back.
    - **ADXR (Average Directional Index Rating)**: `calculate_adxr` provides a smoothed version of ADX (ADX + ADX n-periods ago / 2). It can be used similarly to ADX but gives smoother indications.
- **Considerations**:
    - ADX is a lagging indicator for trend strength.
    - +DI/-DI crossovers can produce whipsaws in non-trending markets. Using ADX to filter these signals (i.e., only taking signals when ADX is above a certain threshold) is crucial.
    - Standard period for ADX, +DI, -DI calculations is typically 14 periods, but this can be adjusted.
    - The underlying Directional Movement calculations (`calculate_plus_dm`, `calculate_minus_dm`) form the basis for +DI and -DI.

### Aroon Indicator & Aroon Oscillator
- **Calculation Functions**:
    - Aroon Indicator (Up & Down lines): `calculate_aroon`
    - Aroon Oscillator: `calculate_aroon_osc`
- **Description**: Developed by Tushar Chande, the Aroon indicator is used to identify trend changes and gauge trend strength. It consists of two lines:
    - **Aroon Up Line**: Measures the strength of the uptrend by tracking the number of periods since the last highest high. A higher Aroon Up value (closer to 100) indicates a stronger uptrend and more recent highs.
    - **Aroon Down Line**: Measures the strength of the downtrend by tracking the number of periods since the last lowest low. A higher Aroon Down value (closer to 100) indicates a stronger downtrend and more recent lows.
    - Both lines oscillate between 0 and 100.
    - The **Aroon Oscillator** is a single line derived by subtracting the Aroon Down from the Aroon Up. It oscillates between -100 and +100.
- **Trading Strategies & Interpretation**:
    - **Trend Identification (Aroon Lines)**:
        - *Uptrend*: Aroon Up is above Aroon Down, and Aroon Up is often above 70 while Aroon Down is below 30.
        - *Downtrend*: Aroon Down is above Aroon Up, and Aroon Down is often above 70 while Aroon Up is below 30.
        - *Consolidation/Weak Trend*: Aroon Up and Aroon Down lines are moving in parallel or are both low (e.g., below 50).
    - **Crossovers (Aroon Lines)**:
        - *Bullish Signal*: Aroon Up crosses above Aroon Down. Suggests the potential start of an uptrend.
        - *Bearish Signal*: Aroon Down crosses above Aroon Up. Suggests the potential start of a downtrend.
    - **Extreme Levels (Aroon Lines)**:
        - Aroon Up reaching 100: Indicates a new high was made very recently, strong bullish sign.
        - Aroon Down reaching 100: Indicates a new low was made very recently, strong bearish sign.
        - Aroon Up near 0: No recent highs, weak uptrend.
        - Aroon Down near 0: No recent lows, weak downtrend.
    - **Aroon Oscillator**:
        - *Positive values (above 0)*: Indicate bullish momentum (Aroon Up > Aroon Down). Stronger uptrends are associated with values closer to +100.
        - *Negative values (below 0)*: Indicate bearish momentum (Aroon Down > Aroon Up). Stronger downtrends are associated with values closer to -100.
        - *Zero Line Crossover*:
            - Bullish: Oscillator crosses above 0.
            - Bearish: Oscillator crosses below 0.
        - *Extreme Levels*: Oscillator approaching +100 signals a strong uptrend; approaching -100 signals a strong downtrend.
- **Considerations**:
    - **Period Setting**: The standard period for Aroon is typically 14 or 25 periods. Shorter periods make it more sensitive to recent price action; longer periods make it smoother.
    - **Lagging Nature**: While designed to detect early trend changes, it can still lag, especially with longer periods.
    - **Whipsaws**: Crossovers can lead to whipsaws in choppy or sideways markets. It's often useful to confirm signals with other indicators or price action.
    - **Not a Measure of Volatility**: Aroon focuses on the time aspect of highs and lows, not the magnitude of price changes.

### Ichimoku Cloud (Ichimoku Kinko Hyo)
- **Calculation Function**: `calculate_ichimoku_cloud`
- **Description**: The Ichimoku Cloud, or Ichimoku Kinko Hyo ("one look equilibrium chart"), is a comprehensive, all-in-one indicator designed to show trend direction, momentum, and dynamic support and resistance levels. It consists of five main components:
    - **Tenkan-sen (Conversion Line)**: Calculated as (Highest High + Lowest Low) / 2 over the last 9 periods. It's a short-term momentum line and can act as minor support/resistance.
    - **Kijun-sen (Base Line)**: Calculated as (Highest High + Lowest Low) / 2 over the last 26 periods. It's a medium-term momentum line and a more significant indicator of trend and support/resistance than the Tenkan-sen.
    - **Senkou Span A (Leading Span A)**: Calculated as (Tenkan-sen + Kijun-sen) / 2, and then plotted 26 periods ahead. It forms the faster boundary of the Kumo (Cloud).
    - **Senkou Span B (Leading Span B)**: Calculated as (Highest High + Lowest Low) / 2 over the last 52 periods, and then plotted 26 periods ahead. It forms the slower boundary of the Kumo (Cloud).
    - **Chikou Span (Lagging Span)**: The current closing price plotted 26 periods behind.
    - The **Kumo (Cloud)** is the area between Senkou Span A and Senkou Span B.
- **Trading Strategies & Interpretation**:
    - **Trend Identification (Price vs. Kumo)**:
        - *Bullish Trend*: Price is trading above the Kumo. The Kumo acts as dynamic support.
        - *Bearish Trend*: Price is trading below the Kumo. The Kumo acts as dynamic resistance.
        - *Sideways/Neutral*: Price is trading inside the Kumo. The Kumo boundaries can offer support/resistance.
    - **Kumo Color & Thickness**:
        - *Bullish Kumo*: Senkou Span A is above Senkou Span B.
        - *Bearish Kumo*: Senkou Span B is above Senkou Span A.
        - A thicker Kumo suggests stronger support/resistance.
    - **Tenkan-sen / Kijun-sen Crossovers**:
        - *Bullish Signal (Strong)*: Tenkan-sen crosses above Kijun-sen, and the crossover occurs above the Kumo.
        - *Bullish Signal (Medium)*: Tenkan-sen crosses above Kijun-sen, and the crossover occurs inside the Kumo.
        - *Bullish Signal (Weak)*: Tenkan-sen crosses above Kijun-sen, and the crossover occurs below the Kumo.
        - *Bearish Signals*: Vice-versa for Tenkan-sen crossing below Kijun-sen.
    - **Chikou Span (Lagging Span) Confirmation**:
        - *Bullish Confirmation*: Chikou Span is above the price action from 26 periods ago.
        - *Bearish Confirmation*: Chikou Span is below the price action from 26 periods ago.
        - Chikou Span breaking through the Kumo can also be a signal.
    - **Kumo Breakouts**:
        - *Bullish*: Price breaks out above the Kumo.
        - *Bearish*: Price breaks out below the Kumo.
    - **Senkou Span Crossovers (Kumo Twist)**: When Senkou Span A and Senkou Span B cross, it indicates a potential future trend change.
- **Considerations**:
    - **Default Periods**: The standard Ichimoku periods are 9, 26, and 52. These were based on historical Japanese trading weeks and months. Some traders adjust these for different markets or timeframes, but many traditionalists stick to the defaults.
    - **Complexity**: Ichimoku provides a lot of information, which can be overwhelming for new users. It's important to understand each component and how they interact.
    - **Lagging Elements**: While Senkou Spans are plotted ahead, the Tenkan-sen, Kijun-sen, and Chikou Span are based on past data, introducing some lag.
    - **Best for Trending Markets**: Like many trend indicators, Ichimoku tends to perform best in trending markets and can give ambiguous signals in choppy, sideways conditions.
    - **Trading Strategies & Interpretation**:
        The Ichimoku system provides a multi-faceted view of the market. Strongest signals often occur when multiple components of the system align and confirm each other (e.g., price action relative to the Kumo, Kumo direction, Tenkan/Kijun cross, and Chikou Span position).
        - **Trend Identification (Price vs. Kumo)**:
            - *Bullish Trend*: Price is trading above the Kumo. The Kumo acts as dynamic support.
            - *Bearish Trend*: Price is trading below the Kumo. The Kumo acts as dynamic resistance.
            - *Sideways/Neutral*: Price is trading inside the Kumo. The Kumo boundaries can offer support/resistance.
    - **Kumo Color & Thickness**:
        - *Bullish Kumo*: Senkou Span A is above Senkou Span B.
        - *Bearish Kumo*: Senkou Span B is above Senkou Span A.
        - A thicker Kumo suggests stronger support/resistance.
    - **Tenkan-sen / Kijun-sen Crossovers**:
        - *Bullish Signal (Strong)*: Tenkan-sen crosses above Kijun-sen, and the crossover occurs above the Kumo.
        - *Bullish Signal (Medium)*: Tenkan-sen crosses above Kijun-sen, and the crossover occurs inside the Kumo.
        - *Bullish Signal (Weak)*: Tenkan-sen crosses above Kijun-sen, and the crossover occurs below the Kumo.
        - *Bearish Signals*: Vice-versa for Tenkan-sen crossing below Kijun-sen.
    - **Chikou Span (Lagging Span) Confirmation**:
        - *Bullish Confirmation*: Chikou Span is above the price action from 26 periods ago.
        - *Bearish Confirmation*: Chikou Span is below the price action from 26 periods ago.
        - Chikou Span breaking through the Kumo can also be a signal.
    - **Kumo Breakouts**:
        - *Bullish*: Price breaks out above the Kumo.
        - *Bearish*: Price breaks out below the Kumo.
    - **Senkou Span Crossovers (Kumo Twist)**: When Senkou Span A and Senkou Span B cross, it indicates a potential future trend change.
- **Considerations**:
    - **Default Periods**: The standard Ichimoku periods are 9, 26, and 52. These were based on historical Japanese trading weeks and months. Some traders adjust these for different markets or timeframes, but many traditionalists stick to the defaults.
    - **Complexity**: Ichimoku provides a lot of information, which can be overwhelming for new users. It's important to understand each component and how they interact.
    - **Lagging Elements**: While Senkou Spans are plotted ahead, the Tenkan-sen, Kijun-sen, and Chikou Span are based on past data, introducing some lag.
    - **Best for Trending Markets**: Like many trend indicators, Ichimoku tends to perform best in trending markets and can give ambiguous signals in choppy, sideways conditions.

### Parabolic SAR (PSAR)
- **Calculation Function**: `calculate_psar`
- **Description**: Developed by J. Welles Wilder Jr., the Parabolic SAR (Stop and Reverse) is a time and price technical indicator primarily used to identify potential stop-loss levels and trend reversals. It appears on a chart as a series of dots either above or below the price bars.
    - When the dots are below the price, it suggests an uptrend.
    - When the dots are above the price, it suggests a downtrend.
    - The indicator is designed to move closer to the price as a trend matures. If the price touches or crosses a dot, it signals a potential reversal, and the dots flip to the opposite side of the price.
- **Trading Strategies**:
    - **Trend Following & Exit Signals (Primary Use)**:
        - *Uptrend*: When PSAR dots are below the price, traders might hold long positions. The dots act as a trailing stop-loss. If the price falls and hits the PSAR dot, it can be a signal to exit the long position and potentially consider a short.
        - *Downtrend*: When PSAR dots are above the price, traders might hold short positions. The dots act as a trailing stop-loss. If the price rises and hits the PSAR dot, it can be a signal to exit the short position and potentially consider a long.
    - **Entry Signals**:
        - *Buy Signal*: When the PSAR dots flip from above the price to below the price.
        - *Sell Signal*: When the PSAR dots flip from below the price to above the price.
    - **Confirmation with Other Indicators**: PSAR signals are often stronger when confirmed by other trend-following indicators (like ADX or moving averages) or momentum oscillators.
- **Considerations**:
    - **Sensitivity (Acceleration Factor)**: PSAR uses an "acceleration factor" (AF) that increases as the trend extends. The step (initial AF, e.g., 0.02) and maximum AF (e.g., 0.20) are key parameters. Smaller steps make it less sensitive (fewer reversals, more lag); larger steps make it more sensitive (quicker reversals, more whipsaws).
    - **Whipsaws in Sideways Markets**: PSAR is notorious for generating many false signals (whipsaws) in non-trending, sideways, or choppy markets because it is always in the market (either long or short). It performs best in markets with sustained trends.
    - **Stop-Loss Placement**: While it provides stop-loss points, these can sometimes be too close or too far depending on volatility. Some traders use it as a reference rather than a hard stop.
    - **Not for Determining Trend Strength**: PSAR primarily indicates trend direction and potential reversal points, not the strength of the trend. For trend strength, indicators like ADX are more suitable.
    - **Mechanical Nature**: Its stop-and-reverse nature is mechanical, which can be an advantage for disciplined trading but may also lead to overtrading if not managed properly.

### Vortex Indicator (VI)
- **Calculation Function**: `calculate_vortex`
- **Description**: The Vortex Indicator (VI) was developed by Etienne Botes and Douglas Siepman and is designed to identify the start of a new trend or the continuation of an existing trend. It consists of two oscillating lines:
    - **Positive Vortex Indicator (VI+)**: Measures upward trend movement.
    - **Negative Vortex Indicator (VI-)**: Measures downward trend movement.
    - These lines are typically plotted below a price chart and oscillate around each other.
- **Trading Strategies**:
    - **Crossovers (Primary Signal)**:
        - *Bullish Crossover (Buy Signal)*: VI+ crosses above VI-. This suggests that upward momentum is taking over from downward momentum and can signal the beginning of an uptrend.
        - *Bearish Crossover (Sell Signal)*: VI- crosses above VI+. This suggests that downward momentum is taking over from upward momentum and can signal the beginning of a downtrend.
    - **Trend Confirmation**:
        - *Uptrend Confirmation*: After a bullish crossover, if VI+ remains consistently above VI-, it helps to confirm the uptrend.
        - *Downtrend Confirmation*: After a bearish crossover, if VI- remains consistently above VI+, it helps to confirm the downtrend.
        - The wider the separation between VI+ and VI- after a crossover, the stronger the indicated trend might be.
    - **Combining with Other Indicators**:
        - Vortex Indicator signals are often used in conjunction with other technical analysis tools for confirmation.
        - For example, a VI+ crossover above VI- might be considered a stronger buy signal if it occurs when the price is above a key moving average or if another momentum indicator (like RSI or MACD) also shows bullish signs.
    - **Filtering Signals (Using a Threshold)**:
        - Some traders apply a threshold level (e.g., 1.0, 1.1) to the dominant line after a crossover to filter out weaker signals. For example, waiting for VI+ to not only cross VI- but also to rise above a certain level before considering a buy signal.
- **Considerations**:
    - **Lagging Nature**: Like many trend-following indicators, the Vortex Indicator can lag behind price movements, especially in fast-moving markets. Crossover signals may occur after a significant portion of the move has already happened.
    - **Whipsaws in Ranging Markets**: In sideways or choppy market conditions, VI+ and VI- can cross frequently, leading to false signals or whipsaws. The indicator is generally more reliable in trending markets.
    - **Period Setting**: The common period setting for the Vortex Indicator is 14 periods, but this can be adjusted. Shorter periods will make the indicator more sensitive (more signals, potentially more false ones), while longer periods will make it less sensitive (fewer signals, potentially later ones).
    - **Not an Overbought/Oversold Indicator**: Unlike oscillators like RSI or Stochastics, the Vortex Indicator does not have defined overbought or oversold levels.

---

## Cycle Indicators

Cycle indicators are designed to identify cyclical patterns in market prices, such as those that might be driven by seasonal factors, economic cycles, or recurring market behaviors. These indicators attempt to measure the timing and duration of these cycles, helping traders anticipate potential turning points where a cycle might be peaking or bottoming out. They often involve analyzing price oscillations and periodicity.

### Hilbert Transform - Dominant Cycle Period (HT DCP)
- **Calculation Function**: `calculate_ht_dcperiod`
- **Description**: Developed by John Ehlers, the Hilbert Transform Dominant Cycle Period is an indicator that attempts to measure the period (length in bars) of the current dominant price cycle in the market. Unlike fixed-period indicators, the HT DCP adapts to changing market conditions by identifying the most significant cycle dynamically. This information can be crucial for timing entries/exits or for tuning other cycle-based indicators.
- **Trading Strategies & Interpretation**:
    - **Adaptive Indicator Tuning**: The primary use of the HT DCP is often to provide an adaptive period length for other indicators. For example, instead of using a fixed 14-period RSI, a trader might use an RSI whose period is dynamically set by the HT DCP. This allows indicators to respond more appropriately to current market cyclicality.
    - **Cycle Turning Points**: By understanding the dominant cycle period, traders can anticipate potential cycle highs and lows. If the HT DCP shows a consistent period (e.g., 20 bars), traders might look for signs of a reversal as the price approaches the end of a 20-bar cycle from a previous turning point.
    - **Confirmation of Cycle-Based Strategies**: When used with other cycle indicators (like Ehlers' Sinewave or Phasor indicators), the HT DCP can help confirm if the market is indeed in a cyclical mode and what the current cycle length is, thereby validating signals from those other tools.
    - **Identifying Changes in Market Character**: A significant change in the HT DCP value can indicate a shift in the market's underlying rhythm. For example, a lengthening cycle period might suggest a market moving into a broader, slower trend, while a shortening period could indicate faster, choppier conditions.
    - **Frequency Analysis**: The DCP value directly relates to the frequency of the dominant cycle. This can be used in more advanced DSP (Digital Signal Processing) based trading strategies.
- **Considerations**:
    - **Complexity**: The Hilbert Transform and its derived indicators are mathematically complex. While their application can be straightforward (e.g., reading the DCP value), understanding their nuances requires some study of Ehlers' work.
    - **Non-Trending Markets**: Cycle indicators, including the HT DCP, generally perform best when the market exhibits some cyclical behavior. In strongly trending markets with little oscillation, or in very flat, non-volatile markets, the concept of a 'dominant cycle' may be less meaningful or harder to identify reliably.
    - **Lag**: As with any indicator calculating based on past data, there will be some lag in the measurement of the cycle period.
    - **Use with Other Hilbert Indicators**: The HT DCP is often a foundational component within a suite of Ehlers' cycle indicators. Its real power is often unlocked when used in combination with `calculate_ht_sine`, `calculate_ht_dcphase`, or `calculate_ht_trendmode` to build a more complete picture of market cycles.

### Hilbert Transform - Dominant Cycle Phase (HT DCPHASE)
- **Calculation Function**: `calculate_ht_dcphase`
- **Description**: Following the identification of the Dominant Cycle Period (HT DCPERIOD), the Hilbert Transform Dominant Cycle Phase indicator measures the current phase angle of that dominant cycle. The phase typically ranges from 0 to 360 degrees, representing the position within the current cycle.
- **Trading Strategies & Interpretation**:
    - **Component for Sine Wave Indicators**: The HT DCPHASE is a critical input for constructing Ehlers' Sine Wave and Lead Sine Wave indicators (see `calculate_ht_sine`). It provides the angle for the sine function calculations.
    - **Understanding Cycle Position**: Knowing the phase helps in gauging where the price is within its current dominant cycle. For example, a phase near 0 or 360 degrees might indicate the beginning/end of a cycle, while a phase near 90 degrees could be the rising part, 180 degrees near the peak, and 270 degrees the falling part.
    - **Anticipating Turns**: While not typically used as a standalone signal generator, observing the progression of the phase can help anticipate when a cycle might be due for a turn, especially when approaching 0/360 degrees or 180 degrees.
- **Considerations**:
    - **Best Used with HT SINE**: The practical application of HT DCPHASE is most evident when used to generate the Sine Wave indicators.
    - **Requires Cyclical Market**: Its interpretation relies on the market exhibiting cyclical behavior as identified by HT DCPERIOD and HT TRENDMODE.

### Hilbert Transform - Sine Wave (HT SINE)
- **Calculation Function**: `calculate_ht_sine` (likely returns both Sine and Lead Sine)
- **Description**: The Hilbert Transform Sine Wave indicator, developed by John Ehlers, is designed to identify cyclic turning points with minimal lag. It consists of two lines:
    - **Sine Wave**: This is `sin(Dominant Cycle Phase)`.
    - **Lead Sine Wave**: This is `sin(Dominant Cycle Phase + 45 degrees)`. The 45-degree advance means the Lead Sine Wave effectively 'leads' the Sine Wave.
- **Trading Strategies & Interpretation**:
    - **Crossovers as Primary Signals**:
        - *Buy Signal*: When the Sine Wave crosses above the Lead Sine Wave. This often anticipates a cyclic trough by approximately 1/8th of the cycle period.
        - *Sell Signal*: When the Sine Wave crosses below the Lead Sine Wave. This often anticipates a cyclic peak by approximately 1/8th of the cycle period.
    - **Trend Mode Behavior**: A key feature highlighted by Ehlers is that in a strong trending market (where `calculate_ht_trendmode` would indicate a trend), the phase changes very little. Because the Sine and Lead Sine are 45 degrees apart, they will not cross during such trending periods, helping to avoid false signals (whipsaws) common with other oscillators in trends.
    - **Identifying Cycle Mode**: Trading these crossover signals is typically recommended when the market is identified to be in a "Cycle Mode" (e.g., via `calculate_ht_trendmode`).
- **Considerations**:
    - **Dependent on Accurate Cycle Measurement**: The effectiveness of the HT SINE relies on the accurate measurement of the Dominant Cycle Period and Phase by `calculate_ht_dcperiod` and `calculate_ht_dcphase`.
    - **Best in Ranging/Cyclical Markets**: While designed to filter out trend-based whipsaws, it's primarily a cycle-finding tool. Its most potent signals occur when the market is oscillating.
    - **Combine with HT TRENDMODE**: It's highly recommended to use this indicator in conjunction with `calculate_ht_trendmode` to determine if the market is in a cycle mode (where signals are more reliable) or a trend mode (where crossover signals should ideally not occur or be ignored).

### Hilbert Transform - Phasor Components (HT PHASOR)
- **Calculation Function**: `calculate_ht_phasor` (likely returns both In-Phase and Quadrature components)
- **Description**: The Hilbert Transform decomposes a detrended price series into two orthogonal components:
    - **In-Phase Component (Real Part)**: Represents the cyclical part of the price movement as it would appear on a chart, but filtered to emphasize the dominant cycle.
    - **Quadrature Component (Imaginary Part)**: This component is 90 degrees out of phase with the In-Phase component. It's essential for calculating the instantaneous phase and amplitude of the cycle.
    - Together, these components describe the price as a phasor (a rotating vector), where the angle of the phasor is the Dominant Cycle Phase and its length is related to the cycle's amplitude.
- **Trading Strategies & Interpretation**:
    - **Foundation for Other HT Indicators**: The primary role of the In-Phase and Quadrature components is to serve as the basis for calculating other Hilbert Transform indicators like Dominant Cycle Period (`calculate_ht_dcperiod`), Dominant Cycle Phase (`calculate_ht_dcphase`), and the Sine Wave indicators (`calculate_ht_sine`).
    - **Phasor Displays (Advanced Analysis)**: Advanced users might plot the Quadrature component against the In-Phase component. In a perfect, stable cycle, this would form a circle. Deviations from a circle (e.g., becoming elliptical or noisy) can indicate changes in cycle amplitude, stability, or the presence of noise.
    - **Amplitude and Phase Information**: The magnitude of these components can be used to derive the instantaneous amplitude of the cycle. The relationship between In-Phase and Quadrature (specifically, their arctangent) directly gives the Dominant Cycle Phase.
    - **Direct Signal Generation (Less Common)**: While less common for direct signals, some advanced strategies might look for specific patterns or relationships between the In-Phase and Quadrature components to anticipate cycle behavior. For instance, the Quadrature component can be seen as leading the In-Phase component in terms of phase.
- **Considerations**:
    - **Intermediate Calculation**: For most traders, these components are intermediate steps. The more directly actionable signals come from indicators like HT SINE or HT TRENDMODE.
    - **Requires Understanding of DSP Concepts**: A deeper understanding of Digital Signal Processing (DSP) and how phasors represent cyclical data is beneficial for interpreting these components directly.
    - **Visualization**: Plotting these components directly on a price chart can be less intuitive than the derived Sine Wave or Trend Mode indicators. Phasor displays (Quadrature vs. In-Phase) are a specific type of chart for their analysis.

### Hilbert Transform - Trend vs Cycle Mode (HT TRENDMODE)
- **Calculation Function**: `calculate_ht_trendmode`
- **Description**: The Hilbert Transform Trend vs Cycle Mode indicator, developed by John Ehlers, determines whether the market is currently in a "Trend Mode" or a "Cycle Mode." 
    - **Trend Mode**: Characterized by persistent directional price movement (up or down). In this mode, trend-following strategies are generally more effective.
    - **Cycle Mode**: Characterized by prices oscillating around a mean, with discernible peaks and troughs. In this mode, oscillator-based strategies and mean-reversion techniques are typically more appropriate.
    - The indicator usually outputs a binary value (e.g., 1 for Trend Mode, 0 for Cycle Mode) or switches between two states.
- **Trading Strategies & Interpretation**:
    - **Strategy Selection Filter**: This is the primary use of HT TRENDMODE. It acts as a filter to help traders decide which type of trading strategy to employ:
        - If HT TRENDMODE indicates **Trend Mode**: Favor trend-following indicators (e.g., moving averages, ADX) and strategies (e.g., breakout trading, holding positions longer).
        - If HT TRENDMODE indicates **Cycle Mode**: Favor oscillators (e.g., HT SINE, RSI, Stochastics) and strategies that capitalize on oscillations (e.g., buying at cyclic troughs and selling at cyclic peaks, mean reversion).
    - **Signal Confirmation/Rejection for Other HT Indicators**: 
        - Signals from the HT SINE Wave indicator (crossovers) are generally considered more reliable when HT TRENDMODE indicates Cycle Mode. Some strategies might ignore HT SINE signals if the market is in Trend Mode, as the Sine/Lead Sine lines are designed not to cross then.
    - **Identifying Market Shifts**: A change in the HT TRENDMODE output (e.g., from Cycle to Trend, or vice-versa) can alert traders to a potential shift in the underlying market character, prompting a re-evaluation of their current strategy and indicators.
    - **Adaptive Systems**: In automated trading systems, HT TRENDMODE can be used to dynamically switch between different trading algorithms or parameter sets optimized for trending or cycling conditions.
- **Considerations**:
    - **Integration with Other Indicators**: HT TRENDMODE is rarely used in isolation. Its value comes from its role as a regime filter for other analytical tools and strategies.
    - **Definition of Trend/Cycle**: The exact mathematical determination of Trend vs. Cycle mode by the indicator is based on Ehlers' specific criteria derived from the Hilbert Transform analysis of price data (often involving the relationship between the In-Phase and Quadrature components and the rate of phase change).
    - **Transitions**: The transition periods when the market is shifting from a Cycle Mode to a Trend Mode (or vice versa) can still be challenging, as the indicator will eventually switch but might lag slightly behind the initial moments of the change.
    - **Not a Directional Indicator**: HT TRENDMODE itself does not predict the direction of a trend or a cycle; it only indicates the *type* of market behavior that is currently dominant.

---

## Pattern Recognition

Pattern recognition indicators automatically identify common chart patterns formed by price movements. These patterns, such as candlestick patterns (e.g., Doji, Hammer, Engulfing) or classical chart patterns (e.g., Head and Shoulders, Triangles), can provide insights into potential trend continuations, reversals, or market indecision. Automating pattern detection helps traders systematically incorporate these formations into their analysis.

### Candlestick Pattern Recognition (Future Implementation)
- **Calculation Function**: `recognize_patterns` (located in `src/indicators/pattern_recognition/candlestick.rs`)
- **Description**: This function is intended to automatically identify various candlestick patterns from OHLC price data. Candlestick patterns can provide short-term signals about potential price reversals, continuations, or market indecision.
- **Current Status**: As of the current version, the `recognize_patterns` function is a **placeholder** for future development. The full implementation of specific pattern detection logic is planned.
- **Intended Patterns (Examples for future inclusion)**:
    - **Doji**: Indicates indecision, where open and close prices are very close.
    - **Hammer/Hanging Man**: Single candlestick patterns that can signal potential reversals depending on context.
    - **Engulfing (Bullish/Bearish)**: Two-candlestick patterns where the body of the second candle engulfs the first, often signaling a strong reversal.
    - **Morning Star/Evening Star**: Three-candlestick reversal patterns.
    - And other common candlestick formations.
- **Trading Strategies**: Detailed trading strategies and interpretations will be provided once the pattern recognition functionality is implemented and specific patterns are supported.

---

## Price Transformation

Price transformation functions modify or rescale price data to highlight different aspects of price action or to prepare data for other types of indicators. These transformations can include things like calculating typical price, median price, or applying mathematical functions to the price series. They are often used as a preliminary step in more complex indicator calculations or to provide a different perspective on price itself.

### Average Price (AVGPRICE)
- **Calculation Function**: `calculate_avgprice`
- **Description**: Average Price is calculated as the average of the four main price points: Open, High, Low, and Close. 
  Formula: `(Open + High + Low + Close) / 4`
- **Interpretation & Usage**:
    - Provides a more comprehensive single price point for a period compared to just the closing price.
    - Can be used as a component in other indicator calculations or as a smoother representation of price in some analyses.
    - Less commonly used for direct trading signals but can help in understanding the period's overall price level.
- **Considerations**:
    - It gives equal weight to all four price points. If one of these points (e.g., an unusually high spike) is an outlier, it can skew the average price.

### Median Price (MEDPRICE)
- **Calculation Function**: `calculate_medprice`
- **Description**: Median Price, also known as High-Low Price, represents the midpoint of the trading range for the period.
  Formula: `(High + Low) / 2`
- **Interpretation & Usage**:
    - Provides a simple measure of the central tendency of prices within a period, focusing only on the extremes (high and low).
    - Often used as a filter or an input for other indicators, such as some types of moving averages or oscillators.
    - Can be used in Donchian Channels or other channel-based indicators where the midpoint of a range is relevant.
    - It gives a quick sense of the average price without considering the open or close specifically.
- **Considerations**:
    - It ignores the open and close prices, so it doesn't capture information about buying or selling pressure at the start or end of the period.
    - Can be volatile if highs or lows are spiky.

### Typical Price (TYPPRICE)
- **Calculation Function**: `calculate_typprice`
- **Description**: Typical Price is a weighted average of the High, Low, and Close prices for a period. It aims to provide a more representative price level than just the closing price.
  Formula: `(High + Low + Close) / 3`
- **Interpretation & Usage**:
    - Offers a smoothed single-value representation of the price for a given period.
    - Commonly used as an input for other technical indicators, such as the Money Flow Index (MFI), Commodity Channel Index (CCI), and Volume Weighted Average Price (VWAP) if only HLC data is available for VWAP calculation instead of tick data.
    - Some traders use moving averages of the Typical Price instead of the Close price for a smoother trend indication.
- **Considerations**:
    - It gives equal weight to the high, low, and close, but excludes the open price.
    - While smoother than using just the close, it still reacts to price extremes within the period (high and low).

### Weighted Close Price (WCLPRICE)
- **Calculation Function**: `calculate_wclprice`
- **Description**: Weighted Close Price places more emphasis on the closing price compared to the high and low prices of the period. 
  Common Formula: `(High + Low + Close + Close) / 4` or `(H + L + 2*C) / 4`
- **Interpretation & Usage**:
    - Provides a price point that gives greater importance to the closing price, which many traders consider the most significant price of the day.
    - Used as an input for some technical indicators or in trading systems where the close is deemed more critical than other price points of the period.
    - Can be seen as a variation of Typical Price or Average Price, but with a bias towards the close.
- **Considerations**:
    - The specific weighting can vary, but the common approach doubles the weight of the close.
    - By emphasizing the close, it tries to capture the final sentiment of the period more strongly.

---

## Statistical Tools

Statistical tools in technical analysis apply concepts from statistics to analyze price data, volatility, and relationships between different financial instruments. These tools can help in quantifying risk, identifying unusual deviations, or understanding the distribution of price changes. Examples include standard deviation, variance, correlation, and regression analysis.

### Beta (β)
- **Calculation Function**: `calculate_beta`
- **Description**: Beta (β) is a statistical measure that quantifies the volatility of an asset (e.g., a stock) or a portfolio in relation to the overall market or a specified benchmark. It indicates how much the asset's price is expected to move for a 1% change in the benchmark. To calculate Beta, you typically need the historical price series of the asset and the benchmark.
- **Interpretation**:
    - **β = 1.0**: The asset's price is expected to move in line with the market. If the market moves up by 1%, the asset is expected to move up by 1%.
    - **β > 1.0**: The asset is more volatile than the market. For example, if β = 1.5, the asset is expected to move 1.5% for every 1% move in the market (in the same direction). These are often referred to as "aggressive" assets.
    - **β < 1.0 (but > 0)**: The asset is less volatile than the market. For example, if β = 0.7, the asset is expected to move 0.7% for every 1% move in the market. These are often referred to as "defensive" assets.
    - **β = 0**: The asset's movement is uncorrelated with the market's movement.
    - **β < 0**: The asset is expected to move in the opposite direction of the market. For example, if β = -0.5, and the market moves up by 1%, the asset is expected to move down by 0.5%. Such assets are rare but can be valuable for diversification (e.g., some gold-related assets during market downturns).
- **Usage**:
    - **Risk Assessment**: Beta is a key measure of systematic risk – the risk inherent to the entire market that cannot be diversified away. Higher beta implies higher systematic risk.
    - **Portfolio Construction**: Investors use beta to construct portfolios that align with their risk tolerance. For example, a risk-averse investor might prefer stocks with low betas.
    - **CAPM (Capital Asset Pricing Model)**: Beta is a crucial input in the CAPM formula, which is used to calculate the expected return of an asset and thus its cost of equity.
    - **Performance Benchmarking**: Comparing an asset's beta to its actual returns can help in evaluating its risk-adjusted performance.
- **Considerations**:
    - **Historical Data**: Beta is calculated using historical price data and assumes that past volatility relationships will continue in the future, which may not always be true.
    - **Benchmark Selection**: The choice of benchmark (e.g., S&P 500, FTSE 100) is important and can affect the beta value. The benchmark should be relevant to the asset being analyzed.
    - **Changing Beta**: A company's beta can change over time due to changes in its business model, financial leverage, or market conditions.
    - **R-squared**: When evaluating beta, it's also useful to consider the R-squared value from the regression. R-squared indicates the percentage of an asset's price movements that can be explained by movements in the benchmark. A low R-squared might suggest that beta is not a very reliable measure of the asset's risk relative to that benchmark.

---

## Stock-Specific Indicators

Stock-specific indicators are tailored to the unique characteristics and data available for equity markets. These can include indicators based on price action patterns common in stocks, fundamental data (if available and integrated), or specialized volume analysis. They aim to provide insights particularly relevant for stock trading and investment decisions.

### Fundamental Stock Analysis (Future Implementation)

The library plans to include indicators that integrate fundamental data with technical analysis. These tools aim to help identify undervalued or growth stocks by combining financial metrics with price action. The configuration for these indicators may be managed via the `FundamentalIndicators` struct.

Currently, the following functions are placeholders for future development:

- **`peg_ratio_with_technical_trigger`**
    - **Intended Purpose**: To combine the Price/Earnings to Growth (PEG) ratio with technical uptrend signals to identify growth stocks at a reasonable price (GARP).

### Price Action Analysis
This subsection covers indicators and functions that analyze stock-specific price and volume patterns.

- **Configuration**: The `StockPricePatterns` struct (with fields like `min_volume_increase`, `min_price_range`, `lookback_period`) can be used to configure parameters for these types of analyses, though its direct usage by the currently implemented functions might vary.

**Implemented Functions:**

- **`detect_stock_breakouts`**
    - **Calculation Function**: `detect_stock_breakouts`
    - **Description**: This function identifies potential stock breakout days. A breakout is typically characterized by a significant price move accompanied by increased volume, often occurring after a period of consolidation or when a key price level is breached. This function specifically looks for:
        1.  **Volume Surge**: Current volume is significantly higher than the average volume over a lookback period (e.g., 20 days). The `min_volume_ratio` parameter controls this (e.g., current volume must be at least X times the average volume).
        2.  **Significant Price Change**: The absolute percentage change in price for the day meets a minimum threshold defined by `min_price_change`.
        3.  **Gap Condition**: The day exhibits a price gap, meaning either the current day's low is above the previous day's high (gap up) or the current day's high is below the previous day's low (gap down).
    - **Inputs**:
        - `df`: DataFrame with OHLCV data (columns: "close", "volume", "high", "low").
        - `min_volume_ratio`: The minimum ratio of current volume to average volume to qualify (e.g., 2.0 for 2x average volume).
        - `min_price_change`: The minimum absolute percentage price change to qualify (e.g., 0.03 for 3%).
    - **Output**: Returns a `PolarsResult<Series>` containing a boolean Series named "stock_breakouts". `true` indicates a potential breakout day according to the criteria.
    - **Trading Strategies & Interpretation**:
        - *Breakout Confirmation*: A `true` signal can suggest that a stock is breaking out of a range or starting a new strong move. Traders might look for such signals to enter positions in the direction of the breakout.
        - *Further Analysis*: Breakout signals should ideally be confirmed with other indicators or chart analysis (e.g., breaking a clear resistance/support level, trend lines).
        - *Entry Points*: For a bullish breakout (price increase with gap up), traders might enter long. For a bearish breakout (price decrease with gap down), traders might consider short positions or exiting longs.
        - *Stop-Loss*: Place stop-losses appropriately, as false breakouts can occur. For example, below the breakout day's low for a long entry.
    - **Considerations**:
        - The parameters `min_volume_ratio` and `min_price_change` need to be tuned based on the stock's volatility and market conditions.
        - Gaps can sometimes fill. Not all breakouts lead to sustained moves.
        - This function provides a starting point for identifying potential breakouts; human discretion and further analysis are often required.

**Future Implementations (Currently Placeholders):**

- **`detect_institutional_activity`**
    - **Intended Purpose**: To identify periods of potential significant buying or selling by institutional investors, often by analyzing large volume spikes (block trades) in relation to price movements.
    - **Current Status**: Placeholder. Not yet implemented.

- **`detect_supply_demand_blocks`**
    - **Intended Purpose**: To identify price zones where significant supply (resistance) or demand (support) has previously occurred, often characterized by high volume and price rejection. These zones can act as future barriers or support levels.
    - **Current Status**: Placeholder. Not yet implemented.

---

## Options-Specific Indicators

Options-specific indicators are designed for analyzing options contracts and their underlying assets. These indicators often involve calculations related to volatility (e.g., implied volatility), option pricing models (e.g., Black-Scholes), and the "Greeks" (Delta, Gamma, Theta, Vega, Rho), which measure the sensitivity of an option's price to various factors. They help options traders assess risk, identify mispricings, and formulate strategies.

### Option Greeks

Option Greeks are a set of risk measures that describe the sensitivity of an option's price to various factors. Understanding Greeks is crucial for options traders to manage risk and develop strategies.

- **`GreeksCalculator` Struct**: This struct is available to hold configuration parameters relevant for Greek calculations, such as `risk_free_rate` and the `pricing_model` (e.g., "black_scholes").

- **`calculate_option_greeks` Function**
    - **Description**: This is the primary function for calculating the basic set of first-order Greeks for a single European-style option. While the internal calculations are currently simplified placeholders, it aims to provide values for Delta, Gamma, Theta, Vega, and Rho.
    - **Inputs**:
        - `spot_price`: Current price of the underlying asset.
        - `strike_price`: Strike price of the option.
        - `time_to_expiry`: Time to expiration in years (e.g., 30 days = 30.0/365.0).
        - `volatility`: Implied volatility of the option, expressed as a decimal (e.g., 0.20 for 20%).
        - `is_call`: Boolean, `true` if the option is a call, `false` if it's a put.
        - `risk_free_rate`: The risk-free interest rate, as a decimal (e.g., 0.02 for 2%).
        - `_dividend_yield`: The dividend yield of the underlying asset, as a decimal (currently a placeholder in calculations but important for accurate pricing).
    - **Output**: Returns a `HashMap<String, f64>` where keys are the names of the Greeks ("delta", "gamma", "theta", "vega", "rho") and values are their calculated figures.
        - **Delta**: Measures the rate of change of the option's price with respect to a $1 change in the underlying asset's price. Ranges from 0 to 1 for calls, -1 to 0 for puts.
        - **Gamma**: Measures the rate of change in an option's delta with respect to a $1 change in the underlying asset's price. It indicates how much the delta will change as the underlying price changes.
        - **Theta**: Measures the rate of change of the option's price with respect to the passage of time (time decay). It's typically negative, meaning options lose value as they approach expiration.
        - **Vega**: Measures the rate of change of the option's price with respect to a 1% change in the implied volatility of the underlying asset. 
        - **Rho**: Measures the rate of change of the option's price with respect to a 1% change in interest rates.
    - **Trading Strategies & Interpretation**: The Greeks are fundamental for most options trading strategies. For example:
        - Delta hedging involves creating a portfolio that is neutral to small changes in the underlying asset's price.
        - Gamma trading strategies attempt to profit from changes in delta (i.e., from changes in the rate of price change).
        - Theta-positive strategies (e.g., selling options) aim to profit from time decay.
        - Vega-sensitive strategies aim to profit from changes in implied volatility.
    - **Considerations**: The current implementation provides simplified placeholder calculations for the Greeks. For precise risk management and trading, these would need to be based on established models like Black-Scholes-Merton.

- **Related Greeks-Based Analysis Functions (Future Implementation)**: The `greeks.rs` module also outlines several other functions that are currently placeholders but indicate planned functionality for more advanced options analysis:
    - **`delta_based_signals`**: Intended to generate trading signals (bullish/bearish/neutral) by analyzing option deltas across different strikes and expirations.
    - **`calculate_gamma_exposure`**: Intended to calculate total gamma exposure at various price levels of the underlying, which can help identify points of market instability or where dealer hedging might accelerate price moves.
    - **`find_highest_theta_options`**: Intended to screen for options with the highest time decay (theta), useful for strategies like selling premium.
    - **`calculate_historical_volatility`**: A utility to calculate the historical volatility of the underlying asset, often used as an input or benchmark for implied volatility.
    - **`find_strikes_by_delta`**: Intended to help identify option strikes that correspond to specific delta values (e.g., finding the 30-delta put).
    - **`options_screening`**: A general function intended for screening options based on a combination of criteria (e.g., Greeks, volume, open interest).
    - **`high_iv_premium_options`**: Intended to find options with high implied volatility and potentially high premium, for strategies focusing on volatility. 
    - **Current Status**: All these functions are currently placeholders. Detailed strategies will be provided upon their full implementation.

### Implied Volatility (IV) Analysis

Implied Volatility (IV) is a crucial concept in options trading, representing the market's forecast of a likely movement in a security's price. It is a key input in option pricing models. Analyzing IV can help traders identify potentially mispriced options and assess market sentiment.

- **`IVSurface` Struct**: This struct is designed to hold configuration parameters for more advanced IV analysis, such as constructing and analyzing an implied volatility surface. Its fields include:
    - `min_strikes_for_skew`: Minimum number of strikes to define a valid IV skew.
    - `min_expirations_for_term`: Minimum number of expirations for a valid term structure.
    - `iv_rank_window_days`: Lookback period (e.g., 252 trading days for a year) for calculating IV Rank.

- **`calculate_iv_rank_percentile` Function**
    - **Description**: This function calculates the Implied Volatility Rank (IV Rank) and Implied Volatility Percentile (IV Percentile) for an asset. These metrics help traders understand where the current IV level stands in relation to its historical range over a specified period.
    - **Inputs**:
        - `current_iv`: The current implied volatility of the asset (as a decimal, e.g., 0.30 for 30%).
        - `historical_iv`: A Polars `Series` containing the historical implied volatility values for the lookback period.
    - **Output**: Returns a tuple `(f64, f64)` representing `(iv_rank, iv_percentile)`.
        - **IV Rank**: Shows where the current IV sits relative to its highest and lowest values over the lookback period. It is calculated as `(current_iv - min_iv) / (max_iv - min_iv)` and typically expressed as a percentage (e.g., 0.75 means current IV is at 75% of its range from the low).
        - **IV Percentile**: Indicates the percentage of days in the lookback period where the IV was lower than the current IV.
    - **Trading Strategies & Interpretation**:
        - **High IV Rank/Percentile**: Suggests that current IV is high compared to its recent history. This might make options relatively expensive. Strategies that benefit from a decrease in volatility (e.g., selling premium like short straddles/strangles, credit spreads) are often considered.
        - **Low IV Rank/Percentile**: Suggests that current IV is low compared to its recent history. This might make options relatively cheap. Strategies that benefit from an increase in volatility (e.g., buying options like long straddles/strangles, debit spreads) might be more attractive.
        - Often used as a filter to decide whether to be a net buyer or seller of volatility.
    - **Considerations**: The choice of lookback period (configured via `IVSurface` or passed implicitly through the length of `historical_iv`) significantly affects these metrics. Longer periods provide a broader historical context.

- **Other IV-Based Analysis Functions (Future Implementation)**: The `implied_volatility.rs` module also outlines several other functions for deeper IV analysis, which are currently placeholders:
    - **`calculate_iv_skew`**
        - **Intended Purpose**: To measure the implied volatility skew, which typically refers to the difference in IV between out-of-the-money (OTM) puts and OTM calls (or across different strike prices). A common pattern is "volatility smile" or "smirk."
        - **Trading Strategies & Interpretation (Conceptual)**:
            - *Sentiment Analysis*: A steep negative skew (OTM puts much higher IV than ATM/OTM calls) often indicates bearish sentiment or high demand for downside protection. A flattening skew can suggest complacency. A positive skew (OTM calls higher IV) can indicate bullish sentiment or speculation on sharp upward moves.
            - *Relative Value Trades*: Identify potentially mispriced options by comparing IV across different strikes. For example, if OTM puts appear excessively expensive due to a very steep skew, strategies like selling OTM put spreads might be considered. Conversely, if skew is unusually flat, buying OTM options for protection might be relatively cheaper.
            - *Spread Construction*: The shape of the skew directly impacts the pricing of vertical spreads (e.g., put debit spreads, call credit spreads), diagonal spreads, and calendar spreads. Understanding skew helps in selecting optimal strike prices for these strategies. For instance, put ratio spreads (buying one ATM put and selling two OTM puts) can be used to capitalize on a steep negative skew.
            - *Hedging Cost Assessment*: Skew indicates the relative cost of OTM options. A steep skew means OTM puts (popular for hedging) are more expensive.
            - *Event-Driven Skew Changes*: Anticipate changes in skew around earnings or news. For instance, skew might flatten after an event resolves uncertainty, potentially offering opportunities for those who positioned accordingly (e.g., by selling expensive OTM options pre-event).
        - **Considerations**: The interpretation of skew should be done in the context of the specific asset and overall market conditions. "Normal" skew varies across asset classes.
        - **Current Status**: Placeholder. Detailed strategies will be provided upon full implementation.
    - **`term_structure_analysis`**: Intended to analyze the implied volatility term structure, which is the relationship between IV and time to expiration for options of the same underlying asset and strike price (typically at-the-money). The shape of the term structure (e.g., contango or backwardation) can indicate market expectations for future volatility.
        - **Trading Strategies & Interpretation (Conceptual)**:
            - *Shape Interpretation*:
                - **Contango (Upward Sloping)**: More common; IV is lower for short-dated options and higher for long-dated ones. Suggests expectations of greater uncertainty or specific risk events further in the future.
                - **Backwardation (Downward Sloping)**: IV is higher for short-dated options. Often occurs before significant near-term events (e.g., earnings, economic data) or during periods of high market stress, with expectations that IV will decline after the event or period of stress.
                - **Flat Structure**: IV is relatively consistent across expirations, suggesting stable volatility expectations.
            - *Calendar Spreads (Time Spreads)*: These are primary strategies. Example: In contango, one might sell a relatively expensive (due to time decay or moderate IV) short-dated option and buy a cheaper (in terms of IV or slower decay) long-dated option at the same strike. The goal is to profit from the faster time decay of the short-term option or changes in the term structure's shape.
            - *Event-Driven Strategies*: Before known events (e.g., earnings), short-term IV often spikes, causing backwardation or a kink. Traders might sell expensive short-term options (e.g., short straddle/strangle) if they expect IV to "crush" post-event, more than the underlying price moves.
            - *VIX Term Structure Trading*: Strategies based on whether the VIX futures curve is in contango (short volatility) or backwardation (potentially long volatility, or expecting mean reversion).
            - *Relative Value Trades*: Identifying if specific parts of the term structure appear mispriced relative to historical norms or other related assets. For example, if near-term IV seems too high compared to mid-term IV without a clear catalyst.
        - **Considerations**: The analysis should consider why the term structure has its current shape (e.g., scheduled news, overall market fear levels). Strategies often require managing multiple Greeks.
        - **Current Status**: Placeholder. Detailed strategies will be provided upon full implementation.
    - **`implied_volatility_regime`**: Intended to generate trading signals or guide strategy selection based on identified IV regimes (e.g., high, low, moderate), often determined using IV Rank (IVR) or IV Percentile (IVP).
        - **Trading Strategies & Interpretation (Conceptual)**:
            - *High IV Regime (e.g., High IVR/IVP)*:
                - *Rationale*: Options are relatively expensive. Favor strategies that benefit from IV contraction (vega profit), time decay (theta profit), or the underlying price not moving as drastically as the high IV suggests.
                - *Strategy Types*: Typically volatility-selling or premium-collection strategies.
                - *Examples*: Short Straddles/Strangles, Iron Condors, Credit Spreads (Call or Put), Covered Calls. These strategies offer higher premiums in high IV environments, providing a larger cushion or improved risk-reward.
            - *Low IV Regime (e.g., Low IVR/IVP)*:
                - *Rationale*: Options are relatively cheap. Favor strategies that benefit from IV expansion or a significant price move in the underlying.
                - *Strategy Types*: Typically volatility-buying or premium-paying strategies.
                - *Examples*: Long Straddles/Strangles, Debit Spreads (Call or Put), Calendar Spreads (if anticipating IV rise). The lower cost makes it easier to profit from price moves or IV increases.
            - *Moderate IV Regime*:
                - *Rationale*: Strategy selection might be more nuanced, potentially combining directional views with volatility considerations, or looking for relative value opportunities.
                - *Examples*: Directional debit/credit spreads, or waiting for IV to move to a more extreme regime.
            - *Regime Shift Anticipation*: Strategies could be employed that anticipate a shift from one regime to another (e.g., positioning for an IV crush after an earnings event that caused a temporary high IV regime).
        - **Considerations**: Clear thresholds for defining regimes are needed. IV can remain high or low for extended periods (mean reversion is not a precise timing tool). Regime signals should ideally be combined with other market analysis.
        - **Current Status**: Placeholder. Detailed strategies and interpretations will be provided upon their full implementation.
      - **Current Status**: All these functions are currently placeholders. Detailed strategies and interpretations will be provided upon their full implementation.

## Day Trading Indicators (Future Implementation)

This section outlines indicators specifically designed or optimized for intraday trading, focusing on short-term price movements, momentum, and volume analysis within a single trading day. The following functions are planned for future implementation; their descriptions are based on their intended purpose.

- **`intraday_momentum_oscillator`**
    - **Intended Purpose**: To provide a faster-responding version of a momentum oscillator (like RSI or Stochastic) specifically calibrated for intraday timeframes (e.g., 1-minute, 5-minute, or tick data). It would help identify short-term overbought/oversold conditions and momentum shifts.
    - **Trading Strategies & Interpretation (Conceptual)**:
        - *Overbought/Oversold Reversals*: Enter short positions when the oscillator reaches a high threshold (e.g., >70 or >80) and shows signs of turning down. Enter long positions when it reaches a low threshold (e.g., <30 or <20) and shows signs of turning up. Best used for scalping or very short-term mean reversion plays.
        - *Divergence*: Look for bullish divergence (price lower low, oscillator higher low) to anticipate short-term upward moves, or bearish divergence (price higher high, oscillator lower high) for potential short-term downward moves.
        - *Centerline Crossovers*: If the oscillator has a centerline (e.g., 50), a cross above can be a short-term buy signal, and a cross below a short-term sell signal, indicating a shift in intraday momentum.
        - *Momentum Confirmation for Breakouts*: Use the oscillator to confirm the strength of intraday breakouts from ranges or short-term patterns. A strong reading in the direction of the breakout adds conviction.
    - **Considerations**: Highly sensitive to noise on very short timeframes. Best used with other intraday tools like VWAP, pivot points, or volume analysis for confirmation. Thresholds for overbought/oversold may need calibration based on asset volatility.
    - **Current Status**: Placeholder. Not yet implemented.

- **`order_flow_imbalance`**
    - **Intended Purpose**: To measure the imbalance between buying and selling pressure using tick-by-tick data and trade direction. This can provide insights into immediate market sentiment and potential short-term price movements, especially when volume-weighted.
    - **Current Status**: Placeholder. Not yet implemented.

- **`intraday_breakout_detector`**
    - **Intended Purpose**: To identify potential intraday breakout patterns from periods of consolidation, confirmed by price action and volume criteria suitable for short timeframes.
    - **Current Status**: Placeholder. Not yet implemented. (Note: `identify_intraday_breakouts` appears to be a similar planned function).

- **`price_velocities`**
    - **Intended Purpose**: To calculate the rate of price change (velocity) across multiple short intraday timeframes, helping to identify accelerating or decelerating momentum quickly.
    - **Current Status**: Placeholder. Not yet implemented.

- **`calculate_pivot_levels`**
    - **Intended Purpose**: To calculate intraday support and resistance levels based on pivot point formulas (using previous session or period OHLC data). Pivot points are commonly used by day traders to anticipate potential turning points or areas of congestion.
    - **Current Status**: Placeholder. Not yet implemented.

- **`calculate_vwap_bands`**
    - **Intended Purpose**: To calculate the Volume Weighted Average Price (VWAP) along with standard deviation bands around it. VWAP is a key intraday benchmark, and the bands can help identify overbought/oversold conditions relative to this volume-weighted average or potential areas for mean reversion.
    - **Current Status**: Placeholder. Not yet implemented.

Detailed descriptions, input parameters, and trading strategies for these day trading indicators will be provided once they are fully implemented in the library.

---

## Short-Term Trading Indicators (Future Implementation)

This section describes indicators and tools optimized for short-term trading strategies, typically with a holding period of days to weeks (e.g., swing trading). The following functions are planned for future development; their descriptions are based on their intended purpose.

- **`swing_strength_index`**
    - **Intended Purpose**: To measure the strength of price swings (up or down) to help identify robust short-term trends or potential exhaustion points for swing reversals.
    - **Trading Strategies & Interpretation (Conceptual)**:
        - *Trend Confirmation*: In an existing short-term trend, when price makes a new swing in the trend direction, a strong reading from the index (positive for uptrends, negative for downtrends) confirms the swing's strength and can be used for entry or adding to positions.
        - *Exhaustion/Reversal (Divergence)*: If price makes a new swing high but the index shows a weaker positive peak than on the previous swing high (bearish divergence), it suggests weakening momentum and a potential reversal. Conversely for a bullish divergence in a downtrend (price new low, index higher low).
        - *Exhaustion (Climax)*: An extremely high positive (or low negative) reading that quickly reverses can signal a climactic end to the current swing, hinting at a potential reversal or significant pullback.
        - *Breakout Validation*: A strong index reading accompanying a price breakout from a multi-day consolidation pattern can validate the breakout's strength.
        - *Filtering Swings*: Use the index to differentiate between strong, tradable swings and weak, choppy price action. Focus on setups where the index indicates clear strength in the intended direction.
    - **Considerations**: The definition of "strong" or "extreme" readings would be relative and potentially asset-specific, requiring observation or calibration. Best used with price patterns, support/resistance, and other indicators.
    - **Current Status**: Placeholder. Not yet implemented.

- **`short_term_regime_detector`**
    - **Intended Purpose**: To identify the current market regime on a short-term basis (e.g., trending bullish, trending bearish, ranging/choppy, high volatility, low volatility). This would help traders adapt their strategies, for example, using trend-following methods in trending regimes and mean-reversion tactics in ranging ones.
    - **Trading Strategies & Interpretation (Conceptual)**:
        - *Bullish Trend Regime*: Employ trend-following strategies (e.g., buying pullbacks to moving averages, bullish breakouts from continuation patterns). Favor long positions.
        - *Bearish Trend Regime*: Employ trend-following strategies (e.g., selling rallies to moving averages, bearish breakouts from continuation patterns). Favor short positions.
        - *Ranging (Sideways) Regime*: Utilize mean-reversion strategies. Buy near identified support, sell near identified resistance within the range. Oscillators (RSI, Stochastic) can help identify overbought/oversold conditions at range boundaries.
        - *High Volatility Regime*: Adjust strategy for wider price swings. Use wider stop-losses (e.g., ATR-based), potentially reduce position size. Breakout strategies might be effective if volatility leads to a new trend. Mean reversion is generally riskier.
        - *Low Volatility (Quiet) Regime*: Look for consolidation patterns (wedges, tight ranges) and prepare for potential breakouts. Place orders to enter on a break of the consolidation. Some tight range-bound strategies might apply with small targets.
    - **Considerations**: The output of the detector should guide strategy selection and parameter tuning (e.g., stop-loss width, profit target size). Regime identification provides context; specific entry/exit signals still require confirmation from price action or other indicators aligned with the regime.
    - **Current Status**: Placeholder. Not yet implemented.

- **`dip_buying_score`**
    - **Intended Purpose**: To create a scoring system that quantifies the attractiveness of buying pullbacks (dips) within an established short-term uptrend. This might involve looking at oversold conditions via oscillators, proximity to support levels, trend strength, retracement depth, volume patterns, and candlestick signals.
    - **Trading Strategies & Interpretation (Conceptual)**:
        - *Entry Qualification*: Use a minimum score threshold (e.g., >70 out of 100) to qualify a dip as a buying opportunity. Only consider dips that meet this quality bar.
        - *Ranking Setups*: When multiple potential dip-buying opportunities exist, use the score to rank them and prioritize capital towards higher-scoring setups.
        - *Confirmation Signal*: A high score can act as a strong confirmation factor when combined with other entry triggers, such as a bullish candlestick pattern at a support level or an oscillator turning up from an oversold reading.
        - *Position Sizing Aid*: Potentially adjust position size based on the score – higher scores might warrant slightly larger positions (within overall risk management rules) due to higher perceived quality of the setup.
        - *Timing Assistance*: While not a precise timing tool itself, a rising score as a dip matures near support can indicate improving conditions for an entry.
    - **Considerations**: The score's components (e.g., trend strength, support proximity, oscillator reading, volume during dip, candlestick patterns) and their weightings would need careful definition and calibration. The primary condition is an already established short-term uptrend. The score helps assess the quality of the pullback within that trend.
    - **Current Status**: Placeholder. Not yet implemented.

- **`multi_day_pattern_detector`**
    - **Intended Purpose**: To automatically identify common multi-day chart patterns such as flags, pennants, triangles (ascending, descending, symmetrical), wedges (rising, falling), rectangles, head and shoulders (and inverse), double/triple tops/bottoms, and cup and handle formations. These patterns often signal continuation or reversal over a short-term horizon (days to weeks) and are crucial for swing trading.
    - **Trading Strategies & Interpretation (Conceptual)**:
        - *Pattern Identification & Classification*: The core function is to alert the trader to the formation and type of a recognized multi-day pattern.
        - *Breakout Entries*:
            - **Continuation Patterns (e.g., Flags, Pennants, Rectangles in trend, Ascending/Symmetrical Triangles in uptrend)**: Enter in the direction of the prior trend upon a decisive price breakout from the pattern's boundary, ideally confirmed by increased volume.
            - **Reversal Patterns (e.g., Head and Shoulders, Double/Triple Tops/Bottoms, Wedges ending a trend)**: Enter in the anticipated new direction when price breaks a key confirmation level (e.g., neckline for H&S, support for Double Top, trendline for Wedge).
        - *Anticipatory Entries*: For well-defined patterns, some traders might enter near a pattern boundary before the breakout, anticipating its direction (e.g., buying near the lower trendline of an ascending triangle in an uptrend). This is higher risk.
        - *Volume Confirmation*: A significant increase in volume on the breakout adds validity to the pattern and the breakout. Decreasing volume during the pattern's formation is typical.
        - *Price Targets (Measured Moves)*:
            - **Flags/Pennants**: Project the height of the initial sharp move (flagpole) from the breakout point.
            - **Triangles/Rectangles**: Project the widest part of the pattern (height) from the breakout point.
            - **Head and Shoulders**: Project the distance from the head to the neckline from the neckline breakout point.
            - **Cup and Handle**: Project the depth of the cup from the handle's breakout point.
        - *Stop-Loss Placement*: Place stop-losses beyond the breakout point on the opposite side, or below/above a key structural point within the pattern (e.g., below the low of a bullish flag, above the right shoulder of a H&S top).
        - *Pattern Failure*: If price breaks out in the opposite direction to what is typically expected from a pattern, it can be a strong signal in that unexpected direction.
        - *Context is Key*: Consider the prevailing longer-term trend. A bullish multi-day pattern is generally more reliable if the longer-term trend is also up.
    - **Considerations**: Pattern recognition can be subjective; clear definition rules for the detector are vital. Patterns perform differently in various market conditions. Confirmation from other indicators (e.g., momentum, moving averages) is highly recommended.
    - **Current Status**: Placeholder. Not yet implemented. (Note: `detect_chart_patterns` appears to be a similar planned function).

- **`calculate_average_range`**
    - **Intended Purpose**: To calculate the average price range (e.g., High minus Low) over a specified number of preceding periods. For example, Average Daily Range (ADR) calculates the average daily H-L over the last N days. This helps gauge typical volatility and potential price movement for a given period.
    - **Trading Strategies & Interpretation (Conceptual)**:
        - *Volatility Assessment*: A higher average range implies higher recent volatility, potentially favoring breakout strategies or wider stops. A lower average range suggests lower volatility, perhaps suiting range-bound approaches or tighter risk management.
        - *Setting Profit Targets*: If an asset's 10-day ADR is 50 pips, and it has already moved 40 pips from its low today, its remaining typical upside for the day might be limited. This can help in setting realistic intraday or short-term profit targets, e.g., near `Day's Low + ADR` for a long trade.
        - *Stop-Loss Placement Idea*: The average range can inform stop-loss distances, giving trades room based on typical price fluctuations (though ATR is more common for this precise purpose due to gap handling).
        - *Breakout/Continuation Potential*: A breakout occurring when only a small fraction of the average range for the period has been covered might suggest more room for the move to develop. Conversely, a breakout after most of the average range is exhausted might have less follow-through potential.
        - *Exhaustion/Mean Reversion Signals*: If price extends significantly beyond its recent average range (e.g., 150-200% of ADR) without a clear catalyst, it might be considered overextended, increasing the likelihood of a pullback or consolidation. This could be a filter for counter-trend trades or caution for trend-following entries.
        - *Trade Filtering*: Useful for selecting assets with sufficient typical movement to achieve profit goals, or for avoiding trades where the stop-loss would need to be excessively large relative to the average range.
        - *Contextual Use*: An expanding average range can indicate increasing momentum. A contracting average range might signal consolidation before a potential breakout.
    - **Considerations**: The lookback period (N) is crucial (e.g., a 5-period average range is more responsive than a 20-period one). The average range is a statistical guide, not a rigid boundary, and prices can exceed it, especially during news. Best used with other analytical tools.
    - **Current Status**: Placeholder. Not yet implemented.

- **`find_swing_points`**
    - **Intended Purpose**: To automatically identify significant swing highs (peaks) and swing lows (troughs) in price action. These points are crucial for trend analysis, support/resistance identification, and pattern recognition in short-term trading.
    - **Trading Strategies & Interpretation (Conceptual)**:
        - *Trend Analysis*:
            - **Uptrend**: A sequence of higher swing highs and higher swing lows.
            - **Downtrend**: A sequence of lower swing highs and lower swing lows.
            - **Range**: Swing highs and lows forming near consistent horizontal levels.
            - A break in the sequence (e.g., a lower high in an uptrend) can signal potential trend weakness or reversal.
        - *Support and Resistance*: Previous swing lows often act as support levels, while previous swing highs act as resistance. The more retests a swing point withstands, the more significant the S/R level.
        - *Trend-Following Entries*:
            - **Long**: In an uptrend, after a pullback forms a new swing low, enter on signs of price resuming upwards, targeting the previous swing high or higher.
            - **Short**: In a downtrend, after a rally forms a new swing high, enter on signs of price resuming downwards, targeting the previous swing low or lower.
        - *Breakout Entries*: Enter on a decisive price break above a major swing high (for longs) or below a major swing low (for shorts), ideally with volume confirmation.
        - *Stop-Loss Placement*: Place stop-losses below a recent significant swing low for long trades, or above a recent significant swing high for short trades.
        - *Profit Targets*: Previous swing points can serve as initial profit targets. For instance, in a long trade initiated after a swing low, the prior swing high is a logical first target.
        - *Chart Pattern Foundation*: Swing points are the core components of classic chart patterns (e.g., head and shoulders, triangles, double tops/bottoms). Identifying them is key to recognizing these patterns.
    - **Considerations**: The sensitivity setting (e.g., number of bars defining a peak/trough) will affect the number and significance of identified swing points. More significant swing points usually form on higher timeframes. Always look for confluence with other indicators or price action signals.
    - **Current Status**: Placeholder. Not yet implemented.

- **`mean_reversion_signals`**
    - **Intended Purpose**: To generate trading signals based on the principle that prices and other metrics (like volatility) tend to revert to their historical average (mean) after significant deviations. This would involve identifying overbought/oversold conditions or statistical extremes.
    - **Trading Strategies & Interpretation (Conceptual)**:
        - *Identifying the Mean*: The system would first need a way to define the 'mean'. This could be a moving average (e.g., 20-period SMA/EMA), the central line of Bollinger Bands, or intraday VWAP.
        - *Detecting Significant Deviations (Overbought/Oversold)*:
            - **Oscillators**: Using RSI (e.g., <30 for oversold, >70 for overbought), Stochastic Oscillator (e.g., <20 for oversold, >80 for overbought).
            - **Volatility Bands**: Price touching or exceeding an outer Bollinger Band (lower for buy, upper for sell) or VWAP standard deviation bands.
            - **Statistical Approach**: Price moving a certain number of standard deviations from its chosen moving average.
        - *Entry Signals*:
            - **Buy (Long) Signal**: When the asset is deemed significantly oversold (e.g., RSI < 30, price at lower Bollinger Band) and potentially shows early signs of turning back up.
            - **Sell (Short) Signal**: When the asset is deemed significantly overbought (e.g., RSI > 70, price at upper Bollinger Band) and potentially shows early signs of turning back down.
        - *Profit Targets*:
            - The identified mean (e.g., price returning to the 20-period MA or middle Bollinger Band).
            - Oscillator returning to a neutral level (e.g., RSI crossing 50).
            - A fixed percentage of the deviation from the mean.
        - *Stop-Loss Strategies*:
            - A price level further beyond the entry if the deviation from the mean continues.
            - A time-based stop if reversion doesn't occur within N periods.
        - *Trend Context*: Mean reversion signals can be traded counter to a larger trend (higher risk) or as pullbacks within a larger trend (e.g., buying an oversold RSI reading during a broader uptrend defined by a long-term moving average).
    - **Considerations**: The effectiveness heavily depends on the correct identification of the mean and deviation thresholds, which can vary per asset and market conditions. Strong trends can lead to sustained overbought/oversold readings, posing risks to mean reversion strategies. Confirmation from price action or other indicators is often beneficial.
    - **Current Status**: Placeholder. Not yet implemented.

Detailed descriptions, input parameters, and trading strategies for these short-term indicators will be provided once they are fully implemented in the library.

---

## Long-Term Investing Indicators (Future Implementation)

This section outlines indicators and analytical tools suited for long-term investing, focusing on identifying major secular trends, market cycles, and valuation metrics over weekly, monthly, or even longer timeframes. The following functions are planned for future development; their descriptions are based on their intended purpose.

- **`secular_trend_strength`**
    - **Intended Purpose**: To identify the strength and duration of long-term secular (multi-year to multi-decade) market trends, often using very long-term moving averages (e.g., 50-week and 200-week MAs, or 100-month and 200-month MAs). This helps in aligning investments with the primary market direction over very extended periods.
    - **Trading Strategies & Interpretation (Conceptual)**:
        - *Primary Secular Trend Identification*:
            - **Secular Bull Market**: Price consistently trades above very long-term MAs (e.g., 200-week MA), with these MAs sloping upwards. Shorter long-term MAs (e.g., 50-week MA) are above longer-term MAs.
            - **Secular Bear Market**: Price consistently trades below very long-term MAs, with these MAs sloping downwards. Shorter long-term MAs are below longer-term MAs.
            - **Secular Sideways Market**: Price oscillates around flat or frequently crossing long-term MAs for many years.
        - *Assessing Trend Strength*:
            - **Strong Trend**: Indicated by a steep, consistent slope in the long-term MAs and clear, sustained separation between them (e.g., 50-week MA significantly above a rising 200-week MA).
            - **Weakening Trend**: Long-term MAs begin to flatten, converge, or price starts to frequently challenge and cross key long-term MAs.
            - **Nascent Trend**: A new, decisive crossover of very long-term MAs after a prolonged period in a different regime signals a potential new secular trend.
        - *Strategic Asset Allocation Guidance*:
            - **Bullish Secular Trend**: Favors a strategic overweight to the asset. Investors might use buy-and-hold strategies or buy significant cyclical dips within the secular uptrend.
            - **Bearish Secular Trend**: Favors a strategic underweight or avoidance for capital appreciation. Focus might shift to capital preservation or alternative assets.
            - **Sideways Secular Trend**: May require more tactical allocation or a focus on income-generating assets if capital appreciation is muted.
        - *Alignment with Investment Horizon*: Most suitable for investors with multi-year to multi-decade outlooks.
        - *Confirmation with Fundamentals*: Secular technical trends are ideally supported by long-term fundamental drivers (e.g., demographic shifts, technological innovation, economic growth trajectories).
        - *Identifying Major Inflection Points*: A confirmed change in direction of very long-term MAs (e.g., 200-week MA turning down after years of rising) can, though lagging, signal a potential major shift in the secular trend, prompting a strategic review.
    - **Considerations**: These indicators are very lagging by nature, confirming trends well underway rather than predicting their inception. Requires extensive historical data. Provides a broad strategic outlook, not for market timing. Demands significant patience from investors.
    - **Current Status**: Placeholder. Not yet implemented.

- **`market_cycle_phase_detector`**
    - **Intended Purpose**: To determine the current phase of a broad market cycle (e.g., accumulation, markup/uptrend, distribution, markdown/downtrend) using long-term price action, volume characteristics, and potentially other indicators. This helps align long-term investment strategies with the market's underlying stage.
    - **Trading Strategies & Interpretation (Conceptual)**:
        - **1. Accumulation Phase**:
            - *Characteristics*: Follows a significant decline. Selling pressure diminishes. "Smart money" begins to buy, forming a price base (sideways consolidation, bottoming patterns like double bottoms or inverse head & shoulders on weekly/monthly charts). Volume may be low initially or show capitulation spikes followed by quiet absorption. Sentiment is generally poor.
            - *Investing Implications*: Ideal for long-term value investors to gradually build positions at potentially undervalued prices. Requires patience, as this phase can be prolonged.
        - **2. Markup (Uptrend) Phase**:
            - *Characteristics*: Price breaks out from the accumulation base, starting a sustained uptrend (higher highs and higher lows on long-term charts). Broader market participation increases; positive news reinforces the trend. Volume ideally confirms the uptrend (higher on rallies, lower on pullbacks).
            - *Investing Implications*: Primary phase for capital appreciation. Hold existing long positions, add on constructive pullbacks to long-term support, or initiate new longs early in this phase.
        - **3. Distribution Phase**:
            - *Characteristics*: Follows a significant markup. Buying momentum wanes; "smart money" takes profits. Price action becomes choppy, forming a topping pattern (sideways range at highs, potential head & shoulders, double/triple tops). Volume may be high with no price progress ("churning") or diminish on rally attempts. Sentiment is often euphoric.
            - *Investing Implications*: Time for increased caution. Consider reducing exposure, taking partial profits, or avoiding new long positions in overextended assets. Risk management is key.
        - **4. Markdown (Downtrend) Phase**:
            - *Characteristics*: Price breaks down from the distribution top, starting a sustained downtrend (lower highs and lower lows). Selling pressure intensifies; negative news prevails. Late buyers may panic sell.
            - *Investing Implications*: Focus on capital preservation. Reduce or exit long positions. Wait for signs of the downtrend ending and a new accumulation phase beginning before re-entering.
    - **Considerations**: Identifying exact phase transitions is often clearer in hindsight and involves some subjectivity. The detector would analyze long-term price patterns, volume, and moving averages. These phases apply to long-term market views (months to years) and help manage investment emotions. (Note: `identify_market_cycles` is a similar planned function).
    - **Current Status**: Placeholder. Not yet implemented.

- **`long_term_valuation_metrics`**
    - **Intended Purpose**: To calculate and analyze key fundamental valuation ratios and potentially intrinsic value estimates. This helps long-term investors assess if an asset is potentially undervalued, fairly valued, or overvalued based on its financial health, earnings power, and growth prospects relative to its current market price.
    - **Trading Strategies & Interpretation (Conceptual)**:
        - *Common Metrics & Their Use*:
            - **Price-to-Earnings (P/E) Ratio (Market Price per Share / EPS)**: Indicates market expectation. Low P/E (vs. history/peers) *may* signal undervaluation; high P/E *may* signal overvaluation or high growth. Context (industry, growth rate, overall market P/E like Shiller P/E) is vital.
            - **Price-to-Book (P/B) Ratio (Market Price per Share / Book Value per Share)**: Compares market value to net asset value. Low P/B (esp. <1) *may* suggest undervaluation, particularly for asset-heavy industries.
            - **Price-to-Sales (P/S) Ratio (Market Cap / Revenue)**: Useful for unprofitable growth companies or cyclical troughs. Low P/S (vs. peers/history) *may* suggest undervaluation if margins are expected to improve.
            - **Dividend Yield (Annual Dividend per Share / Market Price)**: Measures dividend return. High, sustainable yield *may* indicate an undervalued income stock.
            - **Enterprise Value to EBITDA (EV/EBITDA)**: Compares total company value to operating cash flow proxy. Good for cross-company comparisons with different capital structures/tax rates. Lower EV/EBITDA (vs. peers/history) *may* signal undervaluation.
            - **Price/Earnings-to-Growth (PEG) Ratio (P/E / EPS Growth Rate %)**: Adjusts P/E for growth. PEG ~1 *may* be fair value; <1 *may* be undervalued; >1 *may* be overvalued.
            - **Discounted Cash Flow (DCF) Analysis Output**: Provides an intrinsic value estimate. If market price is significantly below DCF value, it *may* suggest undervaluation (margin of safety).
        - *General Investment Approach*:
            - **Comparative Analysis**: Compare current metrics to the company's historical averages, industry peers, and broad market levels.
            - **Seeking Undervaluation**: A confluence of multiple metrics suggesting cheapness relative to fundamentals can highlight long-term buy opportunities.
            - **Assessing Overvaluation**: Consistently high metrics may warrant caution or a review of holdings.
            - **Holistic Assessment**: No single metric is definitive. Use a basket of metrics and consider the qualitative aspects of the business.
    - **Considerations**: Valuation is contextual. Industry norms, growth rates, profitability, debt, management quality, competitive advantages, and overall economic conditions significantly impact interpretation. Beware of "value traps" (cheap for a reason). These metrics are for long-term value assessment, not short-term timing.
    - **Current Status**: Placeholder. Not yet implemented.

- **`long_term_divergence_detector`**
    - **Intended Purpose**: To identify significant and prolonged divergences between an asset's price trend and the trend of key technical indicators (e.g., momentum oscillators like RSI or MACD; volume-based indicators like OBV) on long-term charts (e.g., weekly, monthly). Such divergences can serve as early warnings of potential major trend reversals or a significant loss of momentum in the existing long-term trend.
    - **Trading Strategies & Interpretation (Conceptual)**:
        - *Types of Long-Term Divergence*:
            - **Bullish Divergence (Long-Term)**: Price records lower lows over months/years (on weekly/monthly charts), while the chosen indicator forms higher lows. Suggests waning selling momentum.
            - **Bearish Divergence (Long-Term)**: Price records higher highs over months/years, while the indicator forms lower highs. Suggests waning buying momentum.
        - *Strategic Implications for Long-Term Investors*:
            - **Early Warning System**: Divergences are primarily alerts, not immediate action signals. They suggest the underlying strength of the current long-term trend may be changing.
            - **Interpreting Long-Term Bullish Divergence**: Signals potential exhaustion of a major downtrend. Prompts investors to look for fundamental signs of recovery and confirming price action (e.g., basing patterns, break of long-term downtrend line) before considering accumulation for a long-term hold.
            - **Interpreting Long-Term Bearish Divergence**: Signals potential weakening of a major uptrend. Advises caution, review of highly appreciated positions, consideration of partial profit-taking, or tightening risk management. Suggests prudence before adding new capital if divergence is strong.
        - *Confirmation is Crucial*:
            - Long-term divergences are more reliable when confirmed by price action (e.g., break of a multi-year trendline, completion of a major reversal chart pattern on weekly/monthly charts, decisive crossover of long-term moving averages).
        - *Strength of Divergence Signal*:
            - **Duration**: Longer-forming divergences (many months to years) are typically more significant.
            - **Multiple Swings**: Divergences involving multiple price swings (e.g., "triple divergence") are often considered more robust.
            - **Indicator Extremes**: Divergences forming when an indicator is at historical overbought/oversold levels on long-term charts can be more potent.
    - **Considerations**: Divergences can give false signals or persist for extended periods in strong secular trends. The choice of indicator and its settings matter. These are slow-moving signals requiring patience and should be used in conjunction with fundamental analysis and market cycle assessment. (Note: `find_price_indicator_divergence` is a similar planned function).
    - **Current Status**: Placeholder. Not yet implemented.

- **`long_term_support_resistance`**
    - **Intended Purpose**: To identify major, historically significant support and resistance price zones on long-term charts (e.g., weekly, monthly). These levels often represent critical psychological price points where sustained buying or selling pressure has emerged over months, years, or even decades, frequently marking major trend continuations or reversals.
    - **Trading Strategies & Interpretation (Conceptual)**:
        - *Identification*: Derived from multi-year/decade historical swing highs/lows, major consolidation areas, significant long-term trendlines, or key Fibonacci levels of secular moves.
        - *Strategic Framework*: These levels provide a "big picture" structural map for an asset, highlighting historically critical price areas.
        - *Potential Long-Term Entry Zones (Near Major Support)*: When price on weekly/monthly charts approaches a multi-year support zone, it may offer a favorable risk/reward area to initiate or add to long-term core positions. Look for confirming bullish signals (e.g., long-term basing patterns, bullish divergences, positive fundamental shifts).
        - *Potential Long-Term Exit/Risk Management Zones (Near Major Resistance)*: When price approaches a multi-year resistance zone, consider reviewing long positions, possibly taking partial profits, or avoiding new allocations. Look for confirming bearish signals.
        - *Principle of Polarity (Role Reversal)*: A decisively broken major long-term resistance often becomes new long-term support, and a broken major support can become new resistance.
        - *Decisive Breaks as Major Signals*: A confirmed break *below* a critical multi-year support is a very bearish long-term signal. A confirmed break *above* a critical multi-year resistance is a very bullish long-term signal, often indicating a new secular advance.
    - **Considerations**: The strength of a level increases with more tests over longer periods and higher historical volume. Treat these as zones, not exact lines. Most relevant for investors with multi-year to decade-long horizons. Always assess in conjunction with evolving fundamentals, as drastic changes can invalidate historical levels. (Note: `identify_support_resistance` is a similar planned function).
    - **Current Status**: Placeholder. Not yet implemented.

- **`detect_trend_change`**
    - **Intended Purpose**: To signal potential changes in the long-term trend, typically using crossovers of long-period moving averages or breaks of significant long-term trendlines.
    - **Current Status**: Placeholder. Not yet implemented.

- **`fundamental_trend_correlation`**
    - **Intended Purpose**: To analyze and quantify the correlation between an asset's long-term price trends and changes in its underlying fundamental data (e.g., earnings growth, revenue trends). This can help validate price trends or identify discrepancies.
    - **Current Status**: Placeholder. Not yet implemented.

Detailed descriptions, input parameters, and investing strategies for these long-term indicators will be provided once they are fully implemented in the library.

---

## Conclusion

This guide provides a comprehensive overview of the technical indicators available in RusTaLib, along with common trading strategies and interpretations for both currently implemented features and planned future developments. Remember that no single indicator provides a complete picture, and successful trading often involves combining insights from multiple indicators, price action analysis, and robust risk management practices. As RusTaLib evolves, this guide will be updated to reflect new functionalities and enhanced indicator capabilities. We encourage users to explore these tools, understand their nuances, and integrate them thoughtfully into their trading or investment decision-making processes.