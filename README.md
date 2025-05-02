# Rust Polars Technical Indicators
High-performance technical analysis indicator calculation library for stock market data, built with Rust and leveraging the power of the [Polars DataFrame library](https://www.pola.rs/).

## Overview

This library provides efficient Rust implementations of common financial technical indicators. It takes Polars DataFrames as input (expected to contain OHLCV data) and appends new columns containing the calculated indicator values.

The primary goals are:
* **Performance:** Leverage Rust's speed and Polars' highly optimized backend (Apache Arrow) for fast computations on large datasets.
* **Ergonomics:** Provide a simple and idiomatic Rust API for calculating indicators.
* **Accuracy:** Ensure calculations are correct according to standard indicator definitions.
* **Integration:** Easily integrate into larger Rust-based financial analysis pipelines or applications.

## Features

Currently implemented indicators:

* **Simple Moving Average (SMA)**
* **Exponential Moving Average (EMA)**
* **Moving Average Convergence Divergence (MACD)** (Including signal line and histogram)
* **Relative Strength Index (RSI)**
* **Bollinger Bands** (Upper band, middle band (SMA), lower band)
* **Stochastic Oscillator** (%K and %D)

## Technology Stack

* **[Rust](https://www.rust-lang.org/)** (2021 Edition or later)
* **[Polars](https://github.com/pola-rs/polars)**: Blazingly fast DataFrame library for Rust (and Python). Used for data representation and manipulation.
* **[chrono](https://crates.io/crates/chrono)** (Optional, but likely needed for date handling if not implicitly handled by Polars)

## Installation
