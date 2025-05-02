// Technical indicators for financial analysis
//
// This library provides common technical indicators used in financial analysis
// such as moving averages, RSI, MACD, Bollinger Bands, etc.

pub mod indicators;
pub mod util;

// Re-export commonly used items
pub use indicators::*;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
