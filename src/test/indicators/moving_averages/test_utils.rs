use polars::prelude::*;

pub fn create_test_df() -> DataFrame {
    let price_data = Series::new("price".into(), &[10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0]);
    DataFrame::new(vec![price_data.into()]).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_create_test_df() {
        let df = create_test_df();
        assert_eq!(df.height(), 7);
        assert_eq!(df.width(), 1);
        let price = df.column("price").unwrap().f64().unwrap();
        assert_eq!(price.get(0).unwrap(), 10.0);
        assert_eq!(price.get(6).unwrap(), 16.0);
    }
} 