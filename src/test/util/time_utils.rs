use crate::util::time_utils::*;

#[test]
fn test_parse_valid_date() {
    let date = parse_date("2023-01-01").unwrap();
    assert_eq!(date.year(), 2023);
    assert_eq!(date.month(), 1);
    assert_eq!(date.day(), 1);
}

#[test]
fn test_parse_invalid_date() {
    let result = parse_date("not-a-date");
    assert!(result.is_err());
}

#[test]
fn test_format_date() {
    let date = parse_date("2023-01-01").unwrap();
    let formatted = format_date(&date);
    assert_eq!(formatted, "2023-01-01");
}

#[test]
fn test_edge_dates() {
    let date = parse_date("1970-01-01").unwrap();
    assert_eq!(format_date(&date), "1970-01-01");
    let date = parse_date("9999-12-31").unwrap();
    assert_eq!(format_date(&date), "9999-12-31");
} 