// Stats indicators module

mod beta;
// Uncomment as you add more indicators
// mod correl;
// mod linearreg;
// mod linearreg_slope;
// mod linearreg_intercept;
// mod linearreg_angle;
// mod stddev;
// mod var;
// mod tsf;

// Re-export indicators
pub use beta::calculate_beta;
// Uncomment as you add more indicators
// pub use correl::calculate_correl;
// pub use linearreg::calculate_linearreg;
// pub use linearreg_slope::calculate_linearreg_slope;
// pub use linearreg_intercept::calculate_linearreg_intercept;
// pub use linearreg_angle::calculate_linearreg_angle;
// pub use stddev::calculate_stddev;
// pub use var::calculate_var;
// pub use tsf::calculate_tsf;
