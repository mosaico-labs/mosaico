use rand::{Rng, distr::Alphabetic};

/// Generates a random string of a given `length`
pub fn random_string(length: usize) -> String {
    // ensure that length is a positive number
    assert!(length > 0);

    let mut rng = rand::rng();
    (0..length)
        .map(move |_| rng.sample(Alphabetic) as char)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::panic;

    #[test]
    fn random_string() {
        // Check that requested length
        let s10 = super::random_string(10);
        assert_eq!(s10.len(), 10);

        let s1 = super::random_string(1);
        assert_eq!(s1.len(), 1);

        // If providing a 0 length the function needs to panic
        let result = panic::catch_unwind(|| {
            super::random_string(0);
        });
        assert!(result.is_err());
    }
}
