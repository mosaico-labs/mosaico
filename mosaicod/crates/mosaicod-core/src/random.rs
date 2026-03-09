use rand::{Rng, distr::Alphabetic, distr::Alphanumeric};

/// Generates a random string containing alphabetic chars of a given `length`
pub fn alphabetic(length: usize) -> String {
    assert!(length > 0);

    let mut rng = rand::rng();
    (0..length)
        .map(move |_| rng.sample(Alphabetic) as char)
        .collect()
}

/// Generates a random string containing alphanumeric chars, of a given `length`
pub fn alphanumeric(length: usize) -> String {
    assert!(length > 0);

    let mut rng = rand::rng();
    (0..length)
        .map(move |_| rng.sample(Alphanumeric) as char)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::panic;

    #[test]
    fn random_string() {
        // Check that requested length
        let s10 = super::alphabetic(10);
        assert_eq!(s10.len(), 10);

        let s1 = super::alphabetic(1);
        assert_eq!(s1.len(), 1);

        // If providing a 0 length the function needs to panic
        let result = panic::catch_unwind(|| {
            super::alphabetic(0);
        });
        assert!(result.is_err());
    }
}
