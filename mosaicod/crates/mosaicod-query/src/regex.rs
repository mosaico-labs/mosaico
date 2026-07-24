//! Globbing to Regex conversion methods

use regex::Regex;

const MAX_REGEX_PATTERN_LENGTH: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("regex empty pattern")]
    EmptyPattern,
    #[error("regex pattern too long. Max is {}", MAX_REGEX_PATTERN_LENGTH)]
    PatternTooLong,
    #[error("regex parsing error: {0}")]
    MalformedPattern(#[from] regex::Error),
}

/// Converts a wildcard/glob pattern into an anchored [`Regex`].
///
/// Supported wildcards:
/// - `*`  — zero or more of any character (`.*`)
/// - `?`  — exactly one of any character (`.`)
/// - `#`  — exactly one digit (`[0-9]`)
/// - `[…]` — character class passed through verbatim
///
/// All regex meta-characters (`.`, `+`, `^`, `$`, etc.) are escaped so they
/// match literally. The resulting regex is anchored at both ends (`^…$`), so
/// the pattern must match the entire input string.
///
/// Returns [`Error::EmptyPattern`] for an empty input and
/// [`Error::PatternTooLong`] when the pattern exceeds [`MAX_REGEX_PATTERN_LENGTH`].
pub fn wildcard_to_posix_regex(pattern: &str) -> Result<Regex, Error> {
    if pattern.is_empty() {
        return Err(Error::EmptyPattern);
    }

    if pattern.len() > MAX_REGEX_PATTERN_LENGTH {
        return Err(Error::PatternTooLong);
    }

    let mut regex = String::with_capacity(pattern.len() * 2);

    // Anchor the match to the start of the string
    regex.push('^');

    let mut chars = pattern.chars();

    while let Some(c) = chars.next() {
        match c {
            // '*' matches zero or more of any character
            '*' => {
                // Only push ".*" if the regex doesn't already end with ".*"
                if !regex.ends_with(".*") {
                    regex.push_str(".*");
                }
            }

            // '?' matches exactly one of any character
            '?' => regex.push('.'),

            // '#' matches any single digit
            '#' => regex.push_str("[0-9]"),

            // Handle character ranges like [a-z]
            '[' => {
                regex.push('[');

                for inside_c in chars.by_ref() {
                    regex.push(inside_c);
                    if inside_c == ']' {
                        break;
                    }
                }
            }

            // Escape standard regex characters so they are treated as literals
            '.' | '+' | '^' | '$' | '(' | ')' | '{' | '}' | '|' | '\\' => {
                regex.push('\\');
                regex.push(c);
            }

            // All other characters are treated as normal literals
            _ => regex.push(c),
        }
    }

    // Anchor the match to the end of the string
    regex.push('$');

    Ok(Regex::new(&regex)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper macro to assert successful conversions
    macro_rules! assert_translation {
        ($pattern:expr, $expected:expr) => {
            assert_eq!(
                wildcard_to_posix_regex($pattern).unwrap().to_string(),
                $expected
            );
        };
    }

    // =========================================================================
    // 1. HAPPY PATHS (Standard Wildcard Translation)
    // =========================================================================

    #[test]
    fn test_exact_match_no_wildcards() {
        assert_translation!("hello", "^hello$");
    }

    #[test]
    fn test_asterisk_wildcard() {
        assert_translation!("file*.txt", "^file.*\\.txt$");
    }

    #[test]
    fn test_question_mark_wildcard() {
        assert_translation!("image?.jpg", "^image.\\.jpg$");
    }

    #[test]
    fn test_digit_wildcard() {
        assert_translation!("id_##", "^id_[0-9][0-9]$");
    }

    #[test]
    fn test_character_range_wildcard() {
        assert_translation!("log_[a-z].txt", "^log_[a-z]\\.txt$");
    }

    #[test]
    fn test_combined_wildcards() {
        assert_translation!("user_#_?_*.json", "^user_[0-9]_._.*\\.json$");
    }

    // =========================================================================
    // 2. EDGE CASES & ESCAPING
    // =========================================================================

    #[test]
    fn test_regex_meta_characters_are_escaped() {
        // Characters like ., +, $, ^ should be treated as literal strings
        assert_translation!("v1.0+build^$|", "^v1\\.0\\+build\\^\\$\\|$");
    }

    #[test]
    fn test_wildcards_inside_brackets_are_treated_as_literals() {
        // Inside brackets, our logic passes them through as literals
        assert_translation!("[%-*]", "^[%-*]$");
    }

    #[test]
    fn test_single_asterisk_wildcard() {
        assert_translation!("*", "^.*$");
    }

    // =========================================================================
    // 3. SANITIZATION & OPTIMIZATION
    // =========================================================================

    #[test]
    fn test_collapsing_consecutive_asterisks() {
        // Multiple consecutive asterisks should safely collapse down to a single '.*'
        assert_translation!("file*****.txt", "^file.*\\.txt$");
        assert_translation!("***a***b***", "^.*a.*b.*$");
    }

    // =========================================================================
    // 4. VALIDATION & SECURITY ERRORS
    // =========================================================================

    #[test]
    fn test_empty_pattern_error() {
        assert!(matches!(
            wildcard_to_posix_regex(""),
            Err(Error::EmptyPattern)
        ));
    }

    #[test]
    fn test_malformed_bracket_range_validation() {
        // Checking if the regex parser catches an out-of-order range compilation error
        let result = wildcard_to_posix_regex("file_[z-a].txt");
        assert!(matches!(
            result,
            Err(Error::MalformedPattern(regex::Error::Syntax(_)))
        ));
    }

    #[test]
    fn test_malformed_bracket_range_validation_2() {
        // Inside brackets, our logic passes them through as literals, but the ASCII order is not correct.
        let result = wildcard_to_posix_regex("[a-*z]");
        assert!(matches!(
            result,
            Err(Error::MalformedPattern(regex::Error::Syntax(_)))
        ));
    }
}
