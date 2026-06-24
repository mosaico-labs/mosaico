//! Globbing to Regex conversion methods

use regex::{Error, Regex};

pub fn wildcard_to_posix_regex(pattern: &str) -> Result<Regex, Error> {
    let mut regex = String::with_capacity(pattern.len() * 2);

    // Anchor the match to the start of the string
    regex.push('^');

    let mut chars = pattern.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            // '*' matches zero or more of any character
            '*' => regex.push_str(".*"),

            // '?' matches exactly one of any character
            '?' => regex.push('.'),

            // '#' matches any single digit
            '#' => regex.push_str("[0-9]"),

            // Handle character ranges like [a-z]
            '[' => {
                regex.push('[');
                while let Some(inside_c) = chars.next() {
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

    Regex::new(&regex)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper macro to assert successful conversions
    macro_rules! assert_translation {
        ($pattern:expr, $expected:expr) => {
            assert_eq!(wildcard_to_posix_regex($pattern), Ok($expected.to_string()));
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
        assert_translation!("[a-*z]", "^[a-*z]$");
    }

    // =========================================================================
    // 3. SANITIZATION & OPTIMIZATION
    // =========================================================================

    #[test]
    fn test_collapsing_consecutive_asterisks() {
        // Multiple consecutive stars should safely collapse down to a single '.*'
        assert_translation!("file*****.txt", "^file.*\\.txt$");
        assert_translation!("***a***b***", "^.*a.*b.*$");
    }

    // =========================================================================
    // 4. VALIDATION & SECURITY ERRORS
    // =========================================================================

    #[test]
    fn test_empty_pattern_error() {
        assert_eq!(
            wildcard_to_posix_regex(""),
            Err(ValidationError::EmptyPattern)
        );
    }

    #[test]
    fn test_too_many_wildcards_flood_protection() {
        // Triggering the threshold of > 5 high-density wildcards
        assert_eq!(
            wildcard_to_posix_regex("*a*b*c*d*e*f"),
            Err(ValidationError::TooManyWildcards)
        );
    }

    #[test]
    fn test_malformed_bracket_range_validation() {
        // Checking if the regex parser catches an out-of-order range compilation error
        let result = wildcard_to_posix_regex("file_[z-a].txt");
        assert!(matches!(
            result,
            Err(ValidationError::InvalidRegexSyntax(_))
        ));
    }
}
