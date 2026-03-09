#[derive(Debug)]
pub struct ErrorReport {
    header: String,
    pub errors: Vec<ErrorReportItem>,
}

impl ErrorReport {
    pub fn new(header: String) -> Self {
        Self {
            header,
            errors: Vec::new(),
        }
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

impl From<ErrorReport> for String {
    fn from(error: ErrorReport) -> Self {
        let errors: Vec<String> = error.errors.into_iter().map(Into::into).collect();
        format!("{}\n\n{}", error.header, errors.join("\n"))
    }
}

#[derive(Debug)]
pub struct ErrorReportItem {
    target: String,
    error: String,
}

impl From<ErrorReportItem> for String {
    fn from(error: ErrorReportItem) -> Self {
        format!("* {} - {}", error.target, error.error)
    }
}

impl ErrorReportItem {
    pub fn new(target: impl Into<String>, error: impl std::error::Error) -> Self {
        Self {
            target: target.into(),
            error: error.to_string(),
        }
    }
}
