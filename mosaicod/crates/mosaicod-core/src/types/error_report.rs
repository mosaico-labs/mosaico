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

impl Into<String> for ErrorReport {
    fn into(self) -> String {
        let errors: Vec<String> = self.errors.into_iter().map(Into::into).collect();
        format!("{}\n\n{}", self.header, errors.join("\n"))
    }
}

#[derive(Debug)]
pub struct ErrorReportItem {
    target: String,
    error: String,
}

impl ErrorReportItem {
    pub fn new(target: impl Into<String>, error: impl std::error::Error) -> Self {
        Self {
            target: target.into(),
            error: error.to_string(),
        }
    }
}

impl Into<String> for ErrorReportItem {
    fn into(self) -> String {
        format!("* {} - {}", self.target, self.error)
    }
}
