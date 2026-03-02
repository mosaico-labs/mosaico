use super::*;

pub enum NotificationType {
    Error,
}

impl std::fmt::Display for NotificationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Error => write!(f, "error"),
        }
    }
}

impl std::str::FromStr for NotificationType {
    type Err = std::io::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "error" => Ok(Self::Error),
            _ => Err(std::io::Error::other(format!(
                "unknown notification type `{}`",
                value
            ))),
        }
    }
}

pub struct Notification {
    pub uuid: Uuid,
    pub target: Box<dyn Resource>,
    pub notification_type: NotificationType,
    pub msg: Option<String>,
    pub created_at: DateTime,
}
