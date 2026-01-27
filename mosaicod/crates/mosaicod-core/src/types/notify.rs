pub enum NotifyType {
    Error,
}

impl std::fmt::Display for NotifyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Error => write!(f, "error"),
        }
    }
}

impl std::str::FromStr for NotifyType {
    type Err = std::io::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "error" => Ok(Self::Error),
            _ => Err(std::io::Error::other(format!(
                "unkwnown notify type `{}`",
                value
            ))),
        }
    }
}

pub struct Notify {
    pub id: i32,
    pub target: Box<dyn super::Resource>,
    pub notify_type: NotifyType,
    pub msg: Option<String>,
    pub created_at: super::DateTime,
}

impl Notify {
    pub fn new(ntype: NotifyType, target: Box<dyn super::Resource>, msg: Option<String>) -> Self {
        Self {
            id: -1,
            notify_type: ntype,
            target,
            msg,
            created_at: super::DateTime::now(),
        }
    }
}
