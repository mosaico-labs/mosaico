use tokio_util::sync::CancellationToken;

/// To stop the server use the following command on
/// `ShutdownNotifier`
#[derive(Clone, Default, Debug)]
pub struct ShutdownNotifier(CancellationToken);

impl ShutdownNotifier {
    // Notifies the server to be shutdown
    pub fn shutdown(&self) {
        self.0.cancel();
    }

    pub async fn wait_for_shutdown(&self) {
        self.0.cancelled().await;
    }

    pub fn is_shutdown(&self) -> bool {
        self.0.is_cancelled()
    }

    /// Returns a cloned cancellation token for use across crate boundaries
    /// (e.g. passing to background tasks defined in other crates).
    pub fn token(&self) -> CancellationToken {
        self.0.clone()
    }
}
