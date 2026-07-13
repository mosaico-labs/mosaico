use async_trait::async_trait;
use mosaicod_core::types::auth::{Permissions, Token};
use mosaicod_facade as facade;
use mosaicod_marshal::ActionRequest;
use mosaicod_plugin::auth::{AuthError, AuthPlugin};

// Same global allocator as the host (bin/mosaicod). Box/Arc/String cross the FFI
// boundary, so both sides MUST allocate through the same allocator, otherwise
// freeing memory allocated on the other side is UB.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Built-in auth provider. The `Context` (which owns the DB pool) is injected by
/// the host through `init` before any request is served.
#[derive(Default)]
struct DefaultAuthPlugin {
    ctx: Option<facade::Context>,
}

#[async_trait]
impl AuthPlugin for DefaultAuthPlugin {
    async fn init(&mut self, ctx: facade::Context) -> Result<(), String> {
        self.ctx = Some(ctx);
        Ok(())
    }

    async fn verify_token(&self, token: &str) -> Result<Permissions, AuthError> {
        // Not initialized: cannot verify anything.
        let ctx = self.ctx.as_ref().ok_or(AuthError::Denied)?;

        // A token we cannot parse is malformed, not merely rejected.
        let token = token.parse::<Token>().map_err(|_| AuthError::Malformed)?;

        // Unknown key (or any DB error) counts as a rejection.
        let handle = facade::auth::Handle::try_from_fingerprint(ctx, token.fingerprint())
            .await
            .map_err(|_| AuthError::Denied)?;

        if handle.api_key().is_expired() {
            return Err(AuthError::Denied);
        }

        Ok(handle.api_key().permission)
    }

    fn has_permission(&self, action: &ActionRequest, perms: &Permissions) -> bool {
        match action {
            ActionRequest::SequenceCreate(_) => perms.can_write(),
            ActionRequest::SequenceNotificationCreate(_) => perms.can_write(),
            ActionRequest::TopicCreate(_) => perms.can_write(),
            ActionRequest::TopicNotificationCreate(_) => perms.can_write(),
            ActionRequest::SessionCreate(_) => perms.can_write(),
            ActionRequest::SessionFinalize(_) => perms.can_write(),

            ActionRequest::SequenceDelete(_) => perms.can_delete(),
            ActionRequest::SequenceNotificationPurge(_) => perms.can_delete(),
            ActionRequest::TopicDelete(_) => perms.can_delete(),
            ActionRequest::TopicNotificationPurge(_) => perms.can_delete(),
            ActionRequest::SessionDelete(_) => perms.can_delete(),

            ActionRequest::Query(_) => perms.can_read(),
            ActionRequest::SequenceNotificationList(_) => perms.can_read(),
            ActionRequest::TopicNotificationList(_) => perms.can_read(),
            ActionRequest::TopicFilterClusterize(_) => perms.can_read(),
            ActionRequest::TopicFilterIntersect(_) => perms.can_read(),

            ActionRequest::Version(_) => true,
        }
    }
}

fn register(reg: &mut dyn mosaicod_plugin::PluginRegistrar) {
    reg.register_auth(Box::new(DefaultAuthPlugin::default()));
}

mosaicod_plugin::export_plugin!(register);
