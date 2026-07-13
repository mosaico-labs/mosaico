use crate::error::{PublicErrorGrpcExt, Result};
use mosaicod_core::{self as core, types};
use mosaicod_plugin::auth::{AuthError, AuthPlugin};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tower::{Layer, Service};

// Skeleton from: https://github.com/hyperium/tonic/blob/master/examples/src/tower/server.rs

const MOSAICO_API_KEY_TOKEN: &str = "mosaico-api-key-token";

/// Context used to pass auth data
#[derive(Clone)]
pub struct AuthContext {
    permissions: types::auth::Permissions,
}

impl AuthContext {
    pub fn permissions(&self) -> &types::auth::Permissions {
        &self.permissions
    }
}

#[derive(Clone)]
pub struct AuthLayer {
    /// Plugin used to verify api key. `None` is passthrough mode.
    auth: Option<Arc<dyn AuthPlugin>>,

    /// If permissions passthrough is enabled no auth check is performed
    /// and the given permissions are grated to every question.
    permissions_passthrough: Option<types::auth::Permissions>,
}

impl AuthLayer {
    pub fn new(auth: Arc<dyn AuthPlugin>) -> Self {
        Self {
            auth: Some(auth),
            permissions_passthrough: None,
        }
    }

    /// Enable auth passthrough. No internal check is
    /// performed to validate api keys and a fake permissions
    /// are generated to perform every action.
    pub fn passthrough(permissions: types::auth::Permissions) -> Self {
        Self {
            auth: None,
            permissions_passthrough: Some(permissions),
        }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        AuthMiddleware {
            inner: service,
            auth: self.auth.clone(),
            permissions_passthrough: self.permissions_passthrough,
        }
    }
}

#[derive(Clone)]
pub struct AuthMiddleware<S> {
    inner: S,
    auth: Option<Arc<dyn AuthPlugin>>,
    permissions_passthrough: Option<types::auth::Permissions>,
}

type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for AuthMiddleware<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Default,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, std::result::Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        // See: https://docs.rs/tower/latest/tower/trait.Service.html#be-careful-when-cloning-inner-services
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        if let Some(permissions) = self.permissions_passthrough {
            // Inject permissions to bypass api key management
            Box::pin(async move {
                req.extensions_mut().insert(AuthContext { permissions });
                let response = inner.call(req).await?;
                Ok(response)
            })
        } else {
            let token = req
                .headers()
                .get(MOSAICO_API_KEY_TOKEN)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();

            let auth = self.auth.clone();

            Box::pin(async move {
                let auth_ctx_result: Result<AuthContext> = async {
                    if token.is_empty() {
                        return Err(core::Error::missing_api_key().into());
                    }

                    match auth.as_ref() {
                        // The plugin owns the credential format and decides the
                        // outcome; we only map it to a response.
                        Some(auth) => match auth.verify_token(&token).await {
                            Ok(permissions) => Ok(AuthContext { permissions }),
                            Err(AuthError::Denied) => {
                                Err(core::Error::unauthorized("invalid API key.".to_string())
                                    .into())
                            }
                            Err(AuthError::Malformed) => {
                                Err(core::Error::bad_request("malformed API key.".to_string())
                                    .into())
                            }
                        },
                        // Not passthrough and no plugin loaded: nobody can be
                        // authenticated. The daemon refuses to start in this
                        // state, so this is only a defensive fallback.
                        None => Err(core::Error::unauthorized(
                            "authentication is not configured.".to_string(),
                        )
                        .into()),
                    }
                }
                .await;

                match auth_ctx_result {
                    Ok(auth_ctx) => {
                        req.extensions_mut().insert(auth_ctx);
                        let response = inner.call(req).await?;
                        Ok(response)
                    }
                    Err(err) => {
                        // Here we are calling .to_status() and not .log_to_status()
                        // in order to avoid logging every unauthenticated request
                        Ok(err.to_status().into_http())
                    }
                }
            })
        }
    }
}

pub fn auth_context<T>(req: &tonic::Request<T>) -> Result<AuthContext> {
    req.extensions()
        .get::<AuthContext>()
        .cloned()
        .ok_or_else(|| core::Error::unauthenticated().into())
}
