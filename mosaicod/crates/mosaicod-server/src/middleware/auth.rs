use crate::error::{PublicErrorGrpcExt, Result};
use mosaicod_core::{self as core, types};
use mosaicod_facade as facade;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tower::{Layer, Service};

// Skeleton from: https://github.com/hyperium/tonic/blob/master/examples/src/tower/server.rs

const MOSAICO_API_KEY_TOKEN: &str = "mosaico-api-key-token";

/// Context used to pass auth data
#[derive(Clone)]
pub struct AuthContext {
    permissions: types::auth::Permission,
}

impl AuthContext {
    pub fn permissions(&self) -> &types::auth::Permission {
        &self.permissions
    }
}

#[derive(Clone)]
pub struct AuthLayer {
    context: facade::Context,

    /// If permissions passthrough is enabled no auth check is performed
    /// and a fake permission token with all permission is
    /// generated for every request.
    permissions_passthrough: Option<types::auth::Permission>,
}

impl AuthLayer {
    pub fn new(context: facade::Context) -> Self {
        Self {
            context,
            permissions_passthrough: None,
        }
    }

    /// Enable auth passthrough. No internal check is
    /// performed to validate api keys and a fake permissions
    /// are generated to perform every action.
    pub fn with_permission_passthrough(mut self, permissions: types::auth::Permission) -> Self {
        self.permissions_passthrough = Some(permissions);
        self
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        AuthMiddleware {
            inner: service,
            context: self.context.clone(),
            permissions_passthrough: self.permissions_passthrough,
        }
    }
}

#[derive(Clone)]
pub struct AuthMiddleware<S> {
    inner: S,
    context: facade::Context,
    permissions_passthrough: Option<types::auth::Permission>,
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

            let context = self.context.clone();

            Box::pin(async move {
                let auth_ctx_result: Result<AuthContext> = async {
                    if token.is_empty() {
                        Err(core::Error::missing_api_key())?
                    }

                    let token: types::auth::Token = token.parse()?;

                    let handle =
                        facade::auth::Handle::try_from_fingerprint(&context, token.fingerprint())
                            .await
                            .map_err(|e| match e.error().kind() {
                                core::error::ErrorKind::NotFound(_) => {
                                    core::Error::unauthorized("API key does not exist.".to_string())
                                }
                                _ => e.error(),
                            })?;

                    if handle.api_key().is_expired() {
                        Err(core::Error::unauthorized("API key is expired.".to_string()))?;
                    }

                    Ok(AuthContext {
                        permissions: handle.api_key().permission,
                    })
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
