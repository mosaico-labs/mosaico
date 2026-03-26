use crate::ServerError;
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_facade as facade;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tower::{Layer, Service};

// Skeleton from: https://github.com/hyperium/tonic/blob/master/examples/src/tower/server.rs

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
    db: db::Database,

    /// If permissions passthrough is enabled no auth check is performed
    /// and a fake permission token with all permission is
    /// generated for every request.
    permissions_passthrough: Option<types::auth::Permission>,
}

impl AuthLayer {
    pub fn new(db: db::Database) -> Self {
        Self {
            db,
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
            db: self.db.clone(),
            permissions_passthrough: self.permissions_passthrough,
        }
    }
}

#[derive(Clone)]
pub struct AuthMiddleware<S> {
    inner: S,
    db: db::Database,
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
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
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
                .get("mosaico-api-key-token")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();

            let db = self.db.clone();

            Box::pin(async move {
                if token.is_empty() {
                    return Ok(to_http_error(ServerError::MissingApiKeyToken));
                }

                let token: Result<types::auth::Token, types::auth::ApiKeyError> = token.parse();
                match token {
                    Ok(token) => {
                        let fauth =
                            match facade::Auth::try_from_fingerprint(token.fingerprint(), db).await
                            {
                                Ok(fauth) => fauth,
                                Err(_) => {
                                    return Ok(to_http_error(ServerError::Unauthorized));
                                }
                            };

                        req.extensions_mut().insert(AuthContext {
                            permissions: fauth.into_api_key().permission,
                        });

                        let response = inner.call(req).await?;

                        Ok(response)
                    }
                    Err(_) => Ok(to_http_error(ServerError::Unauthorized)),
                }
            })
        }
    }
}

fn to_http_error<ResBody>(err: ServerError) -> http::Response<ResBody>
where
    ResBody: Default,
{
    tracing::error!("{}", err.unroll());
    let status: tonic::Status = err.into();
    status.into_http()
}
