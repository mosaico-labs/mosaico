//! A TCP proxy that can simulate a network outage, e.g. between mosaicod and PostgreSQL.

use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::common::HOST;

/// Forwards every connection to an upstream address, until [`TcpProxy::cut`] severs open
/// connections and drops new ones, as an unreachable upstream would. [`TcpProxy::restore`]
/// forwards new connections again.
pub struct TcpProxy {
    port: u16,
    up: watch::Sender<bool>,
    accept_loop: JoinHandle<()>,
}

impl TcpProxy {
    pub async fn start(upstream: (String, u16)) -> Self {
        let listener = TcpListener::bind(SocketAddr::new(HOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let up = watch::Sender::new(true);
        let up_rx = up.subscribe();

        let accept_loop = tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    continue;
                };

                // Dropping the connection closes it.
                if !*up_rx.borrow() {
                    continue;
                }

                let mut up_rx = up_rx.clone();
                let upstream = upstream.clone();

                tokio::spawn(async move {
                    let Ok(mut outbound) = TcpStream::connect(upstream).await else {
                        return;
                    };

                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                        _ = up_rx.wait_for(|up| !up) => {}
                    }
                });
            }
        });

        Self {
            port,
            up,
            accept_loop,
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Severs every open connection, and drops new ones until [`TcpProxy::restore`].
    pub fn cut(&self) {
        self.up.send_replace(false);
    }

    /// Forwards new connections again.
    pub fn restore(&self) {
        self.up.send_replace(true);
    }
}

impl Drop for TcpProxy {
    fn drop(&mut self) {
        self.cut();
        self.accept_loop.abort();
    }
}
