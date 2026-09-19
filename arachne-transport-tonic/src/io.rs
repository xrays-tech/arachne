//! The injectable I/O seam for the tonic transport (M1 stage 3a).
//!
//! [`TransportIo`] isolates every touch point where the transport talks to the
//! OS — listener bind, the incoming connection stream, and the client-side
//! dial — behind one seam. The tonic transport is generic over `Io`, so the
//! exact same production code can later be pointed at a deterministic
//! simulator (M1 stage 3b) without a single production line changing.
//!
//! [`TokioIoProvider`] is the production implementation: real tokio TCP. It is
//! the default for [`crate::TonicTransport`] and [`crate::TonicTransportFactory`],
//! so every existing caller keeps compiling and behaving byte-for-byte
//! identically.
//!
//! This file only adds the seam and its production impl; it does not change
//! wire format, handshake, or channel behaviour.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use http::Uri;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::Stream;
use tonic::transport::server::Connected;
use tower::Service;

/// The I/O seam the tonic transport is built on.
///
/// The production impl ([`TokioIoProvider`]) uses real tokio TCP; a
/// deterministic simulator (M1 stage 3b) will provide an impl where
/// `Listener`/`Incoming`/`ClientIo` are virtualized. The transport code stays
/// identical across both.
pub trait TransportIo: Clone + Send + Sync + 'static {
    /// The bound listener handle.
    type Listener: Send + 'static;

    /// The stream of accepted connections produced from a bound listener.
    type Incoming: Stream<Item = Result<Self::ServerIo, std::io::Error>> + Send + Unpin + 'static;

    /// One accepted server-side connection.
    type ServerIo: AsyncRead + AsyncWrite + Connected + Send + Unpin + 'static;

    /// One outgoing client-side connection.
    type ClientIo: hyper::rt::Read + hyper::rt::Write + Send + Unpin + 'static;

    /// A connector that dials a URI's `host:port` and yields a [`Self::ClientIo`].
    ///
    /// `tower::Service` so it plugs into
    /// [`tonic::transport::Endpoint::connect_with_connector`]. The bounds here
    /// mirror the real `connect_with_connector` contract (tonic 0.12.3):
    /// `Response` must be a hyper `Read + Write`, `Future: Send`, and the
    /// error must be `Error + Send + Sync + 'static` (so `tonic::Error` — which
    /// is `Box<dyn Error + Send + Sync>` — can be built from it).
    type Connector: Service<Uri, Response = Self::ClientIo, Error = Self::ConnectError, Future: Send>
        + Clone
        + Send
        + Sync
        + 'static;

    /// The error a [`Self::Connector`] can produce.
    type ConnectError: std::error::Error + Send + Sync + 'static;

    /// Bind a listener to `addr`, returning the handle and the actual bound
    /// address (e.g. the real port when `addr` is `0`).
    fn bind(
        &self,
        addr: SocketAddr,
    ) -> impl Future<Output = std::io::Result<(Self::Listener, SocketAddr)>> + Send;

    /// Produce the incoming-connection stream from a bound listener.
    fn incoming(&self, listener: Self::Listener) -> Self::Incoming;

    /// The client-side connector used to dial peers.
    fn connector(&self) -> Self::Connector;
}

/// Production I/O: real tokio TCP.
///
/// This is the default implementation. It reproduces the transport's existing
/// behaviour exactly: `TcpListener::bind`, `TcpListenerStream`, and a direct
/// `TcpStream::connect` wrapped in [`hyper_util::rt::TokioIo`].
#[derive(Clone, Copy, Default)]
pub struct TokioIoProvider;

impl TransportIo for TokioIoProvider {
    type Listener = tokio::net::TcpListener;
    type Incoming = tokio_stream::wrappers::TcpListenerStream;
    type ServerIo = tokio::net::TcpStream;
    type ClientIo = hyper_util::rt::TokioIo<tokio::net::TcpStream>;
    type Connector = TcpConnector;
    type ConnectError = std::io::Error;

    // RPIT (not `async fn`) is deliberate: the trait declares the returned
    // future as `+ Send`, a bound `async fn` cannot express on a method. The
    // body is a plain async block, so clippy's `manual_async_fn` is a false
    // positive here.
    #[allow(clippy::manual_async_fn)]
    fn bind(
        &self,
        addr: SocketAddr,
    ) -> impl Future<Output = std::io::Result<(Self::Listener, SocketAddr)>> + Send {
        async move {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            let real_addr = listener.local_addr()?;
            Ok((listener, real_addr))
        }
    }

    fn incoming(&self, listener: Self::Listener) -> Self::Incoming {
        tokio_stream::wrappers::TcpListenerStream::new(listener)
    }

    fn connector(&self) -> Self::Connector {
        TcpConnector
    }
}

/// Client-side TCP connector: resolves a URI's `host:port` and opens a real
/// tokio [`tokio::net::TcpStream`], wrapped in [`hyper_util::rt::TokioIo`] so
/// it satisfies tonic's `connect_with_connector` bounds.
///
/// Stateless, so it is `Copy` (and trivially `Clone`).
#[derive(Clone, Copy)]
pub struct TcpConnector;

impl Service<Uri> for TcpConnector {
    type Response = hyper_util::rt::TokioIo<tokio::net::TcpStream>;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        Box::pin(async move {
            let authority = uri
                .authority()
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "URI has no authority (host:port)"))?;
            let addr: SocketAddr = authority.to_string().parse().map_err(
                |err: std::net::AddrParseError| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, err)
                },
            )?;
            let stream = tokio::net::TcpStream::connect(addr).await?;
            // Preserve tonic's default-connector behaviour: `Endpoint` defaults
            // to `tcp_nodelay = true`, which the default (bypassed) connector
            // applied to the client socket. Raft ships small messages, so Nagle
            // would add avoidable latency.
            stream.set_nodelay(true)?;
            Ok(hyper_util::rt::TokioIo::new(stream))
        })
    }
}
