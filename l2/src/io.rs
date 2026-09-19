//! The deterministic I/O seam for the tonic transport, over `turmoil` (M1 stage 3b).
//!
//! [`TurmoilIo`] is the simulator's implementation of the production
//! [`TransportIo`](arachne_transport_tonic::TransportIo) seam. It virtualizes
//! every OS touch point the transport uses — listener bind, the incoming
//! connection stream, and the client-side dial — on top of `turmoil`'s
//! simulated TCP. The transport code (`factory.rs` / `transport.rs`) is
//! unchanged: it is generic over `Io`, and this file is the only thing that
//! differs from production's [`TokioIoProvider`](arachne_transport_tonic::TokioIoProvider).
//!
//! No real network, no wall clock, no real sleeps: everything runs on turmoil's
//! seeded simulated network + clock, which is what makes the 3-node cluster
//! deterministic under a fixed seed.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use http::Uri;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_stream::Stream;
use tonic::transport::server::{Connected, TcpConnectInfo};
use tower::Service;

use turmoil::net::{TcpListener, TcpStream};

/// A simulator implementation of the production [`TransportIo`] seam.
///
/// Stateless, so `Copy` (and trivially `Clone`/`Default`).
#[derive(Clone, Copy, Default)]
pub struct TurmoilIo;

/// One accepted server-side connection: a newtype over a turmoil `TcpStream`
/// that forwards `AsyncRead`/`AsyncWrite` and reports connection info to tonic
/// (so the gRPC server can see local/peer addresses).
#[derive(Debug)]
pub struct Accepted(pub TcpStream);

impl AsyncRead for Accepted {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for Accepted {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl Connected for Accepted {
    type ConnectInfo = TcpConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        TcpConnectInfo {
            local_addr: self.0.local_addr().ok(),
            remote_addr: self.0.peer_addr().ok(),
        }
    }
}

/// Client-side connector: resolves a URI's `host:port` and opens a turmoil
/// `TcpStream`, wrapped in `TokioIo` so it satisfies tonic's
/// `connect_with_connector` bounds.
///
/// Mirrors the production `TcpConnector`; the only differences are the turmoil
/// `TcpStream` and no `set_nodelay` (Nagle is irrelevant under turmoil's
/// in-memory links). Stateless, so `Copy`.
#[derive(Clone, Copy)]
pub struct TurmoilConnector;

impl Service<Uri> for TurmoilConnector {
    type Response = hyper_util::rt::TokioIo<TcpStream>;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        Box::pin(async move {
            let authority = uri
                .authority()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "URI has no authority (host:port)")
                })?;
            let addr: SocketAddr = authority.to_string().parse().map_err(
                |err: std::net::AddrParseError| io::Error::new(io::ErrorKind::InvalidInput, err),
            )?;
            let stream = TcpStream::connect(addr).await?;
            Ok(hyper_util::rt::TokioIo::new(stream))
        })
    }
}

impl arachne_transport_tonic::TransportIo for TurmoilIo {
    type Listener = TcpListener;
    // `Pin<Box<dyn Stream ..>>` is itself `Unpin` (the `Unpin` bound on the
    // associated type is satisfied by the box), so we must NOT also require
    // `+ Unpin` on the erased stream: `async_stream::stream!` is `!Unpin`.
    type Incoming = Pin<Box<dyn Stream<Item = io::Result<Accepted>> + Send + 'static>>;
    type ServerIo = Accepted;
    type ClientIo = hyper_util::rt::TokioIo<TcpStream>;
    type Connector = TurmoilConnector;
    type ConnectError = std::io::Error;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> impl Future<Output = std::io::Result<(Self::Listener, SocketAddr)>> + Send {
        async move {
            let listener = TcpListener::bind(addr).await?;
            let real_addr = listener.local_addr()?;
            Ok((listener, real_addr))
        }
    }

    fn incoming(&self, listener: Self::Listener) -> Self::Incoming {
        // The accept loop owns the bound listener for the life of the stream.
        // A failed accept ends the loop (the server stops taking new
        // connections); under a healthy sim this does not happen, so it is a
        // defensive terminal rather than a retryable error.
        let stream = async_stream::stream! {
            loop {
                match listener.accept().await {
                    Ok((stream, _peer)) => yield Ok(Accepted(stream)),
                    Err(_err) => break,
                }
            }
        };
        Box::pin(stream)
    }

    fn connector(&self) -> Self::Connector {
        TurmoilConnector
    }
}
