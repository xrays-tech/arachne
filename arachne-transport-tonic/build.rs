// Build script for `arachne-transport-tonic`: compiles the gRPC wire protocol
// (`proto/raft.proto`) with `tonic-build` / `prost-build`.
//
// # Why a vendored `protoc`
//
// `prost-build` locates `protoc` via the `PROTOC` environment variable, or a
// `protoc` on `PATH`. We deliberately point it at a **vendored** binary from
// `protoc-bin-vendored` rather than the system one, so the build is
// reproducible and independent of whatever `protoc` (if any) a developer or CI
// runner happens to have installed. (The core `raft` build already runs into
// `protobuf-build` rejecting a system `protoc 25.3`; tonic/prost tolerate it,
// but we do not want the result to depend on that coincidence.)
//
// # The one `unsafe` block
//
// `std::env::set_var` is `unsafe` in the 2024 edition. It is used exactly once
// here, at the very top of the (single-threaded) build script, to publish
// `PROTOC` before `tonic-build` reads it. This is safe: the build script is its
// own process, no other thread runs concurrently to observe the environment
// change, and the variable is set once before any `prost-build` call reads it.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: single-threaded build script; set once before any read of
    // `PROTOC`. No concurrent access to the process environment exists here.
    unsafe { std::env::set_var("PROTOC", &protoc) };

    tonic_build::configure()
        .compile_protos(&["proto/raft.proto"], &["proto"])?;
    Ok(())
}
