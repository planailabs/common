//! End-to-end check that the OTLP wiring actually ships something.
//!
//! Its own test binary because `otel::init` is process-global and only takes
//! effect once — the in-crate unit tests run it without an endpoint.

#![cfg(feature = "otel")]

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::time::Duration;

/// Minimal HTTP sink: answers 200 to anything and reports the request line.
fn spawn_collector() -> (u16, mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();
    let (tx, rx) = mpsc::channel();

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let tx = tx.clone();
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                let n = stream.read(&mut buf).unwrap_or(0);
                let request_line = String::from_utf8_lossy(&buf[..n])
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .to_owned();
                let _ = stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                let _ = tx.send(request_line);
            });
        }
    });

    (port, rx)
}

#[test]
fn exports_traces_and_logs_to_the_configured_endpoint() {
    let (port, rx) = spawn_collector();
    // SAFETY: single-threaded test setup, before any other thread reads the env.
    unsafe {
        std::env::set_var(
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            format!("http://127.0.0.1:{port}"),
        );
    }

    mac_mgmt_common::otel::init("otlp-export-test", "info");

    let span = tracing::info_span!("unit-of-work");
    span.in_scope(|| tracing::error!("something went wrong"));
    drop(span); // spans export on close

    mac_mgmt_common::otel::shutdown();

    let mut paths = HashSet::new();
    while let Ok(line) = rx.recv_timeout(Duration::from_secs(15)) {
        if let Some(path) = line.split_whitespace().nth(1) {
            paths.insert(path.to_owned());
        }
        if paths.contains("/v1/traces") && paths.contains("/v1/logs") {
            break;
        }
    }

    assert!(paths.contains("/v1/traces"), "got {paths:?}");
    assert!(paths.contains("/v1/logs"), "got {paths:?}");
}
