use profiler::common::{Msg, Request};
use reactor_actor::codec::BincodeCodec;
use std::{
    hint::black_box,
    time::{Duration, Instant},
};
use tokio_util::{bytes::BytesMut, codec::Encoder};

fn main() {
    let sample = Msg::Request(Request {
        client_addr: "127.0.0.1:8000".to_owned(),
        key: 1_000_000,
        value: 9_000_000,
        slot_id: 1023,
    });

    let mut codec = BincodeCodec::<Msg, Msg>::default();
    let mut dst = BytesMut::with_capacity(256);
    let mut count = 0_u64;

    println!("Encoding one message at a time for 60 seconds...");

    let start = Instant::now();

    while start.elapsed() < Duration::from_secs(60) {
        dst.clear();

        codec
            .encode(black_box(sample.clone()), &mut dst)
            .unwrap_or_else(|e| panic!("Encode failed: {}", e.io_error));

        black_box(&dst[..]);
        count += 1;
    }

    let elapsed = start.elapsed().as_secs_f64();

    println!("Total encodes: {count}");
    println!("Elapsed: {elapsed:.3} seconds");
    println!("Encodes/sec: {:.0}", count as f64 / elapsed);
    println!("ns/encode: {:.2}", elapsed * 1e9 / count as f64);
}

// Encoding one message at a time for 60 seconds...
// Total encodes: 499404823
// Elapsed: 60.000 seconds
// Encodes/sec: 8_323_414
// ns/encode: 120.14
