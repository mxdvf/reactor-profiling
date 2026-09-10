use profiler::common::{Msg, Request};
use reactor_actor::codec::BincodeCodec;
use std::{
    hint::black_box,
    time::{Duration, Instant},
};
use tokio_util::{
    bytes::BytesMut,
    codec::{Decoder, Encoder},
};

fn main() {
    let sample = Msg::Request(Request {
        client_addr: "127.0.0.1:8000".to_owned(),
        key: 1_000_000,
        value: 9_000_000,
        slot_id: 1023,
    });

    let mut codec = BincodeCodec::<Msg, Msg>::default();
    let mut encoded = BytesMut::with_capacity(256);

    codec
        .encode(sample, &mut encoded)
        .unwrap_or_else(|e| panic!("Encode failed: {}", e.io_error));

    let mut count = 0_u64;

    println!("Decoding one message at a time for 60 seconds...");

    let start = Instant::now();

    while start.elapsed() < Duration::from_secs(60) {
        let mut src = encoded.clone();

        let message = codec
            .decode(black_box(&mut src))
            .expect("Decode failed")
            .expect("Expected a complete message");

        black_box(message);
        count += 1;
    }

    let elapsed = start.elapsed().as_secs_f64();

    println!("Total decodes: {count}");
    println!("Elapsed: {elapsed:.3} seconds");
    println!("Decodes/sec: {:.0}", count as f64 / elapsed);
    println!("ns/decode: {:.2}", elapsed * 1e9 / count as f64);
}

// Decoding one message at a time for 60 seconds...
// Total decodes: 819929583
// Elapsed: 60.000 seconds
// Decodes/sec: 13_665_493
// ns/decode: 73.18
