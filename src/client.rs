use crate::common::{Msg, PAYLOAD_SIZE, Request};

use rand::Rng;
use reactor_actor::codec::BincodeCodec;
use reactor_actor::{BehaviourBuilder, RouteTo, RuntimeCtx, SendErrAction};
use serde::Deserialize;

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::{Duration, Instant};

#[derive(Clone, Deserialize)]
pub struct Workload {
    #[serde(default)]
    pub concurrency: usize,

    #[serde(default)]
    pub run_duration: u64,
}

pub struct WorkloadConfig {
    pub concurrency: usize,
    pub run_duration: Duration,
}

impl WorkloadConfig {
    fn new(workload: Workload) -> Self {
        Self {
            concurrency: workload.concurrency,
            run_duration: Duration::from_secs(workload.run_duration),
        }
    }
}

/*
 * The first 8 bytes of the 128-byte payload are used as a slot ID.
 *
 * The payload is STILL exactly 128 bytes.
 *
 * For concurrency = 200, slot IDs are:
 *
 *     0 .. 199
 *
 * Every response returns the same slot ID, allowing Processor to
 * retrieve the corresponding send timestamp without a HashMap,
 * Arc, Atomic, Mutex, etc.
 */

fn set_slot_id(payload: &mut [u8; PAYLOAD_SIZE], slot_id: u64) {
    payload[..8].copy_from_slice(&slot_id.to_le_bytes());
}

fn get_slot_id(payload: &[u8; PAYLOAD_SIZE]) -> usize {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&payload[..8]);

    u64::from_le_bytes(bytes) as usize
}

fn make_request(client_addr: &str, slot_id: usize) -> Msg {
    let mut payload = [0u8; PAYLOAD_SIZE];

    set_slot_id(&mut payload, slot_id as u64);

    Msg::Request(Request {
        client_addr: client_addr.to_string(),
        payload,
    })
}

/*
 * Generator's ONLY responsibility:
 *
 * seed the initial concurrency window.
 *
 * For concurrency = 200:
 *
 *     slot 0
 *     slot 1
 *     ...
 *     slot 199
 *
 * Then Iterator returns None and disappears from the benchmark.
 */
pub struct WorkloadIterator {
    next_slot_id: usize,
    concurrency: usize,
    client_addr: String,
}

impl WorkloadIterator {
    pub fn new(client_addr: String, concurrency: usize) -> Self {
        Self {
            next_slot_id: 0,
            concurrency,
            client_addr,
        }
    }
}

impl Iterator for WorkloadIterator {
    type Item = Msg;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_slot_id >= self.concurrency {
            return None;
        }

        let slot_id = self.next_slot_id;

        self.next_slot_id += 1;

        Some(make_request(&self.client_addr, slot_id))
    }
}

struct Processor {
    /*
     * Benchmark starts when the FIRST request actually reaches
     * Processor.
     *
     * Therefore BehaviourBuilder/setup time is not included.
     */
    start_time: Option<Instant>,

    client_id: String,
    client_addr: String,

    config: WorkloadConfig,

    /*
     * One timestamp per concurrency slot.
     *
     * No HashMap.
     * No atomics.
     * No locks.
     *
     * send_times[17] is the timestamp for whatever request is
     * currently occupying closed-loop slot 17.
     */
    send_times: Vec<Option<Instant>>,

    generated_requests: u64,
    completed_requests: u64,

    /*
     * Only completions occurring during the configured benchmark
     * interval.
     *
     * Used for throughput AND average latency.
     */
    completed_within_run: u64,

    /*
     * Sum rather than storing every latency sample.
     *
     * This keeps profiling overhead extremely small.
     */
    total_latency_ns: u128,

    /*
     * Number of requests currently in flight.
     */
    outstanding_requests: usize,

    generation_done: bool,
    results_flushed: bool,
}

impl Processor {
    fn handle_initial_request(&mut self, request: Request) -> Vec<Msg> {
        let now = Instant::now();

        /*
         * Start the experiment exactly when the first request
         * enters Processor.
         */
        if self.start_time.is_none() {
            self.start_time = Some(now);
        }

        let slot_id = get_slot_id(&request.payload);

        assert!(
            slot_id < self.config.concurrency,
            "Invalid slot ID {}",
            slot_id
        );

        assert!(
            self.send_times[slot_id].is_none(),
            "Slot {} is already occupied",
            slot_id
        );

        /*
         * Latency clock starts here.
         */
        self.send_times[slot_id] = Some(now);

        self.generated_requests += 1;
        self.outstanding_requests += 1;

        vec![Msg::Request(request)]
    }

    fn handle_response(&mut self, response: crate::common::Response) -> Vec<Msg> {
        let completion_time = Instant::now();

        let slot_id = get_slot_id(&response.payload);

        assert!(
            slot_id < self.config.concurrency,
            "Invalid response slot ID {}",
            slot_id
        );

        let send_time = self.send_times[slot_id]
            .take()
            .expect("Response received for an empty slot");

        let latency_ns = completion_time.duration_since(send_time).as_nanos();

        self.completed_requests += 1;

        assert!(
            self.outstanding_requests > 0,
            "Response received with no outstanding requests"
        );

        self.outstanding_requests -= 1;

        let start_time = self
            .start_time
            .expect("Response received before benchmark started");

        let completed_inside_measurement_window =
            completion_time.duration_since(start_time) < self.config.run_duration;

        /*
         * Only operations completing inside the measurement window
         * contribute to throughput and average latency.
         *
         * Drain-phase responses do not.
         */
        if completed_inside_measurement_window {
            self.completed_within_run += 1;
            self.total_latency_ns += latency_ns;

            /*
             * Response releases one closed-loop slot.
             *
             * Immediately reuse the SAME slot for the next request.
             */
            let replacement_send_time = Instant::now();

            self.send_times[slot_id] = Some(replacement_send_time);

            self.generated_requests += 1;
            self.outstanding_requests += 1;

            return vec![make_request(&self.client_addr, slot_id)];
        }

        /*
         * First response observed after measurement deadline.
         *
         * Stop replacing requests.
         */
        if !self.generation_done {
            self.generation_done = true;

            println!(
                "LOAD GENERATION COMPLETED: {} requests outstanding",
                self.outstanding_requests
            );
        }

        /*
         * Every subsequent response consumes one outstanding slot
         * without creating another request.
         *
         * Eventually:
         *
         *     outstanding_requests == 0
         */
        if self.outstanding_requests == 0 {
            println!("REQUESTS DRAINED.");
        }

        vec![]
    }

    fn flush_results_once(&mut self) {
        if self.results_flushed {
            return;
        }

        self.results_flushed = true;

        if let Err(e) = flush_results(
            &self.client_id,
            &self.config,
            self.generated_requests,
            self.completed_requests,
            self.completed_within_run,
            self.total_latency_ns,
        ) {
            eprintln!(
                "Failed to flush results for client {}: {}",
                self.client_id, e
            );
        }
    }
}

impl Drop for Processor {
    fn drop(&mut self) {
        /*
         * Fallback only.
         *
         * Normally results are flushed when REQUESTS DRAINED.
         */
        self.flush_results_once();
    }
}

impl reactor_actor::ActorProcess for Processor {
    type IMsg = Msg;
    type OMsg = Msg;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        match input {
            /*
             * Only the initial concurrency window enters through
             * this branch.
             */
            Msg::Request(request) => self.handle_initial_request(request),

            Msg::Response(response) => self.handle_response(response),
        }
    }
}

struct Sender {
    server: String,
}

impl reactor_actor::ActorSend for Sender {
    type OMsg = Msg;

    async fn before_send<'a>(&'a mut self, output: &Self::OMsg) -> RouteTo<'a> {
        match output {
            Msg::Request(_) => RouteTo::from(self.server.as_str()),

            Msg::Response(_) => {
                panic!("Client tried to send a response message")
            }
        }
    }
}

impl Sender {
    fn new(server: String) -> Self {
        Self { server }
    }
}

fn flush_results(
    client_id: &str,
    config: &WorkloadConfig,
    generated_requests: u64,
    completed_requests: u64,
    completed_within_run: u64,
    total_latency_ns: u128,
) -> std::io::Result<()> {
    let filename = format!(
        "logs/intermediate/client_{}_{}conc.csv",
        client_id, config.concurrency
    );

    let mut writer = BufWriter::new(File::create(filename)?);

    let request_throughput_rps = completed_within_run as f64 / config.run_duration.as_secs_f64();

    let message_throughput_mps = request_throughput_rps * 2.0;

    /*
     * Weighted aggregation later requires total_latency_ns,
     * not merely average_latency_us.
     */
    let average_latency_us = if completed_within_run == 0 {
        0.0
    } else {
        total_latency_ns as f64 / completed_within_run as f64 / 1000.0
    };

    writeln!(
        writer,
        "run_duration_s,concurrency,generated_requests,completed_requests,completed_within_run,total_latency_ns,average_latency_us,request_throughput_rps,message_throughput_mps"
    )?;

    writeln!(
        writer,
        "{},{},{},{},{},{},{},{},{}",
        config.run_duration.as_secs(),
        config.concurrency,
        generated_requests,
        completed_requests,
        completed_within_run,
        total_latency_ns,
        average_latency_us,
        request_throughput_rps,
        message_throughput_mps,
    )?;

    writer.flush()
}

pub async fn client(
    ctx: RuntimeCtx,
    server: String,
    mut payload: HashMap<String, serde_json::Value>,
) {
    /*
     * Randomized startup delay retained.
     */
    std::thread::sleep(
        Duration::from_millis(3000) + Duration::from_millis(rand::rng().random_range(0..2000)),
    );

    let workload: Workload =
        serde_json::from_value(payload.remove("workload").expect("missing workload"))
            .expect("invalid workload");

    let config = WorkloadConfig::new(workload);

    assert!(
        config.concurrency > 0,
        "concurrency must be greater than zero"
    );

    assert!(
        !config.run_duration.is_zero(),
        "run_duration must be greater than zero"
    );

    let concurrency = config.concurrency;

    let addr = ctx.addr.to_string();

    let client_id = addr.rsplit('_').next().unwrap().to_string();

    BehaviourBuilder::new(
        Processor {
            start_time: None,

            client_id,
            client_addr: addr.clone(),

            send_times: (0..concurrency).map(|_| None).collect(),

            config,

            generated_requests: 0,
            completed_requests: 0,
            completed_within_run: 0,

            total_latency_ns: 0,

            outstanding_requests: 0,

            generation_done: false,
            results_flushed: false,
        },
        BincodeCodec::default(),
    )
    .send(Sender::new(server))
    .generator_if(true, || WorkloadIterator::new(addr, concurrency))
    .on_send_failure(SendErrAction::Drop)
    .build()
    .run(ctx)
    .await
    .unwrap();

    println!("EXPERIMENT COMPLETED, YOU CAN EXIT NOW.");
}
