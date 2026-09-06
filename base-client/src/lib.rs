use rand::Rng;
use serde::Deserialize;

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::time::{Duration, Instant};

#[derive(Clone, Deserialize)]
pub struct Workload {
    #[serde(default)]
    pub concurrency: usize,

    #[serde(default)]
    pub warmup_duration: u64,

    #[serde(default)]
    pub run_duration: u64,
}

pub struct WorkloadConfig {
    pub concurrency: usize,
    pub warmup_duration: Duration,
    pub run_duration: Duration,
}

impl WorkloadConfig {
    fn new(workload: Workload) -> Self {
        Self {
            concurrency: workload.concurrency,
            warmup_duration: Duration::from_secs(workload.warmup_duration),
            run_duration: Duration::from_secs(workload.run_duration),
        }
    }
}

pub fn set_slot_id(payload: &mut [u8], slot_id: usize) {
    payload[..8].copy_from_slice(&(slot_id as u64).to_le_bytes());
}

pub fn get_slot_id(payload: &[u8]) -> usize {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&payload[..8]);

    u64::from_le_bytes(bytes) as usize
}

/*
 * Only seeds the initial concurrency window.
 *
 * The function pointer constructs the protocol-specific message.
 */
pub struct WorkloadIterator<M> {
    next_slot_id: usize,
    concurrency: usize,
    client_addr: String,
    make_request: fn(&str, usize) -> M,
}

impl<M> Iterator for WorkloadIterator<M> {
    type Item = M;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_slot_id >= self.concurrency {
            return None;
        }

        let slot_id = self.next_slot_id;
        self.next_slot_id += 1;

        Some((self.make_request)(&self.client_addr, slot_id))
    }
}

pub struct Benchmark {
    start_time: Option<Instant>,

    client_id: String,
    client_addr: String,

    config: WorkloadConfig,
    send_times: Vec<Option<Instant>>,

    initial_requests: usize,
    generated_requests: u64,
    completed_requests: u64,
    completed_within_run: u64,
    total_latency_ns: u128,
    outstanding_requests: usize,

    generation_done: bool,
}

impl Benchmark {
    pub fn new(client_addr: String, mut payload: HashMap<String, serde_json::Value>) -> Self {
        /*
         * Retain the original randomized startup delay.
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

        let client_id = client_addr.rsplit('_').next().unwrap().to_string();

        Self {
            start_time: None,

            client_id,
            client_addr,

            send_times: vec![None; config.concurrency],
            config,

            initial_requests: 0,
            generated_requests: 0,
            completed_requests: 0,
            completed_within_run: 0,
            total_latency_ns: 0,
            outstanding_requests: 0,

            generation_done: false,
        }
    }

    pub fn client_addr(&self) -> &str {
        &self.client_addr
    }

    /*
     * Returns an owned iterator. It does not borrow Benchmark.
     */
    pub fn workload<M>(&self, make_request: fn(&str, usize) -> M) -> WorkloadIterator<M> {
        WorkloadIterator {
            next_slot_id: 0,
            concurrency: self.config.concurrency,
            client_addr: self.client_addr.clone(),
            make_request,
        }
    }

    fn occupy_slot(&mut self, slot_id: usize, now: Instant) {
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

        self.send_times[slot_id] = Some(now);

        self.generated_requests += 1;
        self.outstanding_requests += 1;
    }

    /*
     * Called when an initial request enters the protocol processor.
     *
     * Setup and iterator construction do not start the benchmark.
     */
    pub fn on_initial_request(&mut self, slot_id: usize) {
        let now = Instant::now();

        if self.start_time.is_none() {
            self.start_time = Some(now);
        }

        self.occupy_slot(slot_id, now);
        self.initial_requests += 1;
    }

    /*
     * Called exactly once per completed logical operation.
     *
     * true:
     *   The slot has been reoccupied and timed.
     *   Return one replacement request for this slot.
     *
     * false:
     *   The slot has been drained.
     *   Do not return a replacement.
     */
    pub fn on_response(&mut self, slot_id: usize) -> bool {
        let completion_time = Instant::now();

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

        let elapsed = completion_time.duration_since(start_time);

        let measurement_end = self.config.warmup_duration + self.config.run_duration;

        /*
         * Only completions inside the measurement interval count.
         *
         * As in the original, a request sent during warmup can
         * count if its completion occurs during measurement.
         */
        if elapsed >= self.config.warmup_duration && elapsed < measurement_end {
            self.completed_within_run += 1;
            self.total_latency_ns += latency_ns;
        }

        /*
         * Both warmup and measurement maintain the closed loop.
         */
        if elapsed < measurement_end {
            self.occupy_slot(slot_id, Instant::now());
            return true;
        }

        /*
         * After the deadline, stop replacing completed requests.
         */
        if !self.generation_done {
            self.generation_done = true;

            println!(
                "LOAD GENERATION COMPLETED: {} requests outstanding",
                self.outstanding_requests
            );
        }

        /*
         * Also check that all initial requests entered Processor,
         * in case initial generation and responses interleave.
         */
        if self.outstanding_requests == 0 && self.initial_requests == self.config.concurrency {
            println!("REQUESTS DRAINED.");
        }

        false
    }

    fn flush_results(&self) -> std::io::Result<()> {
        fs::create_dir_all("logs/intermediate")?;

        let filename = format!(
            "logs/intermediate/client_{}_{}conc.csv",
            self.client_id, self.config.concurrency
        );

        let mut writer = BufWriter::new(File::create(filename)?);

        let request_throughput_rps =
            self.completed_within_run as f64 / self.config.run_duration.as_secs_f64();

        /*
         * Preserve the original CSV convention:
         * one request + one response per logical operation.
         *
         * This does not count protocol-internal messages.
         */
        let message_throughput_mps = request_throughput_rps * 2.0;

        let average_latency_us = if self.completed_within_run == 0 {
            0.0
        } else {
            self.total_latency_ns as f64 / self.completed_within_run as f64 / 1000.0
        };

        writeln!(
            writer,
            "run_duration_s,concurrency,generated_requests,\
             completed_requests,completed_within_run,total_latency_ns,\
             average_latency_us,request_throughput_rps,\
             message_throughput_mps"
        )?;

        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{}",
            self.config.run_duration.as_secs(),
            self.config.concurrency,
            self.generated_requests,
            self.completed_requests,
            self.completed_within_run,
            self.total_latency_ns,
            average_latency_us,
            request_throughput_rps,
            message_throughput_mps,
        )?;

        writer.flush()
    }
}

impl Drop for Benchmark {
    fn drop(&mut self) {
        let _ = self.flush_results();
    }
}
