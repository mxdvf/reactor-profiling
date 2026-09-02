use crate::common::{Msg, Request};
use reactor_actor::codec::BincodeCodec;
use reactor_actor::{BehaviourBuilder, RouteTo, RuntimeCtx, SendErrAction};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
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

pub struct BenchmarkShared {
    generated_requests: AtomicU64,
    completed_requests: AtomicU64,
    completed_within_run: AtomicU64,
    generation_done: AtomicBool,
}

impl BenchmarkShared {
    fn new() -> Self {
        Self {
            generated_requests: AtomicU64::new(0),
            completed_requests: AtomicU64::new(0),
            completed_within_run: AtomicU64::new(0),
            generation_done: AtomicBool::new(false),
        }
    }
}

struct RequestFactory {
    client_addr: String,
    shared: Arc<BenchmarkShared>,
}

impl RequestFactory {
    fn new(client_addr: String, shared: Arc<BenchmarkShared>) -> Self {
        Self {
            client_addr,
            shared,
        }
    }

    fn generate_request(&self) -> Msg {
        self.shared
            .generated_requests
            .fetch_add(1, Ordering::Relaxed);

        Msg::Request(Request {
            client_addr: self.client_addr.clone(),
        })
    }
}

pub struct WorkloadIterator {
    remaining_initial_requests: usize,
    request_factory: RequestFactory,
}

impl WorkloadIterator {
    pub fn new(
        client_addr: String,
        config: Arc<WorkloadConfig>,
        shared: Arc<BenchmarkShared>,
    ) -> Self {
        Self {
            remaining_initial_requests: config.concurrency,
            request_factory: RequestFactory::new(client_addr, shared),
        }
    }
}

impl Iterator for WorkloadIterator {
    type Item = Msg;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_initial_requests == 0 {
            return None;
        }

        self.remaining_initial_requests -= 1;
        Some(self.request_factory.generate_request())
    }
}

struct Processor {
    start_time: Instant,
    client_id: String,
    config: Arc<WorkloadConfig>,
    shared: Arc<BenchmarkShared>,
    request_factory: RequestFactory,
}

impl Processor {
    fn handle_response(&mut self) -> Vec<Msg> {
        self.shared
            .completed_requests
            .fetch_add(1, Ordering::Relaxed);

        if self.start_time.elapsed() < self.config.run_duration {
            self.shared
                .completed_within_run
                .fetch_add(1, Ordering::Relaxed);

            return vec![self.request_factory.generate_request()];
        }

        self.mark_generation_done();
        vec![]
    }

    fn mark_generation_done(&self) {
        let was_already_done = self.shared.generation_done.swap(true, Ordering::Relaxed);

        if was_already_done {
            return;
        }

        let generated = self.shared.generated_requests.load(Ordering::Relaxed);
        let completed = self.shared.completed_requests.load(Ordering::Relaxed);
        let remaining = generated.saturating_sub(completed);

        println!(
            "LOAD GENERATION COMPLETED: {} requests outstanding",
            remaining
        );
    }
}

impl Drop for Processor {
    fn drop(&mut self) {
        let generated_requests = self.shared.generated_requests.load(Ordering::Relaxed);
        let completed_requests = self.shared.completed_requests.load(Ordering::Relaxed);
        let completed_within_run = self.shared.completed_within_run.load(Ordering::Relaxed);

        if let Err(e) = flush_results(
            &self.client_id,
            &self.config,
            generated_requests,
            completed_requests,
            completed_within_run,
        ) {
            eprintln!(
                "Failed to flush results for client {}: {}",
                self.client_id, e
            );
        }
    }
}

impl reactor_actor::ActorProcess for Processor {
    type IMsg = Msg;
    type OMsg = Msg;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        match input {
            Msg::Request(request) => vec![Msg::Request(request)],
            Msg::Response(_) => self.handle_response(),
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
) -> std::io::Result<()> {
    let filename = format!(
        "logs/intermediate/client_{}_{}conc.csv",
        client_id, config.concurrency
    );

    let mut writer = BufWriter::new(File::create(filename)?);

    let request_throughput_rps = completed_within_run as f64 / config.run_duration.as_secs_f64();

    let message_throughput_mps = request_throughput_rps * 2.0;

    writeln!(
        writer,
        "run_duration_s,concurrency,generated_requests,completed_requests,completed_within_run,request_throughput_rps,message_throughput_mps"
    )?;

    writeln!(
        writer,
        "{},{},{},{},{},{},{}",
        config.run_duration.as_secs(),
        config.concurrency,
        generated_requests,
        completed_requests,
        completed_within_run,
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
    std::thread::sleep(
        Duration::from_millis(3000) + Duration::from_millis(rand::rng().random_range(0..2000)),
    );

    let workload: Workload =
        serde_json::from_value(payload.remove("workload").expect("missing workload"))
            .expect("invalid workload");

    let config = Arc::new(WorkloadConfig::new(workload));
    let shared = Arc::new(BenchmarkShared::new());

    let start_time = Instant::now();
    let addr = ctx.addr.to_string();
    let client_id = addr.rsplit('_').next().unwrap().to_string();

    BehaviourBuilder::new(
        Processor {
            start_time,
            client_id,
            config: Arc::clone(&config),
            shared: Arc::clone(&shared),
            request_factory: RequestFactory::new(addr.clone(), Arc::clone(&shared)),
        },
        BincodeCodec::default(),
    )
    .send(Sender::new(server))
    .generator_if(true, || {
        WorkloadIterator::new(addr, Arc::clone(&config), Arc::clone(&shared))
    })
    .on_send_failure(SendErrAction::Drop)
    .build()
    .run(ctx)
    .await
    .unwrap();

    println!("EXPERIMENT COMPLETED, YOU CAN EXIT NOW.");
}
