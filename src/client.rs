use crate::common::{Msg, Request};
use reactor_actor::codec::BincodeCodec;
use reactor_actor::{
    ActorProcess, ActorSend, BehaviourBuilder, RouteTo, RuntimeCtx, SendErrAction,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};

// //////////////////////////////////////////////////////////////////////////////
//                                  RECEIVER
// //////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct Receiver {
    started: Option<Instant>,
    received: u64,
    reported: bool,
}

impl reactor_actor::ActorRecv for Receiver {
    type IMsg = Msg;

    async fn after_recv(&mut self, _: &str, input: &Msg) -> reactor_actor::ChannelAction {
        if !self.reported && matches!(input, Msg::Response(_)) {
            let started = self.started.get_or_insert_with(Instant::now);
            self.received += 1;

            if started.elapsed() >= Duration::from_secs(60) {
                println!("Requests received in ~60s: {}", self.received);
                println!("Throughput is: {}", self.received / 60);
                self.reported = true;
            }
        }

        reactor_actor::ChannelAction::PASS
    }
}

// //////////////////////////////////////////////////////////////////////////////
//                                  PROCESSOR
// //////////////////////////////////////////////////////////////////////////////

struct Processor;

impl ActorProcess for Processor {
    type IMsg = Msg;
    type OMsg = Msg;

    fn process(&mut self, input: Msg) -> Vec<Msg> {
        match input {
            Msg::Request(_) => vec![input],
            _ => vec![],
        }
    }
}

// //////////////////////////////////////////////////////////////////////////////
//                                  SENDER
// //////////////////////////////////////////////////////////////////////////////

struct Sender {
    server: String,
}

impl ActorSend for Sender {
    type OMsg = Msg;

    async fn before_send<'a>(&'a mut self, output: &Msg) -> RouteTo<'a> {
        match output {
            Msg::Request(_) => RouteTo::from(self.server.as_str()),
            _ => {
                panic!("Client tried to send a response message")
            }
        }
    }
}

pub async fn client(ctx: RuntimeCtx, server: String, _payload: HashMap<String, serde_json::Value>) {
    std::thread::sleep(std::time::Duration::from_secs(3));

    let client_addr = ctx.addr.to_string();
    println!("{}", client_addr);

    let workload = std::iter::repeat_with(move || {
        Msg::Request(Request {
            key: 100000,
            value: 10000001,
            slot_id: 100,
        })
    });

    BehaviourBuilder::new(Processor, BincodeCodec::default())
        .recv(Receiver::default())
        .send(Sender { server })
        .generator_if(true, || workload)
        .on_send_failure(SendErrAction::Drop)
        .build()
        .run(ctx)
        .await
        .unwrap();
}
