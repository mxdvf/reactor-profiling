use crate::common::{Msg, Request};

use base_client;

use reactor_actor::codec::BincodeCodec;
use reactor_actor::{BehaviourBuilder, RouteTo, RuntimeCtx, SendErrAction};

use std::collections::HashMap;

// //////////////////////////////////////////////////////////////////////////////
//                                  REQUEST GENERATOR
// //////////////////////////////////////////////////////////////////////////////

fn make_request(client_addr: &str, slot_id: usize) -> Msg {
    Msg::Request(Request {
        client_addr: client_addr.to_string(),
        key: slot_id as u64,
        value: 0,
        slot_id,
    })
}

// //////////////////////////////////////////////////////////////////////////////
//                                  PROCESSOR
// //////////////////////////////////////////////////////////////////////////////

struct Processor {
    benchmark: base_client::Benchmark,
}

impl reactor_actor::ActorProcess for Processor {
    type IMsg = Msg;
    type OMsg = Msg;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        match input {
            Msg::Request(request) => {
                self.benchmark.on_initial_request(request.slot_id);

                vec![Msg::Request(request)]
            }

            Msg::Response(response) => {
                let slot_id = response.slot_id;

                if self.benchmark.on_response(slot_id) {
                    vec![make_request(self.benchmark.client_addr(), slot_id)]
                } else {
                    vec![]
                }
            }
        }
    }
}

// //////////////////////////////////////////////////////////////////////////////
//                                  SENDER
// //////////////////////////////////////////////////////////////////////////////

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

// //////////////////////////////////////////////////////////////////////////////
//                                  ACTOR
// //////////////////////////////////////////////////////////////////////////////

pub async fn client(ctx: RuntimeCtx, server: String, payload: HashMap<String, serde_json::Value>) {
    let benchmark = base_client::Benchmark::new(ctx.addr.to_string(), payload);
    let workload = benchmark.workload(make_request);

    BehaviourBuilder::new(Processor { benchmark }, BincodeCodec::default())
        .send(Sender::new(server))
        .generator_if(true, || workload)
        .on_send_failure(SendErrAction::Drop)
        .build()
        .run(ctx)
        .await
        .unwrap();

    println!("EXPERIMENT COMPLETED, YOU CAN EXIT NOW.");
}
