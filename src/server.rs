use crate::common::{Msg, Response};
use reactor_actor::codec::BincodeCodec;
use reactor_actor::{ActorProcess, ActorRecv, BehaviourBuilder, ChannelAction, RuntimeCtx};
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

impl ActorRecv for Receiver {
    type IMsg = Msg;

    async fn after_recv(&mut self, _: &str, input: &Msg) -> ChannelAction {
        if !self.reported && matches!(input, Msg::Request(_)) {
            let started = self.started.get_or_insert_with(Instant::now);
            self.received += 1;

            if started.elapsed() >= Duration::from_secs(60) {
                println!("Requests received in ~60s: {}", self.received);
                println!("Throughput is: {}", self.received / 60);
                self.reported = true;
            }
        }

        ChannelAction::PASS
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
            Msg::Request(request) => {
                vec![Msg::Response(Response {
                    slot_id: request.slot_id,
                })]
            }
            _ => {
                panic!("Server received a response message")
            }
        }
    }
}

// //////////////////////////////////////////////////////////////////////////////
//                                  SENDER
// //////////////////////////////////////////////////////////////////////////////

struct Sender;

impl reactor_actor::ActorSend for Sender {
    type OMsg = Msg;

    async fn before_send<'a>(&'a mut self, output: &Self::OMsg) -> reactor_actor::RouteTo<'a> {
        match output {
            Msg::Response(_) => reactor_actor::RouteTo::Reply,
            _ => panic!("Server tried sending a request message"),
        }
    }
}

pub async fn server(ctx: RuntimeCtx) {
    BehaviourBuilder::new(Processor, BincodeCodec::default())
        .recv(Receiver::default())
        .send(Sender)
        .build()
        .run(ctx)
        .await
        .unwrap();
}
