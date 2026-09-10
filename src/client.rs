use crate::common::{Msg, Request};
use reactor_actor::codec::BincodeCodec;
use reactor_actor::{
    ActorProcess, ActorSend, BehaviourBuilder, RouteTo, RuntimeCtx, SendErrAction,
};
use std::collections::HashMap;

struct Processor;

impl ActorProcess for Processor {
    type IMsg = Msg;
    type OMsg = Msg;

    fn process(&mut self, input: Msg) -> Vec<Msg> {
        vec![input]
    }
}

struct Sender {
    server: String,
}

impl ActorSend for Sender {
    type OMsg = Msg;

    async fn before_send<'a>(&'a mut self, _: &Msg) -> RouteTo<'a> {
        RouteTo::from(self.server.as_str())
    }
}

pub async fn client(ctx: RuntimeCtx, server: String, _payload: HashMap<String, serde_json::Value>) {
    std::thread::sleep(std::time::Duration::from_secs(3));

    let client_addr = ctx.addr.to_string();

    let workload = std::iter::repeat_with(move || {
        Msg::Request(Request {
            client_addr: client_addr.clone(),
            key: 100000,
            value: 10000001,
            slot_id: 100,
        })
    });

    BehaviourBuilder::new(Processor, BincodeCodec::default())
        .send(Sender { server })
        .generator_if(true, || workload)
        .on_send_failure(SendErrAction::Drop)
        .build()
        .run(ctx)
        .await
        .unwrap();
}
