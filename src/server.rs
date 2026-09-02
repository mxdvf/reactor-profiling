use crate::common::{Msg, Response};
use reactor_actor::codec::BincodeCodec;
use reactor_actor::{BehaviourBuilder, RouteTo, RuntimeCtx};

struct Processor;

impl reactor_actor::ActorProcess for Processor {
    type IMsg = Msg;
    type OMsg = Msg;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        match input {
            Msg::Request(request) => vec![Msg::Response(Response {
                client_addr: request.client_addr,
            })],

            Msg::Response(_) => {
                panic!("Server received a response message")
            }
        }
    }
}

struct Sender;

impl reactor_actor::ActorSend for Sender {
    type OMsg = Msg;

    async fn before_send<'a>(&'a mut self, output: &Self::OMsg) -> RouteTo<'a> {
        match output {
            Msg::Response(response) => RouteTo::from(response.client_addr.clone()),

            Msg::Request(_) => {
                panic!("Server tried to send a request message")
            }
        }
    }
}

pub async fn server(ctx: RuntimeCtx) {
    BehaviourBuilder::new(Processor, BincodeCodec::default())
        .send(Sender)
        .build()
        .run(ctx)
        .await
        .unwrap();
}
