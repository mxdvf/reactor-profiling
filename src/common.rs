use bincode::{Decode, Encode};
use reactor_macros::{DefaultPrio, Msg as DeriveMsg};

#[derive(Encode, Decode, Debug, Clone)]
pub struct Request {
    pub client_addr: String,
}

#[derive(Encode, Decode, Debug, Clone)]
pub struct Response {
    pub client_addr: String,
}

#[derive(Encode, Decode, Debug, Clone, DefaultPrio, DeriveMsg)]
pub enum Msg {
    Request(Request),
    Response(Response),
}
