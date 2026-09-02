use bincode::{Decode, Encode};
use reactor_macros::{DefaultPrio, Msg as DeriveMsg};

pub const PAYLOAD_SIZE: usize = 64;

#[derive(Encode, Decode, Debug, Clone)]
pub struct Request {
    pub client_addr: String,
    pub payload: [u8; PAYLOAD_SIZE],
}

#[derive(Encode, Decode, Debug, Clone)]
pub struct Response {
    pub client_addr: String,
    pub payload: [u8; PAYLOAD_SIZE],
}

#[derive(Encode, Decode, Debug, Clone, DefaultPrio, DeriveMsg)]
pub enum Msg {
    Request(Request),
    Response(Response),
}
