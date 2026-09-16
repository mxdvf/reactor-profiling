use bincode::{Decode, Encode};
use reactor_macros::{DefaultPrio, Msg as DeriveMsg};

#[derive(Encode, Decode, Debug, Clone)]
pub struct Request {
    pub key: u64,
    pub value: u64,
    pub slot_id: usize,
}

impl Default for Request {
    fn default() -> Self {
        Self {
            key: 123456789,
            value: 987654321,
            slot_id: 42,
        }
    }
}

#[derive(Encode, Decode, Debug, Clone)]
pub struct Response {
    pub slot_id: usize,
}

impl Default for Response {
    fn default() -> Self {
        Self { slot_id: 42 }
    }
}

#[derive(Encode, Decode, Debug, Clone, DefaultPrio, DeriveMsg)]
pub enum Msg {
    Request(Request),
    Response(Response),
}

impl Default for Msg {
    fn default() -> Self {
        Msg::Request(Request::default())
    }
}
