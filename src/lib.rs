mod client;
mod common;
mod server;

use crate::client::client as client_behaviour;
use crate::server::server as server_behaviour;
use reactor_actor::RuntimeCtx;
pub use reactor_actor::setup_shared_logger_ref;
use std::collections::HashMap;

lazy_static::lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

#[unsafe(no_mangle)]
fn client(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let server = payload
        .remove("server")
        .expect("missing server")
        .as_str()
        .expect("server must be a string")
        .to_string();

    RUNTIME.spawn(client_behaviour(ctx, server, payload));
}

#[unsafe(no_mangle)]
fn server(ctx: RuntimeCtx, _payload: HashMap<String, serde_json::Value>) {
    RUNTIME.spawn(server_behaviour(ctx));
}
