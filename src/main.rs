use crate::zookeeper::{Zookeeper};
extern crate alloc;
mod node;
mod zookeeper;

#[tokio::main]
async fn main() {
    let mut zk = Zookeeper::new(10);
    zk.run().await;



}


