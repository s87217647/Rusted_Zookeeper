use std::cell::RefCell;
use std::io;
use std::io::Write;
use std::sync::{mpsc, Arc};
use std::time::Duration;
use tokio::task;
use tokio::task::spawn;
use tokio::time::sleep;
use tokio::sync::broadcast;
use tokio::task::id;
use crate::node::{Node, PeerMessage};
use crate::zookeeper::{Zookeeper};
// mod Zookeeper;
mod node;
mod zookeeper;

#[tokio::main]
async fn main() {
    // multi server is working, now, have a new message channel
    // mpsc - multi sending, single consume
    // get a zk running, let's send some msg ;
    let mut zk = Zookeeper::new(5);
    zk.run().await;
    // let n = Node::new(1, zk.sender.clone(), zk.sender.subscribe());
    // task::spawn(n.listen());



}


#[cfg(test)]
mod tests {
    use crate::node::Node;

    #[test]
    fn hello_test(){
        println!("hello test");
    }



    #[test]
    async fn multi_servers(){
        // 3 servers running and responding at the same time, I call that a win


    }

}
