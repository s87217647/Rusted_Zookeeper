use std::cell::RefCell;
use std::io::{self, Write};
use std::rc::{Rc, Weak};
use std::sync::mpsc;
use tokio::time::sleep;
use crate::zookeeper::Zookeeper;

#[derive(Debug)]
pub(crate) enum PeerMessage {
    Heartbeat,
    Election,
    Custom(String),
}



//////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////

pub(crate) struct Node{
    pub id: i32,
    pub zk: Weak<RefCell<Zookeeper>>
}

impl Node{
    pub fn new(id: i32, zk:  Weak<RefCell<Zookeeper>>) -> Node{
        Node{id, zk}
    }
    pub fn get_id(&self) -> i32{self.id}

    pub async fn run(&self, mut receiver: &mpsc::Receiver<String>) {

        loop {
            println!("Looping");
            let msg =  receiver.recv();
            if msg.is_err(){ break;}
            println!("Id is {}, Received a message: {:?}", self.id, msg.unwrap());
        }

    }


}