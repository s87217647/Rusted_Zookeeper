use std::cell::RefCell;
use std::io::{self, Write};
use std::rc::{Rc, Weak};
use std::sync::{mpsc, Arc};
use std::sync::mpsc::Receiver;
use tokio::time::sleep;
use tokio::sync::{broadcast, Mutex};
use tokio::sync::broadcast::Sender;
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
    pub sender: broadcast::Sender<String>,
    pub receiver: broadcast::Receiver<String>,
}

impl Node{
    pub fn new(id: i32, sender: broadcast::Sender<String>, receiver: broadcast::Receiver<String>) -> Node{
        Node{id, sender, receiver}
    }
    pub fn get_id(&self) -> i32{self.id}

    pub async fn send(&self, msg:String){
        if let Err(e) = self.sender.send(msg) {
            eprintln!("Node {} failed to broadcast: {}", self.id, e);
        }
    }
    pub async fn listen(& mut self) {
        let mut n = self;

        println!("Node {} listening", n.id);
        while let Ok(msg) = n.receiver.recv().await {
            println!("Node {} Received: {:?}", n.id, msg);
        }
    }

    pub async fn listen2(n: Arc<Mutex<Node>>){

        let mut n = n.lock().await;
        println!("id: {} form Listen2", n.id);

        while let Ok(msg) = n.receiver.recv().await {
            println!("Node {} Received: {:?}", n.id, msg);
        }



        //------




        // println!("Node {} listening", n.id);
        // while let Ok(msg) = n.receiver.recv().await {
        //     println!("Node {} Received: {:?}", n.id, msg);
        // }

    }

    // pub async fn listen3(self, mut receiver: broadcast::Receiver<String>) {
    //
    //     loop {
    //         let msg =  receiver.recv();
    //         if msg.is_err(){ break;}
    //         println!("Id is {}, Received a message: {:?}", self.id, msg.unwrap());
    //     }
    //
    // }

}