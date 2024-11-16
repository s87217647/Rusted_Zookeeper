use std::cell::RefCell;
use crate::node::{Node, PeerMessage};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::mpsc;
use std::thread::spawn;
use rand::random;
use tokio::task;

pub struct Zookeeper {
    size: i32,
    servers: HashMap<i32, Node>
}

impl Zookeeper{
    pub async fn run(zk: Rc<RefCell<Zookeeper>>){
        let mut zk_binding = zk.borrow_mut();
        let handler = task::spawn(async move{

            let (sender_channel, receiver_channel) = mpsc::channel();

            for (id, n) in zk_binding.servers.iter_mut(){
                n.run(&receiver_channel).await
            }
        });

    }

}


impl Zookeeper {
    pub fn new(size: i32) -> Rc<RefCell<Zookeeper>> {
        let mut zk = Rc::new(RefCell::new (Zookeeper {size, servers: HashMap::new()}));
        let mut servers = HashMap::new();

        for i in 0..size {

            let mut unique_id = random::<i32>();
            while servers.contains_key(&unique_id) {unique_id = random::<i32>();}

            let node = Node::new(unique_id, Rc::downgrade(&zk));
            servers.insert(unique_id, node);
        }


        zk.borrow_mut().servers = servers;
        zk
    }
}

