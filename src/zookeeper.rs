use std::cell::RefCell;
use crate::node::{Node, PeerMessage};
use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::rc::Rc;
use std::sync::{mpsc, Arc};
use std::sync::mpsc::Receiver;
use std::thread::spawn;
use rand::random;
use tokio::task;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use futures::future::join_all;


pub struct Zookeeper {
    size: i32,
    servers: HashMap<i32, Arc<Mutex<Node>>>,
    pub sender: broadcast::Sender<String>,
    pub receiver: broadcast::Receiver<String>
}

impl Zookeeper{
    pub async fn run(&mut self) {
        // let mut handles = HashMap::new();
        // let mut handles: Vec<_> = vec![];



        // for (id, mut n) in self.servers{
        //     task::spawn(Node::listen(self.servers[&0]));

        // }

        // for (id, server) in &mut self.servers {
        //     // let mut server_clone = Node{id: server.id, sender: server.sender.clone(), receiver: server.sender.subscribe() };
        //     let handle = task::spawn(async move {
        //         Node::listen2(server.clone()).await;
        //     });
        //     handles.push(handle);
        // }

        let keys = self.servers.keys().cloned().collect::<Vec<_>>();

        let mut server = self.servers.get(&keys[0]).unwrap();
        let mut server2 = self.servers.get(&keys[1]).unwrap();

        println!("Starting zookeeper");


        let handle1 = task::spawn(Node::listen2(server.clone()));
        let handle2 = task::spawn(Node::listen2(server2.clone()));

        // task::spawn(Node::listen2(server.clone())).await;
        // task::spawn(Node::listen2(server.clone())).await;

        loop{
            // print!("> ");
            io::stdout().flush().unwrap(); // Ensure prompt is shown immediately

            // Read input from stdin
            let mut input = String::new();
            io::stdin().read_line(&mut input).expect("Failed to read line");

            // Trim the input to remove any extra whitespace
            let input = input.trim();
            println!("Sending: {}", input);
            self.sender.send(input.to_string()).unwrap();

        }
        handle1.await.unwrap();




    }

}


impl Zookeeper {
    pub fn new(size: i32) -> Zookeeper {

        let mut servers = HashMap::new();
        let(sender, receiver) = broadcast::channel((2* size + 1) as usize);


        for i in 0..size {

            let mut unique_id = random::<i32>();
            while servers.contains_key(&unique_id) {unique_id = random::<i32>();}

            let node = Node::new(unique_id, sender.clone(), sender.subscribe());
            servers.insert(unique_id, Arc:: new(Mutex::new(node)));
        }

        Zookeeper{size, servers, sender, receiver}
    }
}

