use std::cell::RefCell;
use crate::node::{Node, Message};
use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::rc::Rc;
use std::sync::{mpsc, Arc};
use std::sync::mpsc::Receiver;
use rand::random;
use tokio::task;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use futures::future::join_all;


pub struct Zookeeper {
    size: i32,
    servers: HashMap<i32, Arc<Mutex<Node>>>,
    pub sender: broadcast::Sender<Message>,
    pub receiver: broadcast::Receiver<Message>
}

impl Zookeeper{
    pub async fn run(&mut self) {

        println!("Starting zookeeper");
        let mut handlers = vec![];

        for (id, node) in self.servers.iter_mut(){
            let handler = task::spawn(Node::run(node.clone()));
            handlers.push(handler);
        }

        loop{
            // loop for human interaction
            print!("> ");
            io::stdout().flush().unwrap(); // Ensure prompt is shown immediately

            // Read input from stdin
            let mut input = String::new();
            io::stdin().read_line(&mut input).expect("Failed to read line");

            // Trim the input to remove any extra whitespace
            let input = input.trim();
            println!("Sending: {}", input);


        }
        join_all(handlers).await;

    }

}


impl Zookeeper {
    pub fn new(size: i32) -> Zookeeper {

        let mut servers = HashMap::new();
        let(sender, receiver) = broadcast::channel((2 * size + 1) as usize);


        for i in 0..size {

            let mut unique_id = random::<i32>();
            while servers.contains_key(&unique_id) {unique_id = random::<i32>();}

            let node = Node::new(unique_id, sender.clone(), sender.subscribe(),size);
            servers.insert(unique_id, Arc:: new(Mutex::new(node)));
        }

        Zookeeper{size, servers, sender, receiver}
    }
}

