use crate::node::{Node, Message, MessageType, Transaction, TxActions, NodeStatus};
use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::sync::{Arc};
use std::time::Duration;
use rand::random;
use tokio::time::{sleep};
use tokio::task;
use tokio::sync::{broadcast, Mutex};
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

        let mut latest_leader = -5; // not -1

        loop{
            while !self.receiver.is_empty(){
                let msg = self.receiver.recv().await.unwrap();
                if msg.msg_type == MessageType::Heartbeat{
                    latest_leader = msg.sender_id;
                }
            }

            // loop for human interaction
            println!("\n\
            *************\n\
            enter user input\n\
            -d, [Key] for delete;\n\
            -n, [Key], [Val] for new Value\n\
            -c to crash the leader\n\
            -r to report on all nodes\n\
            ******************\n\
            ");

            io::stdout().flush().unwrap(); // Ensure prompt is shown immediately

            // Read input from stdin
            let mut input = String::new();
            io::stdin().read_line(&mut input).expect("Failed to read line");

            // Trim the input to remove any extra whitespace
            let input = input.trim();
            let parts = input.split(" ").collect::<Vec<&str>>();

            match parts[0]{
                "-n" =>{
                    // e.g. -n new_key new_val
                    let mut tx = Transaction::new();
                    tx.action = TxActions::New;
                    tx.key = parts[1].to_string();
                    tx.val = parts[2].to_string();

                    let mut update = Message::new(-2, MessageType::Write);
                    update.receiver_id = latest_leader;
                    update.tx = tx;

                    self.sender.send(update).unwrap();
                }

                "-c" =>{
                    //to do, how to

                    let mut leader = self.servers[&latest_leader].lock().await;
                    leader.status = NodeStatus::Following;

                    while !leader.history.is_empty() {
                        leader.history.pop();
                    }
                    Node::node_report(&leader);

                    sleep(Duration::from_secs(20)).await;


                }


                "-r" =>{
                    // first acquire all the locks so it can report at once
                    let mut locks = Vec::new();

                    for (k, v) in self.servers.iter(){
                        locks.push(v.lock().await);
                    }

                    for n in locks.iter(){
                        Node::node_report( &n);
                    }

                }


                _ =>{
                    println!("invalid command")
                }
            }


        }
        join_all(handlers).await;

    }

}


impl Zookeeper {
    pub fn new(size: i32) -> Zookeeper {
        let mut servers = HashMap::new();
        let(sender, receiver) = broadcast::channel((size * size * size) as usize);


        for i in 0..size {
            // let mut unique_id = random::<i32>();
            let mut unique_id = i;
            while servers.contains_key(&unique_id) {unique_id = random::<i32>();}

            let node = Node::new(unique_id, sender.clone(), sender.subscribe(),size);
            servers.insert(unique_id, Arc:: new(Mutex::new(node)));
        }

        Zookeeper{size, servers, sender, receiver}
    }
}

