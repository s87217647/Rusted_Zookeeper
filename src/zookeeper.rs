use crate::node::{Message, MessageType, Node, NodeStatus, Transaction, TxActions};
use futures::future::join_all;
use rand::random;
use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Mutex};
use tokio::task;
use tokio::task::JoinHandle;
use tokio::time::sleep;

pub struct Zookeeper {
    size: i32,
    pub servers: HashMap<i32, Arc<Mutex<Node>>>,
    pub sender: broadcast::Sender<Message>,
    pub receiver: broadcast::Receiver<Message>,
    pub latest_leader: i32,
}

impl Zookeeper {
    pub fn start_servers(&mut self) -> Vec<JoinHandle<()>> {
        println!("Starting zookeeper");
        let mut handlers = vec![];

        for (id, node) in self.servers.iter_mut() {
            let handler = task::spawn(Node::run(node.clone()));
            handlers.push(handler);
        }
        handlers
    }

    pub fn new_transaction(&mut self, key: String, val: String) {
        let mut tx = Transaction::new();
        tx.action = TxActions::New;
        tx.key = key;
        tx.val = val;

        let mut update = Message::new(-2, MessageType::Write);
        update.receiver_id = self.latest_leader;
        update.tx = tx;

        self.sender.send(update).unwrap();
    }

    pub async fn crash_leader(&mut self) {
        println!("leader {} is gonna crash", self.latest_leader);
        let mut leader = self.servers[&self.latest_leader].lock().await;
        leader.status = NodeStatus::Following;

        while !leader.history.is_empty() {
            leader.history.pop();
        }
        Node::node_report(&leader);

        sleep(Duration::from_secs(20)).await;
    }

    async fn report(&mut self) {
        // first acquire all the locks so it can report at once
        let mut locks = Vec::new();

        for (k, v) in self.servers.iter() {
            locks.push(v.lock().await);
        }

        for n in locks.iter() {
            Node::node_report(&n);
        }
    }

    pub async fn quit(&mut self) {
        let mut locks = Vec::new();

        for (k, v) in self.servers.iter() {
            locks.push(v.lock().await);
        }
        // should save all nodes

        println!("saving & quiting zookeeper");
        sleep(Duration::from_secs(3)).await;
        let mut msg = Message::new(-2, MessageType::Quit);
        msg.receiver_id = -1;
        self.sender.send(msg).unwrap();
    }

    pub async fn find_latest_leader(&mut self) {
        while !self.receiver.is_empty() {
            let msg = self.receiver.recv().await.unwrap();
            if msg.msg_type == MessageType::Heartbeat {
                self.latest_leader = msg.sender_id;
            }
        }
    }

    pub async fn run(&mut self) {
        let handlers = self.start_servers();

        // All nodes id starts from 0, receiver's id = -1 is for broadcast to all, sender == -2 is from Zookeeper

        let prompt = String::from("\n\
            *************\n\
            enter user input\n\
            -n, [Key], [Val] for new Value\n\
            -d, [Key] for delete;\n\
            -c to crash the leader\n\
            -r to report on all nodes\n\
            -q to quit Zookeeper\n\
            ******************\n");

        loop {
            self.find_latest_leader().await;

            println!("{}", prompt);
            io::stdout().flush().unwrap();
            let mut user_input = String::new();
            io::stdin().read_line(&mut user_input).expect("Failed to read line");
            let parts: Vec<_> = user_input.trim().split(" ").collect();

            match parts[0] {
                // e.g. -n new_key new_val
                "-n" => {
                    self.new_transaction(parts[1].to_string(), parts[2].to_string())
                }

                "-c" => {
                    self.crash_leader().await;
                }

                "-r" => {
                    self.report().await;
                }

                "-q" => {
                    self.quit().await;
                    break;
                }

                _ => {
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
        let (sender, receiver) = broadcast::channel((size * size * size) as usize);


        for i in 0..size {
            // let mut unique_id = random::<i32>();
            let mut unique_id = i;
            while servers.contains_key(&unique_id) { unique_id = random::<i32>(); }

            let node = Node::new(unique_id, sender.clone(), sender.subscribe(), size);
            servers.insert(unique_id, Arc::new(Mutex::new(node)));
        }

        Zookeeper { size, servers, sender, receiver, latest_leader: -5 }
    }
}

