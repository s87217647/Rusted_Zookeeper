use alloc::fmt::format;
use std::alloc::System;
use std::arch::is_aarch64_feature_detected;
use std::process::id;
use std::cell::{Ref, RefCell};
use std::collections::HashSet;
use std::f32::consts::LOG2_10;
use std::hint::spin_loop;
use std::io::{self, Write};
use std::net::UdpSocket;
use std::ops::Add;
use std::rc::{Rc, Weak};
use std::sync::{mpsc, Arc, MutexGuard};
use std::sync::mpsc::Receiver;
use std::time::Duration;
use rand::{random, Rng};
use tokio::time::{sleep, Instant};
use tokio::sync::{broadcast, Mutex};
use tokio::task;
use tokio::sync::broadcast::Sender;
use crate::node::MessageType::{AckTX, Running, SyncRequest};
use crate::node::NodeStatus::Following;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeStatus {
    //Leaderless, following represent following + alive_leader flag
    Following, Leading, Running,
    // Sync, Discovery, Broadcast
}

pub(crate) struct Node{
    pub id: i32,
    pub status:  NodeStatus,
    pub sender: broadcast::Sender<Message>,
    pub receiver: broadcast::Receiver<Message>,
    pub leader_id: core::option::Option<i32>,
    pub epoch: i32,
    pub msg_buffer: Vec<Message>,
    pub history: Vec<Transaction>, //aka history
    pub cluster_size: i32,
    pub last_alive_from_leader: Instant,
    pub supporting_nodes : HashSet<i32>,
    // don't need a map to represent data, coz, technically, having
}


#[derive (Clone, Debug)]
pub enum TxActions{
    NA,
    Del, New
}

#[derive (Clone, Debug)]
pub struct Transaction{
    pub zxid:i32,
    pub action: TxActions, // delete/ create / update
    pub key: String,
    pub val: String
}

impl Transaction{
    pub fn new() -> Self{
        Transaction{zxid: -1, action: TxActions::NA, key: String::new(), val: String::new()}
    }
}


#[derive (Clone, Debug)]
pub struct Message{
    pub msg_type: MessageType,
    pub sender_id: i32,
    last_zxid: i32,
    epoch: i32,
    pub receiver_id: i32, // only used in voting, msg is broadcast by default
    pub tx:Transaction,
    history: Vec<Transaction>

}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MessageType {
    Blank, // (Default placeholder)
    Heartbeat,
    Running, Approve, Reject,
    Recovery, SyncRequest, Sync,
    Write, AckTX, Commit

}

impl Message {
    pub fn new(sender_id: i32, msg_type: MessageType) -> Self {
        Message{sender_id, msg_type, last_zxid: 0, epoch: 0, receiver_id: -1, tx: Transaction::new(), history: Vec::new()} // -1 stands for broadcast
    }

    pub fn message_report(&self){
        println!("from {}, to {}, {:?}", self.sender_id, self.receiver_id, self.msg_type);
    }
}


impl Node{
    pub fn new(id: i32, sender: broadcast::Sender<Message>, receiver: broadcast::Receiver<Message>, cluster_size:i32) -> Node{
        Node{id, leader_id: None, status: NodeStatus::Following, sender, receiver,
            epoch:0, msg_buffer: Vec::new(), history: Vec::new(),
            cluster_size, last_alive_from_leader: Instant::now(), supporting_nodes: HashSet::new()}
    }


    async fn broadcast(node: &Node, msg: Message) {
        let random_delay = rand::thread_rng().gen_range( 200 .. 800);
        sleep(Duration::from_millis(random_delay)).await;

        node.sender.send(msg).unwrap();
    }


    async fn listen(node: Arc<Mutex<Node>>){
        loop{
            sleep(Duration::from_millis(100)).await;
            let mut n = node.lock().await;
            while !n.receiver.is_empty(){
                let Ok(msg) = n.receiver.recv().await else{ panic!("Error receiving message")};
                // println!("Node {} received msg: {}", n.id, &msg);
                n.msg_buffer.push(msg);
            }
        }
    }

    fn last_zxid(n: &Node) -> i32{
        let len = n.history.len();

        if len == 0{
            return 0;
        }

        n.history[len-1].zxid
    }


    async fn follow_and_discovery(n: &mut Node, msg: &Message){
        // This is where discovery begin, establish new connection
        n.leader_id = Some(msg.sender_id);
        n.last_alive_from_leader = Instant::now();
        n.status = Following;
        n.epoch = msg.epoch;
        // Broadcast all it's history
        for tx in n.history.iter(){
            let mut history = Message::new(n.id, MessageType::Recovery);
            history.receiver_id = n.leader_id.unwrap();
            history.tx = tx.clone();
            Node:: broadcast(&n, history).await;
        }
        sleep(Duration::from_secs(2)).await;

        let mut request_sync = Message::new(n.id, MessageType::SyncRequest);
        request_sync.receiver_id = msg.sender_id;
        Node::broadcast(&n, request_sync).await;
        // request a sync from leader

    }

    async fn give_up_election(n: &mut Node, msg: &Message){
        println!("{} give up election", n.id);
        // Node::node_report(n);
        // msg.message_report();

        n.status = Following;
        n.leader_id = None;
        n.last_alive_from_leader = Instant::now();
        n.supporting_nodes = HashSet::new();
    }

    pub async fn execute_tx(n: &mut Node){

    }

    pub fn node_report(n: &Node){


        let mut report;
        if n.leader_id == None{
            report = format!("id: {}, status: {:?}, leader: None epoch {}, ", n.id, n.status, n.epoch);
        }else {
            report = format!("id: {}, status: {:?}, leader: {} epoch {}, ", n.id, n.status, n.leader_id.unwrap(), n.epoch);
        }


        let transactions = String::new();

        for i in 0..n.history.len(){
            let tx = &n.history[i];
            let tx_report = &format!(" |tx idx: {}, zxid: {}, action: {:?}, key: {}, val: {} |" , i, tx.zxid, tx.action, tx.key, tx.val);
            report = report + tx_report;
        }
        println!("{}", report.to_string());
        io::stdout().flush().unwrap(); // Ensure prompt is shown immediately
    }

    async fn answering(node: Arc<Mutex<Node>>){
        loop {
            sleep(Duration::from_millis(10)).await;

            let mut n = node.lock().await; // Acquire the lock here
            while !n.receiver.is_empty() {
                let msg= n.receiver.recv().await.unwrap();
                if msg.sender_id == n.id { continue }
                // -1 stands for broadcast to all, -2 stands for zookeeper
                if msg.receiver_id != -1 && msg.receiver_id != n.id{ continue }



                // println!("id {} , status {:?}, leader {:?}, epoch {}, ",n.id, n.status, n.leader_id, n.epoch);

                match msg.msg_type {
                    MessageType:: Commit =>{
                        if n.status == Following || n.leader_id != None {continue}

                        if n.leader_id.unwrap() == msg.sender_id && msg.epoch == n.epoch && msg.last_zxid == Node::last_zxid(&n){
                            Node::execute_tx(&mut n).await;
                        }


                    }


                    MessageType::AckTX =>{
                        if n.status != NodeStatus::Leading {continue}
                        if msg.last_zxid == Node::last_zxid(&n) && msg.epoch == n.epoch{
                            n.supporting_nodes.insert(msg.sender_id);
                        }

                        if n.supporting_nodes.len() as i32 > (n.cluster_size + 1) / 2 {
                            let mut commit = Message::new(n.id, MessageType::Commit);
                            commit.epoch = n.epoch;
                            commit.last_zxid = Node::last_zxid(&n);
                            Node::broadcast(&n, commit).await;
                            Node:: execute_tx(&mut n);
                        }

                    }


                    MessageType::Write =>{
                        if n.status ==  NodeStatus::Leading{
                            //should only come from client
                            let mut new_tx = msg.tx.clone();
                            new_tx.zxid = Node::last_zxid(&n) + 1;
                            n.history.push(new_tx.clone());

                            let mut new_write = Message::new(n.id, MessageType::Write);
                            new_write.tx = new_tx;
                            new_write.epoch = n.epoch;
                            Node::broadcast(&n, new_write).await;
                        }
                        if n.status == Following && n.leader_id != None && n.leader_id.unwrap() == msg.sender_id{
                            n.history.push(msg.tx);

                            let mut ack = Message:: new(n.id, AckTX);
                            ack.receiver_id = n.leader_id.unwrap();
                            ack.last_zxid = Node::last_zxid(&n);
                            ack.epoch = msg.epoch;
                        }

                    }


                    MessageType::Sync =>{
                        if n.status != NodeStatus::Following && n.leader_id !=None && n.leader_id.unwrap() == msg.sender_id{continue}
                        n.history = msg.history.clone();
                    }

                    MessageType::SyncRequest => {
                        if n.status != NodeStatus::Leading{ continue }
                        let mut sync_msg = Message::new(msg.sender_id, MessageType::Sync);
                        sync_msg.history = n.history.clone();
                        Node::broadcast(&n, sync_msg).await;
                    }


                    MessageType::Recovery =>{
                        if n.status != NodeStatus::Leading{ continue }
                        let follower_history = msg.tx;
                        // history is sorted from low idx to hi -> lo to hi zxid
                        // go through history low to high, see if the gaps between two tx can fit in this one
                        if n.history.is_empty(){
                            n.history.push(follower_history.clone());
                        }else {
                            for i in 0 .. n.history.len() - 1{
                                let left = n.history[i].zxid;
                                let right = n.history[i + 1].zxid;
                                if left < follower_history.zxid && follower_history.zxid < right{
                                    n.history.insert(i + 1, follower_history.clone());
                                }

                            }

                        }




                    }


                    MessageType::Heartbeat =>{
                        // receiver can decide if they wanna follow msg sender as leader
                        // println!("msg from {} to {}, {:?}", msg.sender_id, msg.receiver_id, msg.msg_type);

                        match n.status{
                            NodeStatus::Following =>{
                                if n.leader_id == None{
                                    if Node::sender_is_better_leader(&n, &msg){
                                        Node::follow_and_discovery(&mut n, &msg).await;
                                    }else{
                                        n.epoch = msg.epoch;
                                    }

                                }

                                if n.leader_id != None && n.leader_id.unwrap() == msg.sender_id{
                                    n.last_alive_from_leader = Instant::now();
                                    n.epoch = msg.epoch;
                                }
                            }

                            NodeStatus::Running =>{
                                if Node::sender_is_better_leader(&n, &msg) {
                                    Node::give_up_election(&mut n, &msg).await;
                                }
                            }



                            NodeStatus::Leading =>{
                                if Node::sender_is_better_leader(&n, &msg){
                                    Node::give_up_election(&mut n, &msg).await;
                                    Node::follow_and_discovery(&mut n, &msg).await;
                                }else{
                                    Node::starts_running(&mut n).await;
                                }
                            }

                            _ =>{
                                println!("Default case")
                            }

                        }
                    }

                    MessageType::Running => {
                        // another node is running
                        let mut answer = Message::new(n.id, MessageType::Blank);
                        answer.epoch = msg.epoch; // todo: bounce back the proposed epoch?
                        answer.last_zxid = Node::last_zxid(&n);
                        answer.receiver_id = msg.sender_id;

                        if Node::sender_is_better_leader(&n, &msg){
                            answer.msg_type = MessageType::Approve;

                        }else {
                            answer.msg_type = MessageType::Reject;
                        }

                        Node::broadcast(&n, answer).await;
                    }

                    MessageType::Approve =>{
                        if n.status != NodeStatus::Running{continue}
                        n.supporting_nodes.insert(msg.sender_id);

                        if n.supporting_nodes.len() as i32 > (n.cluster_size + 1) / 2{
                            //Starts leading and send out heartbeats
                            //Later heartbeat will be interpreted as winning msg
                            n.status = NodeStatus::Leading;
                            n.leader_id = None;
                            n.epoch += 1;
                            n.supporting_nodes = HashSet::new();
                        }

                    }

                    MessageType::Reject =>{
                        if n.status != NodeStatus::Running{continue}

                        if Node::sender_is_better_leader(&n, &msg){
                            Node::give_up_election(&mut n, &msg).await;
                        }else{
                            //adjust epoch, ready for next campaign
                            n.epoch = msg.epoch;

                        }


                    }

                    _ =>{
                        println!("message_type's {:?} answer is not implemented", msg.msg_type);
                    }
                }
            }
        }


    }

    fn sender_is_better_leader(n: &Node, msg: &Message) -> bool{
        //compared to current node, if the msg sender is a better leader
        // Does if sender is better depends on the state of this node as well?
        // eg. if  this node is at epoch 2, heartbeat coming from another have a epoch 3, same zxid, should find it agreeable, right?

        let zxid = Node::last_zxid(n);
        let mut epoch = n.epoch;

        if msg.msg_type == MessageType::Running{
            epoch += 1
        }

        if msg.epoch < epoch || msg.last_zxid < zxid{
            return false;
        }

        if msg.last_zxid == zxid && msg.epoch == epoch{ // tie
            return if msg.sender_id > n.id { true } else { false }
        }

        true
    }



    async fn heartbeat_emitter(node: Arc<Mutex<Node>>){
        loop{
            sleep(Duration::from_millis(20)).await;
            let n = node.lock().await;
            if n.status == NodeStatus::Leading{
                let mut heartbeat = Message::new(n.id, MessageType::Heartbeat);
                heartbeat.last_zxid = Node::last_zxid(&n);
                heartbeat.epoch = n.epoch;
                Node::broadcast(&n, heartbeat).await;
            }
        }
    }

    async fn starts_running(n: &mut Node){
        n.status = NodeStatus::Running;
        n.leader_id = None;

        n.supporting_nodes.insert(n.id);

        let mut begin_campaign = Message::new(n.id, MessageType::Running);
        begin_campaign.last_zxid = Node::last_zxid(&n);
        begin_campaign.epoch = n.epoch + 1;
        Node::node_report(n);
        Node::broadcast(&n, begin_campaign).await;

    }
    async fn heartbeat_monitor(node: Arc<Mutex<Node>>){
        loop{
            sleep(Duration::from_secs(1)).await;
            let mut n = node.lock().await;
            if n.status == NodeStatus::Following && n.last_alive_from_leader.elapsed() > Duration::from_secs(5){
                Node::starts_running(&mut n).await;
            }

        }

    }




    pub async fn run(node: Arc<Mutex<Node>>){
        // let listen_handler = task::spawn(Node::listen(node.clone()));
        let processing_handler = task::spawn(Node::answering(node.clone()));
        let emitter_handler = task::spawn(Node::heartbeat_emitter(node.clone()));
        let monitor_handler = task::spawn(Node::heartbeat_monitor(node.clone()));

        // how about run another background thread here monitoring heartbeat?
        // 2 kinds of behavior, acting, reacting


        // listen_handler.await.unwrap();
        processing_handler.await.unwrap();
        emitter_handler.await.unwrap();
        monitor_handler.await.unwrap();
    }
}
