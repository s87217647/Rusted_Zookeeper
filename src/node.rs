use std::process::id;
use std::cell::{Ref, RefCell};
use std::collections::HashSet;
use std::io::{self, Write};
use std::net::UdpSocket;
use std::rc::{Rc, Weak};
use std::sync::{mpsc, Arc, MutexGuard};
use std::sync::mpsc::Receiver;
use std::time::Duration;
use rand::{random, Rng};
use tokio::time::{sleep, Sleep};
use tokio::sync::{broadcast, Mutex};
use tokio::task;
use tokio::sync::broadcast::Sender;
use crate::node::NodeStatus::{Candidate, Following};


#[derive(Clone, Debug, Eq, PartialEq)]
enum NodeStatus {
    Following, Leading, Candidate, Voting,
    Sync, Discovery, Broadcast
}

pub(crate) struct Node{
    pub id: i32,
    pub leader_id: core::option::Option<i32>,
    pub status:  NodeStatus,
    pub sender: broadcast::Sender<Message>,
    pub receiver: broadcast::Receiver<Message>,
    pub epoch: i32,
    pub msg_buffer: Vec<Message>,
    pub log: Vec<Transaction>,
    pub cluster_size: i32
}


pub struct Transaction{
    pub zxid:i32,
}

#[derive (Clone, Debug)]
pub struct Message{
    sender_id: i32,
    msg_type: MessageType,
    last_zxid: i32,
    epoch: i32,
    vote_to: i32

}
#[derive(Clone, Debug, Eq, PartialEq)]
enum MessageType {
    Ping,
    Alive,
    Candidacy,
    Vote,
    Won

}

impl Message {
    pub fn new(sender_id: i32, msg_type: MessageType) -> Self {
        Message{sender_id, msg_type, last_zxid: 0, epoch: 0, vote_to:0}
    }
}


impl Node{
    pub fn new(id: i32, sender: broadcast::Sender<Message>, receiver: broadcast::Receiver<Message>, cluster_size:i32) -> Node{
        Node{id, leader_id: None, status: Following, sender, receiver, epoch:0, msg_buffer: Vec::new(), log: Vec::new(), cluster_size}
    }



    async fn broadcast(node: Arc<Mutex<Node>>, msg: Message) {
        let random_delay = rand::thread_rng().gen_range( 200 .. 800);
        sleep(Duration::from_millis(random_delay)).await;

        println!("from {}, message type {:?}", msg.sender_id, msg.msg_type );
        node.lock().await.sender.send(msg).expect("TODO: panic message");
    }


    async fn listen(node: Arc<Mutex<Node>>){
        loop{
            sleep(Duration::from_millis(10)).await;
            let mut n = node.lock().await;
            while !n.receiver.is_empty(){
                let Ok(msg) = n.receiver.recv().await else{ panic!("Error receiving message")};
                // println!("Node {} received msg: {}", n.id, &msg);
                n.msg_buffer.push(msg);
            }
        }
    }

    fn last_zxid(n: &Node) -> i32{
        let len = n.log.len();

        if len == 0{
            return 0;
        }

        n.log[len-1].zxid
    }

    async fn active_leader_exists(node: Arc<Mutex<Node>>) -> bool{
        //How to know if there is an active leader?
        // send out "Alive?" and check buffer if within certain time, there is an "Alive!" msg from the leader?

        // Need to know the time, the time, so gotta have a time stamp?

        // just be careful and figure out the thread thing. could this func block the listener?

        if node.lock().await.leader_id.is_none(){ return false}


        let mut book_mark = 0;
        let mut n = node.lock().await;
        if !n.msg_buffer.is_empty(){book_mark  = n.msg_buffer.len() - 1;}

        let heartbeat_out_msg = Message::new(n.id, MessageType::Ping);
        sleep(Duration::from_secs(1)).await;
        println!("{} sending out ping", n.id);
        drop(n);

        Node::broadcast(node.clone(), heartbeat_out_msg).await;

        let mut n = node.lock().await;
        for msg in &n.msg_buffer[book_mark..]{
            if msg.sender_id == n.leader_id.unwrap(){
                return true;
            }
        }
        drop(n);


        false
    }

    // by passing n as a reference, this func keep the lock, blocking the listen
    async fn run_election(node: Arc<Mutex<Node>>){

        //Todo:: Race condition still exist, how to avoid multiple candidate running at the same time?

        node.lock().await.status = NodeStatus::Candidate;

        // becoming_candidate_msg
        let mut book_mark = 0;
        if !node.lock().await.msg_buffer.is_empty(){book_mark  = node.lock().await.msg_buffer.len() - 1;}

        let intend_to_lead = Message::new(node.lock().await.id, MessageType::Candidacy);
        Node::broadcast(node.clone(), intend_to_lead).await;

        let n = node.lock().await;
        sleep(Duration::from_secs(3)).await;


        let mut supporting_nodes = HashSet::new();

        supporting_nodes.insert(node.lock().await.id);


        for msg in &node.lock().await.msg_buffer[book_mark..] {
            if msg.msg_type == MessageType::Vote && msg.vote_to == node.lock().await.id {
                supporting_nodes.insert(msg.sender_id);
            }
        }

        let quorum_size = (node.lock().await.cluster_size + 1) / 2;
        if supporting_nodes.len() >= quorum_size as usize {
            let mut n = node.lock().await;
            // won the election size
            n.status = NodeStatus::Leading;
            let winning_news = Message:: new(n.id, MessageType::Won);
            println!("{} won", n.id);
        }

    }

    async fn buffer_processing(node: Arc<Mutex<Node>>){
        let random_delay = rand::thread_rng().gen_range( 500 .. 800);
        sleep(Duration::from_millis(random_delay)).await;

        loop {
            sleep(Duration::from_secs(2)).await;
            let mut n = node.lock().await;

            // Election
            // Types of msg send out looking msg, other node will send vote

            while !n.msg_buffer.is_empty() {
                println!("{} is processing buffer", n.id);
                let msg = n.msg_buffer.pop().unwrap();
                // msg is from this node, ignore and go to the next round
                if msg.sender_id == n.id { continue }


                match msg.msg_type {
                    MessageType::Ping => {
                        println!("{} is alive", n.id);
                        let alive_msg = Message::new(n.id, MessageType::Alive);
                        Node::broadcast(node.clone(), alive_msg).await;
                    }

                    MessageType::Candidacy =>{
                        if msg.last_zxid > Node::last_zxid(&n){
                            n.status = NodeStatus::Voting;
                            let mut vote = Message::new(n.id, MessageType::Vote);
                            vote.vote_to = msg.sender_id;
                            println!("{} voted for {}", vote.sender_id, vote.vote_to);
                            Node::broadcast(node.clone(), vote).await;
                        }else{
                            Node::run_election(node.clone()).await;
                        }
                    }

                    MessageType:: Won =>{
                        //objectively better
                        if msg.last_zxid > Node::last_zxid(&n){
                            // becomes the follower
                            n.status = NodeStatus::Following;
                            n.leader_id = Some(msg.sender_id);
                        } else if msg.last_zxid == Node::last_zxid(&n) && msg.sender_id > n.id{
                            // greater id number wins
                            n.status = NodeStatus::Following;
                            n.leader_id = Some(msg.sender_id);
                        }

                    }

                    _ =>{
                        println!("{:?} message_type's answer is not implemented", msg.msg_type);
                    }
                }

            }
            drop(n);

            if !Node::active_leader_exists(node.clone()).await && node.lock().await.status == Following {
                // No active leader, go into leader election till found a leader or becomes a leader
                // send out candidate message and becomes a candidate
                println!("{} has no active leader", node.lock().await.id);
                Node::run_election(node.clone()).await;

            }
        }


    }

    pub async fn run(node: Arc<Mutex<Node>>){
        let listen_handler = task::spawn(Node::listen(node.clone()));
        let processing_handler = task::spawn(Node:: buffer_processing(node.clone()));

        listen_handler.await.unwrap();
        processing_handler.await.unwrap();
    }
}
