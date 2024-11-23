use std::arch::is_aarch64_feature_detected;
use std::process::id;
use std::cell::{Ref, RefCell};
use std::collections::HashSet;
use std::f32::consts::LOG2_10;
use std::hint::spin_loop;
use std::io::{self, Write};
use std::net::UdpSocket;
use std::rc::{Rc, Weak};
use std::sync::{mpsc, Arc, MutexGuard};
use std::sync::mpsc::Receiver;
use std::time::Duration;
use rand::{random, Rng};
use tokio::time::{sleep, Instant, Sleep};
use tokio::sync::{broadcast, Mutex};
use tokio::task;
use tokio::sync::broadcast::Sender;
use crate::node::MessageType::Running;
use crate::node::NodeStatus::Following;

#[derive(Clone, Debug, Eq, PartialEq)]
enum NodeStatus {
    //Leaderless, following represent following + alive_leader flag
    Following, Leading, Running,
    Sync, Discovery, Broadcast
}

pub(crate) struct Node{
    pub id: i32,
    pub status:  NodeStatus,
    pub sender: broadcast::Sender<Message>,
    pub receiver: broadcast::Receiver<Message>,
    pub leader_id: core::option::Option<i32>,
    pub epoch: i32,
    pub msg_buffer: Vec<Message>,
    pub log: Vec<Transaction>, //aka history
    pub cluster_size: i32,
    pub last_alive_from_leader: Instant,
    pub supporting_nodes : HashSet<i32>,
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
    receiver_id: i32, // only used in voting, msg is broadcast by default

}
#[derive(Clone, Debug, Eq, PartialEq)]
enum MessageType {
    Blank, // (Default placeholder)
    Heartbeat,
    Running,
    Approve,
    Reject, Won
}

impl Message {
    pub fn new(sender_id: i32, msg_type: MessageType) -> Self {
        Message{sender_id, msg_type, last_zxid: 0, epoch: 0, receiver_id: -1}
    }
}


impl Node{
    pub fn new(id: i32, sender: broadcast::Sender<Message>, receiver: broadcast::Receiver<Message>, cluster_size:i32) -> Node{
        Node{id, leader_id: None, status: NodeStatus::Following, sender, receiver,
            epoch:0, msg_buffer: Vec::new(), log: Vec::new(),
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
        let len = n.log.len();

        if len == 0{
            return 0;
        }

        n.log[len-1].zxid
    }

    async fn need_new_leader(node: Arc<Mutex<Node>>) -> bool{
        //following & No leader heartbeat
        if node.lock().await.status != NodeStatus::Following {return false}


        // if node.lock().await.leader_id == None{ return true}
        //
        //
        //
        // let mut n = node.lock().await;
        // let mut book_mark = 0;
        // if !n.msg_buffer.is_empty(){book_mark  = n.msg_buffer.len() - 1;}
        // let heartbeat_out_msg = Message::new(n.id, MessageType::);
        //
        // sleep(Duration::from_secs(1)).await;
        // println!("{} sending out ping", heartbeat_out_msg.sender_id);
        // Node::broadcast( &n, heartbeat_out_msg).await;
        //
        // for msg in &n.msg_buffer[book_mark..]{
        //     if msg.sender_id == n.leader_id.unwrap(){
        //         return true;
        //     }
        // }
        // drop(n);


        false
    }

    async fn answering(node: Arc<Mutex<Node>>){
        loop {
            sleep(Duration::from_millis(500)).await;

            let mut n = node.lock().await; // Acquire the lock here
            while !n.receiver.is_empty() {
                let msg= n.receiver.recv().await.unwrap();
                if msg.sender_id == n.id { continue }
                // println!("id {} , status {:?}, leader {:?}, epoch {}, ",n.id, n.status, n.leader_id, n.epoch);


                match msg.msg_type {
                    MessageType::Heartbeat =>{
                        // receiver can decide if they wanna follow msg sender as leader
                        // println!("msg from {} to {}, {:?}", msg.sender_id, msg.receiver_id, msg.msg_type);

                        // establish following
                        if Node::msg_sender_is_the_better_leader(&n, &msg)|| n.leader_id == None{
                            n.leader_id = Some(msg.sender_id);
                            n.last_alive_from_leader = Instant::now();
                            n.status = Following;
                            n.epoch = msg.epoch;
                            // Leader's Tx history = Follower's Tx history, or at least, fill up what is missing
                            // sync with leader tx

                            // discover should happen here.
                        }

                        // renew timer
                        if n.leader_id.unwrap() == msg.sender_id && n.status == NodeStatus::Following{
                            n.last_alive_from_leader = Instant::now();
                        }




                        // if n.leader_id == None && n.status == NodeStatus::Following{
                        //     // Switch leader, maybe need to go to sync phase
                        //     n.leader_id = Some(msg.sender_id);
                        //     n.epoch = msg.epoch;
                        //
                        // }
                        //
                        // // Heart beat is from this node's leader
                        // if n.status == NodeStatus::Following && msg.sender_id == n.leader_id.unwrap(){
                        //     println!("**{} timer update", n.id);
                        //     n.last_alive_from_leader = Instant::now();
                        // }
                    }

                    MessageType::Running => {
                        // another node is running
                        let mut answer = Message::new(n.id, MessageType::Blank);
                        answer.epoch = msg.epoch; // todo: bounce back the proposed epoch?
                        answer.last_zxid = Node::last_zxid(&n);
                        answer.receiver_id = msg.sender_id;

                        if Node::msg_sender_is_the_better_leader(&n, &msg){
                            answer.msg_type = MessageType::Approve
                        }else {
                            answer.msg_type = MessageType::Reject
                        }
                        Node::broadcast(&n, answer).await;
                    }

                    MessageType::Approve =>{
                        if msg.receiver_id != n.id {continue}
                        n.supporting_nodes.insert(msg.sender_id);

                        if n.supporting_nodes.len() as i32 > (n.cluster_size + 1) / 2{
                            n.leader_id = None;
                            n.status = NodeStatus::Leading; // later heartbeat msg will act as winning declaration
                            n.epoch += 1;
                            n.supporting_nodes = HashSet::new();
                        }

                    }

                    MessageType::Reject =>{
                        if n.status != NodeStatus::Running || msg.receiver_id != n.id{continue}

                        if Node::msg_sender_is_the_better_leader(&n, &msg){
                            // give up campaign
                            println!("{} give up election", n.id);
                            n.status = Following;
                            n.leader_id = None;
                            n.last_alive_from_leader = Instant::now();
                            n.supporting_nodes = HashSet::new();
                        }

                    }

                    _ =>{
                        println!("message_type's {:?} answer is not implemented", msg.msg_type);
                    }
                }
            }
        }


    }

    fn msg_sender_is_the_better_leader(n: &Node, msg: &Message) -> bool{
        //compared to current node, if the msg sender is a better leader
        let zxid = Node::last_zxid(n);

        if msg.epoch <= n.epoch{
            return false;
        }

        if msg.last_zxid < zxid{
            return false;
        }

        if msg.last_zxid == zxid{ // tie
            return if msg.sender_id > n.id { true } else { false }
        }

        true
    }



    async fn heartbeat_emitter(node: Arc<Mutex<Node>>){
        loop{
            sleep(Duration::from_secs(1)).await;
            let n = node.lock().await;
            if n.status == NodeStatus::Leading{
                let mut heartbeat = Message::new(n.id, MessageType::Heartbeat);
                heartbeat.last_zxid = Node::last_zxid(&n);
                heartbeat.epoch = n.epoch;

                println!("{} heartbeat, epoch {}, last zxid {}", heartbeat.sender_id, heartbeat.epoch, heartbeat.last_zxid);
                Node::broadcast(&n, heartbeat).await;
            }
        }
    }

    async fn heartbeat_monitor(node: Arc<Mutex<Node>>){
        loop{
            sleep(Duration::from_secs(1)).await;
            let mut n = node.lock().await;

            if n.status == NodeStatus::Following && n.last_alive_from_leader.elapsed() > Duration::from_secs(5){
                n.leader_id = None;
                n.status = NodeStatus::Running;
                let first_supporter = n.id;
                n.supporting_nodes.insert(first_supporter);

                let mut begin_campaign = Message::new(n.id, MessageType::Running);
                begin_campaign.last_zxid = Node::last_zxid(&n);
                begin_campaign.epoch = n.epoch + 1;
                println!("{} starts campaign, zxid {}, epoch {}", n.id, begin_campaign.last_zxid, begin_campaign.epoch);
                Node::broadcast(&n, begin_campaign).await;
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
