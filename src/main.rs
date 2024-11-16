use std::io;
use std::io::Write;
use std::sync::mpsc;
use std::time::Duration;
use tokio::task;
use tokio::time::sleep;
use crate::node::{Node,PeerMessage};
use crate::zookeeper::{Zookeeper};
// mod Zookeeper;
mod node;
mod zookeeper;

#[tokio::main]
async fn main() {
    // multi server is working, now, have a new message channel
    // mpsc - multi sending, single consume
    // get a zk running, let's send some msg ;
    let zk = zookeeper::new(5);




}

async fn multi_server_communication_test(){
    // Create a simulated message channel
    let (tx, rx) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();

    // Create a node
    let node = Node::new(1);
    let node2 = Node::new(2);

    // Run the node in the background
    let node_handle = task::spawn(node.run(rx));
    let node_handle2 = task::spawn(node2.run(rx2));


    loop {
        // Prompt user for input
        // print!("> ");
        io::stdout().flush().unwrap(); // Ensure prompt is shown immediately

        // Read input from stdin
        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Failed to read line");

        // Trim the input to remove any extra whitespace
        let input = input.trim();

        let parts = input.split(",").collect::<Vec<&str>>();
        let node_pic = String::from(parts[0]).parse::<i32>().unwrap();
        let content = String::from(parts[1]);

        if node_pic == 1 {
            tx.send(content).unwrap();
        }else{
            tx2.send(content).unwrap();
        }
    }

    node_handle.await.unwrap();
    node_handle2.await.unwrap();
}



#[cfg(test)]
mod tests {
    use crate::node::Node;

    #[test]
    fn hello_test(){
        println!("hello test");
    }



    #[test]
    async fn multi_servers(){
        // 3 servers running and responding at the same time, I call that a win


    }

}
