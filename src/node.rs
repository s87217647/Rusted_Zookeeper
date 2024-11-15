use std::io::{self, Write};
use std::sync::mpsc;
use tokio::time::sleep;

#[derive(Debug)]
pub(crate) enum PeerMessage {
    Heartbeat,
    Election,
    Custom(String),
}



//////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////
pub struct Node{
    pub id: i32
}

impl Node{
    pub fn new(id: i32) -> Node{
        Node{id}
    }
    pub fn get_id(&self) -> i32{self.id}

    pub fn listen(&self) {
        println!("Node is ready to echo messages. Type 'exit' to quit.");

        loop {
            // Prompt user for input
            print!("> ");
            io::stdout().flush().unwrap(); // Ensure prompt is shown immediately

            // Read input from stdin
            let mut input = String::new();
            io::stdin().read_line(&mut input).expect("Failed to read line");

            // Trim the input to remove any extra whitespace
            let input = input.trim();

            // Exit the loop if the user types "exit"
            if input.eq_ignore_ascii_case("exit") {
                println!("Shutting down the node.");
                break;
            }

            // Echo the input back
            println!("Echo: {}", input);
        }
    }

    pub async fn run(self, mut receiver: mpsc::Receiver<String>) {

        loop {
            let msg =  receiver.recv();
            if msg.is_err(){ break;}
            println!("Id is {}, Received a message: {:?}", self.id, msg.unwrap());
        }

    }


}