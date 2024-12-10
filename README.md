## Barebone Zookeeper service in Rust.

Barebone as each node stores transaction in in history with executing the command.The project focuses on establishing consensus. 

###Election
The protocol makes sure only the one with the largest last zxid wins the election, and the epoch makes sure the election will not go backward.When multiple nodes with the same last_zxid and epoch are running, unique id of each node is used to break tie, the one with the largest id wins. If a node receives an election message from a less qualified candidate than itself, it will reject the candidacy and start running for election.

###Discovery
During discovery, each node talks to the leader, and gives the  leader the missing information if the leader asks since the leader always has the highest last_zxid, at the end of the discovery phase, leader will have the most holistic data among all

###Sync
After discovery, each node syncs with their leader, getting the holistic data the leader have

###Broadcast
Users interact with Zookeeper, Zookeeper sends transactions to the leader and leader broadcasts it to all followers. If the leader does not crash, it stays in the Broadcast phase indefinitely

###Heartbeat monitor and emitter
When a node is leading, it sends out heartbeats periodically. When a node is following, it resets its heartbeat timer every time it receives a heartbeat message. If the timer ticks, it starts running for an election

###Program interface
-r: reports all node’s status, transaction history
-n: key val: create a new transaction and send it to the leader
-c: mimic a leader crash. Hold the leader’s lock for a long time, render it inactive, then, other nodes will start running again
