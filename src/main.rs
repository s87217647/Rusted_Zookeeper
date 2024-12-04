extern crate alloc;
use crate::zookeeper::Zookeeper;
use futures::future::join_all;
use std::time::Duration;
mod node;
mod zookeeper;

#[tokio::main]
async fn main() {
    let mut zk = Zookeeper::new(10);
    zk.run().await;
}

#[tokio::test]
async fn election_test() {
    let zk_size = 10;

    let mut zk = Zookeeper::new(zk_size);
    let handlers = zk.start_servers();
    tokio::time::sleep(Duration::from_secs(20)).await;
    zk.find_latest_leader().await;
    assert_eq!(zk.latest_leader, zk_size - 1);
    zk.quit().await;
    join_all(handlers).await;
}
#[tokio::test]
async fn leader_crash_test() {
    let zk_size = 10;
    let mut zk = Zookeeper::new(zk_size);
    let handlers = zk.start_servers();
    tokio::time::sleep(Duration::from_secs(20)).await;

    zk.find_latest_leader().await;
    zk.crash_leader().await;
    tokio::time::sleep(Duration::from_secs(30)).await;

    zk.find_latest_leader().await;
    assert_eq!(zk.latest_leader, zk_size - 2);

    zk.quit().await;
    join_all(handlers).await;
}


#[tokio::test]
async fn new_transactions() {
    let mut zk = Zookeeper::new(10);
    let handlers = zk.start_servers();

    tokio::time::sleep(Duration::from_secs(20)).await;
    let latest_leader = zk.find_latest_leader().await;
    zk.new_transaction("k1".to_string(), "v1".to_string());
    zk.new_transaction("k2".to_string(), "v2".to_string());
    tokio::time::sleep(Duration::from_secs(2)).await;

    for (id, node) in zk.servers.iter() {
        let n = node.lock().await;
        let tx1 = n.history.get(0).unwrap();
        let tx2 = n.history.get(1).unwrap();
        assert_eq!(tx1.key, "k1".to_string());
        assert_eq!(tx1.val, "v1".to_string());
        assert_eq!(tx2.key, "k2".to_string());
        assert_eq!(tx2.val, "v2".to_string());
    }

    zk.quit().await;
    join_all(handlers).await;
}


