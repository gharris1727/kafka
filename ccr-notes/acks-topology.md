Replication Link Data & Acks Topology
* Solid arrows are data
* Dotted arrows are acks
```mermaid
flowchart TB
    subgraph source-cluster [Source Cluster]
        source-leader(Leader)
        source-follower-0(Replica)
        source-follower-1(Replica)
        source-leader --> source-follower-0 & source-follower-1
        source-follower-0 & source-follower-1 -.-> source-leader
    end
    source-producer(Producer)
    source-producer --> source-leader
    source-leader -.-> source-producer
    subgraph target-cluster [Target Cluster]
        target-leader(Target Leader)
        target-follower-0(Replica)
        target-follower-1(Replica)
        target-leader --> target-follower-0 & target-follower-1
        target-follower-0 & target-follower-1 -.-> target-leader
    end
    source-leader ---> target-leader
    target-leader -.-> source-leader
```
Only one data/acks pair passes the cluster boundary, so the cost of performing replication and the number of outbound connections is kept low.

Producers are not aware that the target cluster exists and is replicating the data, except for the ack latency.

For a link which is replicating N topic-partitions including the __consumer_offsets topic, the number of connections should be N + 1.
This is because one connection is established between controllers to communicate metadata about the link bidirectionally.

