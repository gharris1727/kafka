# State Transition Sequence Diagram

This diagram shows some example state transitions among the main "running" link states.
Catching Up -> Following -> Syncing -> In-Sync -> Out-Of-Sync -> Catching Up
```mermaid
sequenceDiagram
    participant SAdmin as Admin
    box Source Cluster
        participant SProducer as Producer
        participant SCC as Controller Quorum
        participant STC as Transaction Coordinator
        participant SPL as Partition Leader
    end
    box Target Cluster
        participant TCC as Cluster Controller
        participant TRL as Remote Topic Leader
    end

    note over SCC: Catching Up
    note over TCC: Catching Up

    note over SProducer, TRL: Catch-up after creation or outage
    TCC->>SCC: Connect
    TRL->>SPL: Connect
    SPL->>TRL: Replication
    TRL-->>SPL: 
    SPL->>SCC: Mark topic-partition in-sync
    SCC->>SCC: Persist Following
    note over SCC: Following
    SCC-)TCC: Notify Following
    TCC->>TCC: Persist Following
    note over TCC: Following

    par Distribute Metadata
        SCC-)SPL: 
        SCC-)STC: 
        TCC-)TRL: 
    end

    note over SProducer, TRL: Reconfigure for Synchronous Operation
    SAdmin->>SCC: Set synchronous
    SCC->>SCC: Persist configuration change
    SCC->>SCC: Persist Syncing
    note over SCC: Syncing
    SCC-->>SAdmin: Success
    SCC->>TCC: Set synchronous
    TCC->>TCC: Persist configuration change
    TCC->>TCC: Persist Syncing
    note over TCC: Syncing
    par Distribute Metadata
        SCC->>SPL: 
        SCC->>STC: 
        TCC->>TRL: 
    end
    par Collect Heartbeats
        SPL-->>SCC: 
        STC-->>SCC: 
        TRL-->>TCC: 
    end
    TCC->>TCC: Persist In-Sync
    note over TCC: In-Sync
    TCC-->>SCC: 
    SCC->>SCC: Persist In-Sync
    note over SCC: In-Sync

    par Distribute Metadata
        SCC-)SPL: 
        SCC-)STC: 
        TCC-)TRL: 
    end
    
    note over SProducer, TRL: Transition to Out-Of-Sync
    alt Target detects out-of-sync 
        alt
            SPL--xTRL: disconnected
            TRL->>TCC: Report Out-Of-Sync
        else
            TRL--xTCC: heartbeat missing
        else
            SCC--xTCC: heartbeat missing
        end
        TCC->>TCC: Persist Out-Of-Sync
        note over TCC: Out-Of-Sync
        TCC-)SCC: Notify out-of-sync (best-effort)
        SCC->>SCC: Persist Out-Of-Sync
        note over SCC: Out-Of-Sync
    else Source detects out-of-sync
        alt
            TRL--xSPL: ack missing
            SPL->>SCC: Report out-of-sync
        else
            STC--xSCC: heartbeat missing
        else
            SPL--xSCC: heartbeat missing
        else
            TCC--xSCC: heartbeat missing
        end
        SCC->>SCC: Persist Out-Of-Sync
        note over SCC: Out-Of-Sync
        SCC-)TCC: Notify Out-Of-Sync (best-effort)
        TCC->>TCC: Persist Out-Of-Sync
        note over TCC: Out-Of-Sync
    end

    par Distribute Metadata
        SCC-)SPL: 
        SCC-)STC: 
        TCC-)TRL: 
    end

note over SProducer, TRL: Reconfigure for Asynchronous Operation
    SAdmin->>SCC: Set asynchronous
    SCC->>SCC: Persist configuration change
    SCC->>SCC: Persist Catching Up
    note over SCC: Catching Up
    SCC-->>SAdmin: Success
    SCC-)TCC: Set asynchronous (best-effort)
    TCC->>TCC: Persist configuration change
    TCC->>TCC: Persist Catching Up
    note over TCC: Catching Up
    TCC-->>SCC: 

    par Distribute Metadata
        SCC-)SPL: 
        SCC-)STC: 
        TCC-)TRL: 
    end

```