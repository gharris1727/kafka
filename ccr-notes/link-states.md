# Replication Link States
* Solid arrows are initiated by the AdminClient
* Dotted arrows happen due to background processes, changes in the hardware or network
```mermaid
flowchart LR
    no-link([No Link])
    created([Created])
    no-link --> |Create Link| created
    created -.-> |Handshake with remote cluster| async-syncing
    subgraph asynchronous [Asynchronous Operation]
        async-syncing([Catching up])
        async-in-sync([Following])
        async-in-sync & async-syncing --> |Add topics to link| async-syncing
        async-in-sync --> |Create topic-partition| async-in-sync
        async-syncing --> |Create topic-partition| async-syncing
        async-in-sync --> |Remove in-sync topic-partitions| async-in-sync
        async-syncing --> |Remove all out-of-sync topic-partitions| async-in-sync
        async-in-sync -.-> |Lose connection for any topic-partition| async-syncing
        async-syncing -.-> |Catch-up replication| async-in-sync
    end
    subgraph synchronous [Synchronous Operation]
        syncing([Syncing])
        in-sync([In-Sync])
        out-of-sync([Out-Of-Sync])
        syncing -.-> |All brokers ack config change| in-sync
        syncing & in-sync & out-of-sync --> |"Add topics to link (see note)"| out-of-sync
        syncing --> |Create topic-partition| syncing
        in-sync --> |Create topic-partition| in-sync
        out-of-sync --> |Create topic-partition| out-of-sync
        syncing --> |Remove topic-partitions| syncing
        in-sync --> |Remove in-sync topic-partitions| in-sync
        out-of-sync --> |Remove in-sync topic-partitions| out-of-sync
        out-of-sync --> |Remove all out-of-sync topic-partitions| in-sync
        in-sync -.-> |Lose connection for any topic-partition| out-of-sync
        out-of-sync -.-> |Catch-up replication| in-sync
    end
    async-in-sync --> |Set Synchronous| syncing
    synchronous --> |Set Asynchronous| async-syncing
    disconnected([Disconnected])
    synchronous & asynchronous ---> |Disconnect Link| disconnected
```
Individual brokers do not depend on the global state of the link, and instead operate based on their copy of the metadata.
If their copy of the metadata says the link is synchronous, they will delay acks as necessary.

A link can only guarantee exactly once delivery when all brokers are in the In-Sync or Out-Of-Sync states.
No automatic transitions out of these states are allowed, in order to preserve this property.
Making the link asynchronous immediately voids this guarantee, even if some brokers are still "in-sync" or "out-of-sync".
One can get a summary of the guarantees at any point in time by examining the link state persisted by the _active controller of the source cluster_.

### In Sync -> Out Of Sync Manual Transition
* The "Out-Of-Sync" link state causes the added source topics to be offline for transactional producers, and should be avoided
* If existing topics are added to the link via AdminClient, then the state will change as those topics may be nonempty, and have a backlog of data that needs to be replicated.
* This state change is temporary, as the topic may quickly replicate. But until it catches up, the link is degraded.
* To avoid this, operators should first change the link to asynchronous and then add the topics.
  * This is a footgun, should this state transition be disallowed?
  * It would be useful in hardening the system as it allows users to deliberately make the link out-of-sync to stop upstream producers to test the failure mode
  * It is obviously dangerous in a production environment, maybe it should have different permissions than adding topics to async links.











