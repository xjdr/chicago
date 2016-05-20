Chicago
=======

High performance distributed K/V store, built on [xio](https://github.com/xjdr/xio)
and [rocksdb](https://github.com/facebook/rocksdb). Chicago was written for use in our
observability stack for real(ish) time stats and alerting. Chicago was designed to always be
available for writes and have consistent write times regardless

All data is encrypted on the wire by default but that feature can be disabled if you are either
in a locally trusted env, if performance is more important that security (you are probably wrong on this one),
if your data is already encrypted by another application. Snappy compression can also be enabled if packet size
over the wire is more important than cpu usage.

Chicago is horizontally scalable to N nodes but using a [RendezvousHash](https://en.wikipedia.org/wiki/Rendezvous_hashing)

Chicago is optimized for low latency, high volume writes. Replicated writes should be able to happen in the
3 - 5ms range at millions of objects a second (with the appropriate configuration) + network latency. Chicago
can be implemented for local DC writes and cross DC reads by configuring the appropriate "Views" for the client.

## Replications

### Write Replication
By default, Chicago will write your data to 3 servers for each request in parallel. Each request is hashed and checksummed
upon encoding, over the wire, on decoding, on DB write and on db read. This might seem excessive but Chicago enforces
correctness on write vs error correction on read. Chicago will only ack the client if all 3 replicas are successfully written.
If any of the 3 write requests return unsuccessful, the write will be retried (to a maximum configurable count) until successful
or the retry count is exceeded. If the retry count is exceeded, the keys will be deleted from the replica set and the operation
will return unsuccessful (TODO).

### Read Replication
With the assumption that correctness is enforced on write, read requests are sent out to each node in the replica set
simultaneously and the first successful response is returned. Reads should be successful as long as one of the nodes in
the replica set is available.

### Key re-balancing on Node addition or Removal
Any time a node is added or removed, each server will perform an out of band operation to rebalance its keys. To accomplish this,
each server will read all the keys from its local db, calculate the hash for the new node list (the view) and then redistribute the
keys as appropriate. The strategy for replica rebalancing is configurable, but by default keys will be re-written by each node as
is it calculated in the hash. The trade off here is additional write requests to ensure correctness and durability. Other tradeoffs
can be configured via replication strategies depending on your use case (TODO).

## Views
A view is a list of nodes kept in zookpeer for which the client will perform the local hash to attempt to set or
retrieve a key. In it's simplest form the client receives a single view for reads and writes. For local quorum,
cross dc replication, or whatever use case your heart desires, you can customize views (and thus the subsequent hash ring)
for a particular application (TODO).


This work is heavily inspired by Twitter's Manhattan
https://blog.twitter.com/2014/manhattan-our-real-time-multi-tenant-distributed-database-for-twitter-scale
and
https://blog.twitter.com/2016/observability-at-twitter-technical-overview-part-i