Chicago
=======

High performance distributed K/V store, build on [xio](https://github.com/xjdr/xio) , [bookkeeper](https://)
and [rocksdb](https://github.com/facebook/rocksdb)

Chicago is horizontally scalable to N nodes but using a [RendezvousHash](https://en.wikipedia.org/wiki/Rendezvous_hashing)

Chicago is optimized for low latency, high volume writes. Replicated writes should be able to happen in the
10 - 20ms range at millions of objects a second (with the appropriate configuration).

This work is heavily inspired by Twitter's Manhattan
https://blog.twitter.com/2014/manhattan-our-real-time-multi-tenant-distributed-database-for-twitter-scale
and
https://blog.twitter.com/2016/observability-at-twitter-technical-overview-part-i