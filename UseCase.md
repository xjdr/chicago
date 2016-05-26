Use Cases :


* Logging

    1. Writes
        * System level logs and PPFE logs needs to be published to chicago through a client.
        * Writes need to be asynchronous, and best effort based.
        * Each write operation should not be more than 10 ms(Not sure if that is achievable).
    2. Reads
        * Consumer from chicago should be able to read messages based on a topic name in milli seconds.

* PPFE 
    1. Block DDOS attach
        *  In a scenario when a particular IP is sending millions of requests in short duration of time, PPFE needs to block that IP address by re-using analysis from Chicago stream logs.
        * The logs sent to Chicago needs to be consumed by a consumer in order of milli seconds.

    2. Block bad requests
        * In case certain bad requets are sent to PPFE, which can be parsed in certain way, we need to send the logs to Chicago.
        * By a stream consumer, algorithms can detect such requests and make decisions to block the requests without manual intervention.


Requirements :

    * Fast asynchronous writes.
    * Ordered(Not sure if we need ordering) Reads based on a topic/column family as soon as they are wriiten.
    * Consumers to tail the logs from Chicago and push to to respective topics in Kafka for all other cliets like splunk,dashboard to consume.
    * Replication of messages to configurable no. of brokers
    * Fault tolerant - Writes and reads are not affected by a node in cluster going down as long as the quorom is maintained.
    * Retention of messaged in RocksDB based on size or time period or as soon as they are pushed to Kafka.
