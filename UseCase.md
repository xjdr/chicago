* Chicago DB Use Cases

** Key Value Store

*** Write Guarantees

**** Always available writes

**** Very low latency writes

***** Time from issuing write request to ack from N quorum nodes is less than 10ms

**** Always replicated, if you receive an ack it means the write was replicated to N quorum nodes

**** Failure modes somewhat understood, recovery still lacking

*** Read Guarantees

**** If at least 1 of the N quorum nodes is available you will get a response

**** If none of the N quorum nodes is available you will get a failure

**** If you wrote the key and received a successful response, the value will be available immediately

*** Replication Guarantees

**** For a given quorum setting, key/values will be written to N quorum nodes.

**** If a node joins/leaves the cluster, the key/values in the cluster will be redistributed across the remaining nodes. Such that each key/value will exist on N quorum nodes.

**** Redistribution of Data
     As a client this means that if there are N nodes that have my
     key/value and one of them leaves the cluster, N-1 nodes will have
     my key/value until such time that the key/values are
     redistributed and once again there are N nodes with my key/value

** Streaming Time Series




* Logging

    1. Writes
        * System level logs and PPFE logs needs to be published to chicago through a client.
        * Writes need to be asynchronous, and best effort based.
        * Each write operation should not be more than 10 ms(Not sure if that is achievable).
        * incremental reads by either timestamp / offset / byte size , do not resend logs previously piped to kafka
        * Restrict size of logs being uploaded to kafka via chicago in order of < 20 MB per update to avoid clogging both from host -> chicago and chicago -> kafka 
        * Chicago module should have HEADER in logs for each task being logged 
        * Create consistent format for logging

    2. Reads
        * Consumer from chicago should be able to read messages based on a topic name in milli seconds.
        * Chicago should upload to existing kafka topic
        * consistent updates and format with HEADER on updates for each update
        * Consumer should be able to filter the kafka topic with task information 


* PPFE 
    1. Block DDOS attach
        *  In a scenario when a particular IP is sending millions of requests in short duration of time, PPFE needs to block that IP address by re-using analysis from Chicago stream logs.
        * The logs sent to Chicago needs to be consumed by a consumer in order of milli seconds.

    2. Block bad requests
        * In case certain bad requets are sent to PPFE, which can be parsed in certain way, we need to send the logs to Chicago.
        * By a stream consumer, algorithms can detect such requests and make decisions to block the requests without manual intervention.

* ProdNG 
    1. Accomodate custom requirements to Agent 
    	1. *TODO* grab big brother use cases 
    	2. *TODO* grab Zabbix use cases  
    2. Chicago module to update logs 
    3. Create source list and format patterns by application / system log monitoring / custom tasks 
    4. Accomodate custom operations [ adhoc scripts / app logs ] 
    5. Agent should be able to run scripts on demand and channel logs via chicago to Kafka topic 
    6. Log retention time
    7. Report Health
    8. Capture all stacktraces 
    9. Memory / cpu report parameters 
    10. log zombie processes 
    11. machine health reporting 
        a. space 
        b. load 
        c. free memory 
        d. app running stats  
    12. sshd logs 
    13. seperate logging filter from ProdNG for kafka topic 
    14. Identify delay in logs being consumed 
    15. Alerting on chicago server(s)
    16. break updates into shards for topics / breakdown for filter 
    17. Log rotation and reporting 
    18. init.d service monitoring 
    19. Create chicago module for transferring data from Host to chicago 


* Kafka Setup 
    1. Cluster Availability and maintenance

Requirements :

    * Very Fast asynchronous writes.
    * Ordered(Not sure if we need ordering) Reads based on a topic/column family as soon as they are wriiten.
    * Consumers to tail the logs from Chicago and push to to respective topics in Kafka for all other cliets like splunk,dashboard to consume.
    * Replication of messages to configurable no. of brokers
    * Fault tolerant - Writes and reads are not affected by a node in cluster going down as long as the quorom is maintained.
    * Retention of messaged in RocksDB based on size or time period or as soon as they are pushed to Kafka.
    * configure size of logs being updated by interval and size of logs 
    * logs should be truncated after each update and rolled into old 
    * offset filter for host / topic 
    * offset filter for big brother / zabbix
