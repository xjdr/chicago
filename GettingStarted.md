Getting started with Chicago
============================


## Starting Chicago Servers ##

To build and run Chicago, you need to have apache maven 3.0 or above and Java 8 or above.
You would also need XIO 12 or above which you can download and build from : https://github.com/xjdr/xio
Clone the repo and build it using maven :
```mvn clean compile install```

### Download and build chicago ###

Clone the repo and build it with maven:
  ```mvn clean compile install -DskipTests```


### Running Chicago cluster ###

Before starting the cluster, you need to have a Zookeeper instance.
Please refer to https://zookeeper.apache.org/doc/trunk/zookeeperStarted.html

Once you have a zookeeper running, modify the application.conf in config/ folder.
Fill in the following entries :
  1. `zookeeperCluster`
  2. `bindHost` with IP address of the machine for  admin, stats and db
   
Start the server :

``` # bin/chiServer.sh & ```

By default Logs will go to `bin/chi.log` file.


## Chicago clients ##

Import the chicago jar to your java project and use the Chicago client:


    ChicagoAsyncClient ctsa = new ChicagoAsyncClient(zookeeperConnectionString, quoromSize);
    ctsa.start();
    
    // To send a Key/Value 
    ctsa.write(key, value);
    
    //To send value to a TimeSeries DB:
    ctsa.tsWrite(topic, value);
