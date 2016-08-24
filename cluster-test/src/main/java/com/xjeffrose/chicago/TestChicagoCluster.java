package com.xjeffrose.chicago;

import com.xjeffrose.chicago.ZkClient;
import com.xjeffrose.chicago.client.ChicagoAsyncClient;

import java.util.HashMap;


/**
 * Created by smadan on 6/20/16.
 */
public class TestChicagoCluster {



    public ZkClient zkClient;
    public final HashMap<String, ChicagoAsyncClient> chicagoClientHashMap = new HashMap<>();
    //public final ChicagoClient chicagoClient;
    public ChicagoAsyncClient chicagoClient;
    public HashMap<String, String> servers;
    public final static String ELECTION_PATH = "/chicago/chicago-elect";
    public final static String NODE_LIST_PATH = "/chicago/node-list";
    public final static String NODE_LOCK_PATH = "/chicago/replication-lock";

    public TestChicagoCluster(HashMap<String,String> servers, String zkConnectString, int quorom) throws Exception{
        this.servers = servers;
        zkClient = new ZkClient(zkConnectString,false);
        zkClient.start();
        chicagoClient = new ChicagoAsyncClient(zkConnectString,quorom);
        chicagoClient.start();

        servers.keySet().forEach(k ->{
            try {
                String server = servers.get(k);
                ChicagoAsyncClient ccl = new ChicagoAsyncClient(server);
                ccl.start();
                chicagoClientHashMap.put(k,ccl);
            }catch (Exception e){
                e.printStackTrace();
            }
        });
    }

    public void markNodeDown(String serverName){
        zkClient.delete(NODE_LIST_PATH+"/"+servers.get(serverName));
    }

    public void markNodeUp(String serverName){
        zkClient.createIfNotExist(NODE_LIST_PATH+"/"+servers.get(serverName),"");
    }

    public boolean checkIfNodeExists(String path){
        try {
            return (zkClient.getClient().checkExists().forPath(path) != null);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
