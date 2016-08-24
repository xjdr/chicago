package com.smadan.chicago;

import com.google.common.primitives.Longs;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.client.ChicagoAsyncClient;
import com.xjeffrose.chicago.client.ChicagoClient;
import org.junit.Before;
import org.junit.runners.Parameterized;
import org.junit.Test;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by smadan on 6/20/16.
 */

public class ClusterTest {
    TestChicagoCluster testChicagoCluster;
   HashMap<String,String> servers = new HashMap<>();

  @Before
  public void setup() throws Exception {
    Config config = ConfigFactory.load("test").getConfig("testing-servers");

    for(String server: config.getString("servers").split(",")){
      String serverEndpoint=server.concat(":").concat(config.getString("dbport"));
      servers.put(serverEndpoint,serverEndpoint);
    }

    String zkString = config.getString("zkstring");
    testChicagoCluster = new TestChicagoCluster(servers, zkString, 3);
  }

    public String forServer(String server){
        String result=null;
        for(String k : servers.keySet()){
            String s = servers.get(k);
            if(s.equals(server)){
                result = k;
            }
        }
        return result;
    }

    @Test
    public void writeTSSequence() throws Exception{
        byte[] offset = null;
        String tsKey = "testKey";
        deleteColFam(tsKey);
        List<String> nodes = testChicagoCluster.chicagoClient.getNodeList(tsKey.getBytes());
        for (int i = 0; i < 30; i++) {
            String _v = "val" + i;
            byte[] val = _v.getBytes();
            assertEquals(i,
                Longs.fromByteArray(testChicagoCluster.chicagoClient.tsWrite(tsKey.getBytes(), val).get()));
        }
        assertTSClient(tsKey,29,"val29");

        //Test overwriting of data
        int key  = 29;
        String val  = "value29";
        //write
        assertTrue(testChicagoCluster.chicagoClient.write(tsKey.getBytes(),Longs.toByteArray(key), val.getBytes()).get());
        //Assert no overwrite took place
        assertTSClient(tsKey,key,"val29");
        //deleteColFam(tsKey);
    }

    public void assertTSClient(String colFam, int key, String val){
        List<String> nodes = testChicagoCluster.chicagoClient.getEffectiveNodes(colFam.getBytes());
        nodes.forEach(n -> {
            System.out.println("Checking node "+n);
            try {
                ChicagoClient cc = new ChicagoClient(n);
                assertEquals(val,new String(cc.read(colFam.getBytes(), Longs.toByteArray(key)).get().get(0)));
            }catch (Exception e){
                e.printStackTrace();
                return;
            }
        });
    }

    @Test @Parameterized.Parameters
    public void writeCCSequence() throws Exception{
        byte[] offset = null;
        List<String> nodes=null;
        for (int i = 0; i < 30; i++) {
            String key = "key"+i;
            String _v = "val" + i;
            byte[] val = _v.getBytes();
            assertTrue(testChicagoCluster.chicagoClient.write(key.getBytes(), val).get());
        }

        //Assert all nodes have the data
        assertCCdata("key29","val29");

        //Test overwriting of data
        String key = "key29";
        String val  = "value29";
        assertTrue(testChicagoCluster.chicagoClient.write(key.getBytes(), val.getBytes()).get());
        //Assert overwrite is successful
        assertCCdata(key,val);
        deleteColFam("chicago");
    }


    public void assertCCdata(String key,String val){
        List<String> nodes = testChicagoCluster.chicagoClient.getEffectiveNodes(ChiUtil.defaultColFam.getBytes());
        nodes.forEach(n -> {
            ChicagoAsyncClient cc = testChicagoCluster.chicagoClientHashMap.get(forServer(n));
            try {
                assertEquals(val,new String(cc.read(key.getBytes()).get()));
            }catch (Exception e){
                e.printStackTrace();
            }
        });
    }

    @Test @Parameterized.Parameters
    public void testReplication() throws Exception{
        String tsKey = "tsRepKey";
        try {
            deleteColFam(tsKey);
            List<String> nodes = testChicagoCluster.chicagoClient.getNodeList(tsKey.getBytes());
            assert (true == !testChicagoCluster.zkClient.list("/chicago/node-list").isEmpty());
            //Write some data.
            for (int i = 0; i < 2000; i++) {
                String _v = "val" + i;
                byte[] val = _v.getBytes();
                testChicagoCluster.chicagoClient.tsWrite(tsKey.getBytes(), val);
                if (i % 50 == 0) System.out.println(i);
            }
            assertTSClient(tsKey, 90, "val90");

            //Bring down the node.
            String server = nodes.get(0).split(":")[0];
            System.out.println("Shutting down " + server);
            String command = "sudo kill $(ps aux|  grep 'chicago'  | awk '{print $2}')";
            remoteExec(server, command);

            //Check for node in Zookeeper.
            long startTime = System.currentTimeMillis();
            String path = testChicagoCluster.NODE_LOCK_PATH + "/" + tsKey;
            //Wait for replication to happen
            List<String> repNodes = testChicagoCluster.zkClient.list(path);
            while (repNodes.size() < 2) {
                repNodes = testChicagoCluster.zkClient.list(path);
            }
            System.out.println("Lock path populated in "
                + (System.currentTimeMillis() - startTime)
                + "ms  repNode :"
                + repNodes.toString());

            assertTSClient(tsKey, 90, "val90");
            repNodes = testChicagoCluster.zkClient.list(path);
            while (!repNodes.isEmpty()) {
                Thread.sleep(10);
                repNodes = testChicagoCluster.zkClient.list(path);
            }
            //Thread.sleep(2000);
            assertTSClient(tsKey, 90, "val90");

            //Bring the server back up again.
            System.out.println("Starting server " + server);
            command = "sudo sh -c \"cd /home/smadan/chicago; ./chicago &\"";
            remoteExec(server, command);

            //Thread.sleep(2000);
            assertTSClient(tsKey, 90, "val90");
        }catch ( Exception e){
            e.printStackTrace();
            throw e;
        }finally {
            deleteColFam(tsKey);
        }
    }

    public void deleteColFam(String colFam){
        for(String server : servers.values()){
            try {
                ChicagoClient cc = new ChicagoClient(server);
                cc.deleteColFam(colFam.getBytes()).get();
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }


    public void remoteExec(String ip, String command) throws Exception{
        String host=ip;
        String user="prodngdev";
        String command1=command;
        String password = "kenobi";
        try{

            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            JSch jsch = new JSch();
            Session session=jsch.getSession(user, host, 22);
            session.setPassword(password);
            session.setConfig(config);
            session.connect();
            System.out.println("Connected");

            Channel channel=session.openChannel("exec");
            ((ChannelExec)channel).setCommand(command1);
            channel.setInputStream(null);
            ((ChannelExec)channel).setErrStream(System.err);

            InputStream in=channel.getInputStream();
            channel.connect();
            byte[] tmp=new byte[1024];
            long startTime = System.currentTimeMillis();
            while(true){
                if(System.currentTimeMillis() - startTime > 2000){
                    break;
                }
                while(in.available()>0){
                    int i=in.read(tmp, 0, 1024);
                    if(i<0)break;
                    System.out.print(new String(tmp, 0, i));
                }
                if(channel.isClosed()){
                    System.out.println("exit-status: "+channel.getExitStatus());
                    break;
                }
                try{Thread.sleep(1000);}catch(Exception ee){}
            }
            channel.disconnect();
            session.disconnect();
            System.out.println("DONE");
        }catch(Exception e){
            e.printStackTrace();
        }
    }



}
