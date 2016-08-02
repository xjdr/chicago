package com.xjeffrose.chicago.client;

import com.google.common.primitives.Longs;
import com.xjeffrose.chicago.TestChicago;
import com.xjeffrose.chicago.ZkClient;
import com.xjeffrose.chicago.server.ChicagoServer;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.curator.test.TestingServer;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class ChicagoReplicationTSClientTest {
	TestingServer testingServer;
	private final static String REPLICATION_LOCK_PATH = "/chicago/replication-lock";
	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();
	List<ChicagoServer> servers;
	ChicagoClient chicagoClient;
	ZkClient zkClient;
	File tmpDir = null;
	String testKey="tsKey";

	@Before
	public void setup() throws Exception {
		testingServer = new TestingServer(true);
		tmpDir = TestChicago.chicago_dir(tmp);
		servers = TestChicago.makeServers(tmpDir, 4,
				testingServer.getConnectString());
		for (ChicagoServer server : servers) {
			server.start();
		}

    chicagoClient = new ChicagoClient(testingServer.getConnectString(),
				3);
		zkClient = new ZkClient(testingServer.getConnectString(),false);
    zkClient.start();
		System.out.println("Started all four nodes");

		writeAndRead1000Values();
	}

	@After
	public void teardown() throws Exception {
		for (ChicagoServer server : servers) {
			server.stop();
		}
    servers.clear();
    zkClient.stop();
		chicagoClient.stop();
    chicagoClient = null;
    zkClient = null;
		testingServer.stop();
	}

	public void writeAndRead1000Values() throws Exception {
		byte[] offset = null;
		for (int i = 0; i < 1000; i++) {
			String _v = "val" + i;
			byte[] val = _v.getBytes();
			assertNotNull(chicagoClient.tsWrite(testKey.getBytes(), val).get().get(0));
			System.out.println(i);
		}

		byte[] resp = chicagoClient.stream(testKey
      .getBytes(), Longs.toByteArray(0)).get().get(0);
		assertNotNull(resp);
	}
	public void testValidResponse(List<String> nodes, int key) throws Exception{
		ChicagoClient cc=new ChicagoClient(nodes.get(0));
		String response1 = new String(cc.read(testKey.getBytes(), Longs.toByteArray(key)).get().get(0));
		cc.stop();
		cc = new ChicagoClient(nodes.get(1));
		String response2 = new String(cc.read(testKey.getBytes(), Longs.toByteArray(key)).get().get(0));
		cc.stop();
		cc = new ChicagoClient(nodes.get(2));
		String response3 = new String(cc.read(testKey.getBytes(), Longs.toByteArray(key)).get().get(0));
		cc.stop();
		String expectedResponse="val"+key;
		assertEquals(response1,(expectedResponse));
		assertEquals(response2,( expectedResponse));
		assertEquals(response3,( expectedResponse));
		System.out.println("Response is valid");
	}

	@Test
	public void testReplicationOnNodesAfterWaitAA() throws Exception {

		List<String> nodes = chicagoClient.getNodeList(testKey.getBytes());
		System.out.println("Querying old set of nodes" + nodes.toString());
		testValidResponse(nodes,100);
		testValidResponse(nodes,999);

		for (String node : nodes) {
			System.out.println("Stopping server.. "
					+ node);
			zkClient.delete(ChicagoClient.NODE_LIST_PATH +"/"+ node);
			break;
		}

		List<String> newNodes = chicagoClient.getNodeList(testKey.getBytes());
		List<String> replicationList = null;
		System.out.println("Waiting for replication lock to be created");
		replicationList = zkClient.list(REPLICATION_LOCK_PATH + "/"
				+ new String(testKey));

		while  (replicationList.isEmpty()){
			Thread.sleep(1);
			//do nothing, wait for the lock path to get created
			replicationList = zkClient.list(REPLICATION_LOCK_PATH + "/"
					+ new String(testKey));
		}

    System.out.println("This is the list being replicated right now "+ replicationList.toString());

    System.out.println("Waiting for replication to terminate");
    replicationList = zkClient.list(REPLICATION_LOCK_PATH + "/"
        + new String(testKey));
    while (!replicationList.isEmpty()){
       Thread.sleep(1);
        //do nothing, wait for the lock path to get deleted
        replicationList = zkClient.list(REPLICATION_LOCK_PATH + "/"
            + new String(testKey));
    }
    System.out.println("Replication completed!!");


		//Test after replication is completed
		for (int i = 1; i < 5; i++) {
			System.out.println(i + "th iteration - Querying new set of nodes"
					+ nodes.toString());
			testValidResponse(newNodes,100);
			testValidResponse(newNodes,999);
		}
	}

	@Test
	public void testReplicationOnNodesWithDelayedWait() throws Exception {

		List<String> nodes = chicagoClient.getNodeList(testKey.getBytes());

		System.out.println("Querying old set of nodes" + nodes.toString());
		testValidResponse(nodes,100);
		testValidResponse(nodes,999);

		for (String node : nodes) {
			System.out.println("Test stopping a server.. "
					+ node);
			zkClient.delete(ChicagoClient.NODE_LIST_PATH +"/"+ node);
			break;
		}

		List<String> newNodes = chicagoClient.getNodeList(testKey.getBytes());

		for (int i = 0; i < 5; i++) {
			System.out.println(i + "th iteration - Querying new set of nodes"
					+ newNodes.toString());

			int noOfGoodResponses=getNoOfValidResponse(newNodes,999);

			if (i == 0) {
				List<String> replicationList = null;
				System.out.println("No of good responses without sleeping: "
						+ noOfGoodResponses); // Expect 2/3 depending on
												// performance good responses as
												// replication is ongoing
				System.out.println("Waiting for replication lock to be created");
				replicationList = zkClient.list(REPLICATION_LOCK_PATH + "/"
						+ new String(testKey));
				while  (replicationList.isEmpty()){
					Thread.sleep(1);
					//do nothing, wait for the lock path to get created
					replicationList = zkClient.list(REPLICATION_LOCK_PATH + "/"
							+ new String(testKey));
				}
        System.out.println("This is the list being replicated right now "+ replicationList.toString());
        System.out.println("Waiting for replication to be over");
        while (!replicationList.isEmpty()){
					 Thread.sleep(1);
						//do nothing, wait for the lock path to get deleted
						replicationList = zkClient.list(REPLICATION_LOCK_PATH + "/"
								+ new String(testKey));
        }


				//Test after replication is completed

			} else {
				assertEquals("Failed on read after Delayed wait: ", 3,
						noOfGoodResponses);
			}
		}
	}

	private int getNoOfValidResponse(List<String> nodes, int key) throws Exception {
		ChicagoClient cc=new ChicagoClient(nodes.get(0));
		int noOfGoodResponse=0;
		String expectedResponse="val"+key;
		if(expectedResponse.equals(new String(cc.read(testKey.getBytes(), Longs.toByteArray(key)).get().get(0)))){
			noOfGoodResponse++;
		}
		cc.stop();
		cc = new ChicagoClient(nodes.get(1));
		if(expectedResponse.equals(new String(cc.read(testKey.getBytes(), Longs.toByteArray(key)).get().get(0)))){
			noOfGoodResponse++;
		}
		cc.stop();
		cc = new ChicagoClient(nodes.get(2));
		if(expectedResponse.equals(new String(cc.read(testKey.getBytes(), Longs.toByteArray(key)).get().get(0)))){
			noOfGoodResponse++;
		}
		cc.stop();
		return noOfGoodResponse;

	}

}
