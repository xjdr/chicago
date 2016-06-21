package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.TestChicago;
import com.xjeffrose.chicago.ZkClient;
import com.xjeffrose.chicago.server.ChicagoServer;
import com.google.common.primitives.Ints;
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
	ChicagoTSClient chicagoTSClient;
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

		chicagoTSClient = new ChicagoTSClient(testingServer.getConnectString(),
				3);
		zkClient = new ZkClient(testingServer.getConnectString());
		chicagoTSClient.startAndWaitForNodes(4);
		System.out.println("Started all four nodes");

		writeAndRead1000Values();
	}

	@After
	public void teardown() throws Exception {
		for (ChicagoServer server : servers) {
			server.stop();
		}
		chicagoTSClient.stop();
		testingServer.stop();
	}

	public void writeAndRead1000Values() throws Exception {
		byte[] offset = null;
		for (int i = 0; i < 1000; i++) {
			String _v = "val" + i;
			byte[] val = _v.getBytes();
			assertNotNull(chicagoTSClient.write(testKey.getBytes(), val));
			System.out.println(i);
		}

		ListenableFuture<ChicagoStream> f = chicagoTSClient.stream(testKey
				.getBytes());
		ChicagoStream cs = f.get();
		ListenableFuture<byte[]> resp = cs.getStream();

		assertNotNull(resp.get());
		String result = new String(resp.get());
		byte[] old = null;
		int count = 0;
		while (result.contains("@@@")) {

			if (count > 10) {
				break;
			}
			offset = result.split("@@@")[1].getBytes();
			if (old != null && Arrays.equals(old, offset)) {
				Thread.sleep(500);
			}
			System.out.println(result.split("@@@")[0]);
			cs.close();
			ListenableFuture<ChicagoStream> _f = chicagoTSClient.stream(
					testKey.getBytes(), offset);
			cs = _f.get();
			resp = cs.getStream();
			result = new String(resp.get());
			old = offset;
			count++;
		}

		ListenableFuture<ChicagoStream> _f = chicagoTSClient.stream(
				"tskey".getBytes(), offset);
		cs = _f.get();
		ListenableFuture<byte[]> _resp = cs.getStream();

		assertNotNull(_resp.get());
		System.out.println(new String(_resp.get()));

	}
	public void testValidResponse(List<String> nodes, int key) throws InterruptedException, ExecutionException, ChicagoClientTimeoutException{
		ChicagoClient cc=new ChicagoClient(nodes.get(0));
		String response1 = new String(cc.read(testKey.getBytes(), Ints.toByteArray(key)).get());
		cc = new ChicagoClient(nodes.get(1));
		String response2 = new String(cc.read(testKey.getBytes(), Ints.toByteArray(key)).get());
		cc = new ChicagoClient(nodes.get(2));
		String response3 = new String(cc.read(testKey.getBytes(), Ints.toByteArray(key)).get());
		String expectedResponse="val"+key;
		assertEquals(response1.startsWith(expectedResponse),true);
		assertEquals(response2.startsWith( expectedResponse),true);
		assertEquals(response3.startsWith( expectedResponse),true);
	}

	@Test
	public void testReplicationOnNodesAfterWait() throws Exception {

		List<String> nodes = chicagoTSClient.getNodeList(testKey.getBytes());
		System.out.println("Querying old set of nodes" + nodes.get(0)
				+ nodes.get(1) + nodes.get(2));
		testValidResponse(nodes,100);
		testValidResponse(nodes,999);
		
		for (ChicagoServer server : servers) {
			System.out.println("Stopping server.. "
					+ server.getDBAddress());
			zkClient.delete("/chicago/node-list/" + server.config.getDBBindEndpoint());
			break;
		}

		List<String> newNodes = chicagoTSClient.getNodeList(testKey.getBytes());
		List<String> replicationList = null;

		do {
			System.out.println("Waiting for replication lock to be created");
			replicationList = zkClient.list(REPLICATION_LOCK_PATH + "/"
					+ new String(testKey));
			while  (replicationList.isEmpty()){
				//do nothing, wait for the lock path to get created
			}
			for (String repli : replicationList) {
				System.out.println("This is the list being replicated right now "+ repli);
			}
		} while (!replicationList.isEmpty());
		//Test after replication is completed
		
		for (int i = 1; i < 5; i++) {
			System.out.println(i + "th iteration - Querying new set of nodes"
					+ newNodes.get(0) + newNodes.get(1) + newNodes.get(2));
			testValidResponse(newNodes,100);
			testValidResponse(newNodes,999);
		}
	}

	@Test
	public void testReplicationOnNodesWithDelayedWait() throws Exception {

		List<String> nodes = chicagoTSClient.getNodeList(testKey.getBytes());

		ChicagoTSClient cc = new ChicagoTSClient(nodes.get(0));
		System.out.println("Querying old set of nodes" + nodes.get(0)
				+ nodes.get(1) + nodes.get(2));
		testValidResponse(nodes,100);
		testValidResponse(nodes,999);

		for (ChicagoServer server : servers) {
			System.out.println("Test stopping a server.. "
					+ server.getDBAddress());
			zkClient.delete("/chicago/node-list/" + server.config.getDBBindEndpoint());
			break;
		}

		List<String> newNodes = chicagoTSClient.getNodeList(testKey.getBytes());

		for (int i = 0; i < 5; i++) {
			System.out.println(i + "th iteration - Querying new set of nodes"
					+ newNodes.get(0) + newNodes.get(1) + newNodes.get(2));
			
			int noOfGoodResponses=getNoOfValidResponse(newNodes,999);

			if (i == 0) {
				List<String> replicationList = null;
				System.out.println("No of good responses without sleeping: "
						+ noOfGoodResponses); // Expect 2/3 depending on
												// performance good responses as
												// replication is ongoing
				do {
					System.out.println("Waiting for replication lock to be created");
					replicationList = zkClient.list(REPLICATION_LOCK_PATH + "/"
							+ new String(testKey));
					while  (replicationList.isEmpty()){
						//do nothing, wait for the lock path to get created
					}
					for (String repli : replicationList) {
						System.out.println("This is the list being replicated right now "+ repli);
					}
				} while (!replicationList.isEmpty());
				//Test after replication is completed
				
			} else {
				assertEquals("Failed on read after sleep of: " + 40000, 3,
						noOfGoodResponses);
			}
		}
	}

	private int getNoOfValidResponse(List<String> nodes, int key) throws InterruptedException, ExecutionException, ChicagoClientTimeoutException {
		ChicagoClient cc=new ChicagoClient(nodes.get(0));
		int noOfGoodResponse=0;
		String expectedResponse="val"+key;
		if(expectedResponse.equals(new String(cc.read(testKey.getBytes(), Ints.toByteArray(key)).get()))){
			noOfGoodResponse++;
		}
		cc = new ChicagoClient(nodes.get(1));
		if(expectedResponse.equals(new String(cc.read(testKey.getBytes(), Ints.toByteArray(key)).get()))){
			noOfGoodResponse++;
		}
		cc = new ChicagoClient(nodes.get(2));
		if(expectedResponse.equals(new String(cc.read(testKey.getBytes(), Ints.toByteArray(key)).get()))){
			noOfGoodResponse++;
		}
		return noOfGoodResponse;
		
	}

}
