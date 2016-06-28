package com.xjeffrose.chicago.client;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import com.xjeffrose.chicago.ChiUtil;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.TestChicago;
import com.xjeffrose.chicago.server.ChicagoServer;

public class ChicagoTSClientTest {
	TestingServer testingServer;
	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();
	List<ChicagoServer> servers;
	ChicagoTSClient chicagoTSClient;

	@Before
	public void setup() throws Exception {
		InstanceSpec spec = new InstanceSpec(null, 2182, -1, -1, true, -1,
				2000, -1);
		testingServer = new TestingServer(spec, true);
		servers = TestChicago.makeServers(TestChicago.chicago_dir(tmp), 4,
				testingServer.getConnectString());
		for (ChicagoServer server : servers) {
			server.start();
		}
		chicagoTSClient = new ChicagoTSClient(testingServer.getConnectString(),
				3);
		chicagoTSClient.startAndWaitForNodes(4);
	}

	@After
	public void teardown() throws Exception {
		for (ChicagoServer server : servers) {
			server.stop();
		}
		chicagoTSClient.stop();
		testingServer.stop();
	}

	@Test
	public void transactStreamWhileWritingSameClient() throws Exception {
		for (int i = 0; i < 10; i++) {
			String _v = "val" + i;
			byte[] val = _v.getBytes();
			assertNotNull(chicagoTSClient.write("tskey".getBytes(), val));
			// System.out.println(i);
		}
		final ChicagoTSClient chicagoTSClientParalellel = new ChicagoTSClient(
				testingServer.getConnectString(), 3);
		chicagoTSClientParalellel.startAndWaitForNodes(4);
		final ArrayList<String> response = new ArrayList<String>();
		Runnable runnableStreamer = new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(200);
					ListenableFuture<ChicagoStream> _f = chicagoTSClientParalellel
							.stream("tskey".getBytes(), Ints.toByteArray(2));
					ChicagoStream cs = _f.get();
					ListenableFuture<byte[]> _resp = cs.getStream();
					assertNotNull(_resp.get());
					response.add(new String(_resp.get()));
					Thread.sleep(200);
					_f = chicagoTSClientParalellel.stream("tskey".getBytes(),
							Ints.toByteArray(4));
					cs = _f.get();
					_resp = cs.getStream();
					assertNotNull(_resp.get());
					response.add(new String(_resp.get()));
					cs.close();
					Thread.sleep(200);
					_f = chicagoTSClientParalellel.stream("tskey".getBytes(),
							Ints.toByteArray(6));
					cs = _f.get();
					_resp = cs.getStream();
					assertNotNull(_resp.get());
					response.add(new String(_resp.get()));
					cs.close();
				} catch (Exception e) {

				}
			}
		};

		Runnable runnable2 = new Runnable() {
			@Override
			public void run() {
				for (int i = 10; i < 20; i++) {
					String _v = "val" + i;
					byte[] val = _v.getBytes();
					try {
						chicagoTSClientParalellel
								.write("tskey".getBytes(), val);
						try {
							Thread.sleep(200);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} catch (ChicagoClientTimeoutException e) {
						e.printStackTrace();
					} catch (ChicagoClientException e) {
						e.printStackTrace();
					}
				}
			}
		};

		Thread t1 = new Thread(runnableStreamer);
		Thread t2 = new Thread(runnable2);

		t1.start();
		t2.start();
		t1.join();
		t2.join();
		Assert.assertEquals(response.size(), 3);
		System.out.println(response.get(0) + " " + response.get(1)
				+ response.get(2));
		Assert.assertEquals(response.get(0).contains("val11"), false);// Ensures
																		// that
																		// insert
																		// and
																		// read
																		// are
																		// interwoven
		Assert.assertEquals(response.get(2).contains("val11"), true);// Ensures
																		// that
																		// insert
																		// and
																		// read
																		// are
																		// interwoven

	}

	@Test
	public void transactStreamWhileWritingDifferentClients() throws Exception {
		for (int i = 0; i < 10; i++) {
			String _v = "val" + i;
			byte[] val = _v.getBytes();
			assertNotNull(chicagoTSClient.write("tskey".getBytes(), val));
			// System.out.println(i);
		}
		final ChicagoTSClient chicagoTSClientParalellel = new ChicagoTSClient(
				testingServer.getConnectString(), 3);
		chicagoTSClientParalellel.startAndWaitForNodes(4);
		final ArrayList<String> response = new ArrayList<String>();
		Runnable runnableStreamer = new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(200);
					ListenableFuture<ChicagoStream> _f = chicagoTSClient
							.stream("tskey".getBytes(), Ints.toByteArray(2));
					ChicagoStream cs = _f.get();
					ListenableFuture<byte[]> _resp = cs.getStream();
					assertNotNull(_resp.get());
					response.add(new String(_resp.get()));
					Thread.sleep(200);
					_f = chicagoTSClient.stream("tskey".getBytes(),
							Ints.toByteArray(4));
					cs = _f.get();
					_resp = cs.getStream();
					assertNotNull(_resp.get());
					response.add(new String(_resp.get()));
					Thread.sleep(200);
					_f = chicagoTSClient.stream("tskey".getBytes(),
							Ints.toByteArray(6));
					cs = _f.get();
					_resp = cs.getStream();
					assertNotNull(_resp.get());
					response.add(new String(_resp.get()));
					cs.close();
				} catch (Exception e) {

				}
			}
		};

		Runnable runnable2 = new Runnable() {
			@Override
			public void run() {
				for (int i = 10; i < 20; i++) {
					String _v = "val" + i;
					byte[] val = _v.getBytes();
					try {
						chicagoTSClientParalellel
								.write("tskey".getBytes(), val);
						try {
							Thread.sleep(200);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} catch (ChicagoClientTimeoutException e) {
						e.printStackTrace();
					} catch (ChicagoClientException e) {
						e.printStackTrace();
					}
				}
			}
		};

		Thread t1 = new Thread(runnableStreamer);
		Thread t2 = new Thread(runnable2);

		t1.start();
		t2.start();
		t1.join();
		t2.join();
		Assert.assertEquals(response.size(), 3);
		System.out.println(response.get(0) + " " + response.get(1)
				+ response.get(2));
		Assert.assertEquals(response.get(0).contains("val11"), false);// Ensures
																		// that
																		// insert
																		// and
																		// read
																		// are
																		// interwoven
		Assert.assertEquals(response.get(2).contains("val11"), true);// Ensures
																		// that
																		// insert
																		// and
																		// read
																		// are
																		// interwoven

	}

	@Test
	public void transactRoundTripStream() throws Exception {
		long startTime=System.currentTimeMillis();
		for (int i = 0; (i < 1000); i++) {
			String _v = "val" + i;
			byte[] val = _v.getBytes();
			assertNotNull(chicagoTSClient.write("tskey".getBytes(), val));
			boolean gotResponse = false;
			while (!gotResponse) {
				ListenableFuture<ChicagoStream> _f = chicagoTSClient.stream(
						"tskey".getBytes(), Ints.toByteArray(i));
				ChicagoStream cs = _f.get();
				ListenableFuture<byte[]> _resp = cs.getStream();
				assertNotNull(_resp.get());
				String response = new String(_resp.get());
				if (response.contains(_v)){
					gotResponse = true;
				}
				cs.close();
			}
		}
		long testTime=(System.currentTimeMillis()-startTime)/1000;
		System.out.println("Tiem taken for one round robin read/write on avergae is: "+testTime );
		Assert.assertTrue(testTime <10);
	}

	@Test
	public void transactStream() throws Exception {

		for (int i = 0; i < 1000; i++) {
			String _v = "val" + i + "                                                   " +
        "                                                                          " +
        "                                                                    end!!"+i;
			byte[] val = _v.getBytes();
			assertNotNull(chicagoTSClient.write("tskey".getBytes(), val));
			// System.out.println(i);
		}

    String delimiter = ChiUtil.delimiter;


		ListenableFuture<ChicagoStream> f = chicagoTSClient.stream("tskey"
				.getBytes());
		ChicagoStream cs = f.get();
		ListenableFuture<byte[]> resp = cs.getStream();

		assertNotNull(resp.get());
		byte[] resultArray = resp.get();
    int offset = ChiUtil.findOffset(resultArray);
    String result = new String(resultArray);

		int old = -1;
		int count = 0;
		while (result.contains(delimiter)) {

			if (count > 10) {
				break;
			}
			offset = ChiUtil.findOffset(resultArray);
			if (old != -1 && (old == offset)) {
				Thread.sleep(500);
			}

      String output = result.split(ChiUtil.delimiter)[0];
			System.out.println(output);
			cs.close();

      if(!output.isEmpty()){
        offset = offset +1;
      }
			ListenableFuture<ChicagoStream> _f = chicagoTSClient.stream(
					"tskey".getBytes(), Ints.toByteArray(offset));
			cs = _f.get();
			resp = cs.getStream();
      resultArray = resp.get();
			result = new String(resp.get());
			old = offset;
			count++;
		}

		ListenableFuture<ChicagoStream> _f = chicagoTSClient.stream(
				"tskey".getBytes(), Ints.toByteArray(offset));
		cs = _f.get();
		ListenableFuture<byte[]> _resp = cs.getStream();

		assertNotNull(_resp.get());
		System.out.println(new String(_resp.get()));
		cs.close();
	}

	@Test
	public void transactLargeStream() throws Exception {
		byte[] offset = null;
		for (int i = 0; i < 10; i++) {
			byte[] val = new byte[10240];
			if (i == 12) {
				offset = chicagoTSClient.write("LargeTskey".getBytes(), val);
			}
			assertNotNull(chicagoTSClient.write("tskey".getBytes(), val));
		}

		ListenableFuture<byte[]> f = chicagoTSClient.read("tskey".getBytes());
		byte[] resp = f.get();

		System.out.println(new String(resp));

		ListenableFuture<ChicagoStream> _f = chicagoTSClient.stream(
				"tskey".getBytes(), offset);
		ChicagoStream _cs = _f.get();
		ListenableFuture<byte[]> _resp = _cs.getStream();

		System.out.println(new String(_resp.get()));
		_cs.close();
	}
}
