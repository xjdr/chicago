package com.xjeffrose.chicago.client;

import com.google.common.collect.ImmutableList;
import com.netflix.curator.test.TestingServer;
import com.xjeffrose.chicago.Chicago;
import java.util.Collections;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class ClientNodeWatcherTest {

  static TestingServer testingServer;
  static Chicago chicago1;
  static Chicago chicago2;
  static Chicago chicago3;
  static Chicago chicago4;
  static ChicagoClient chicagoClientDHT;

  @BeforeClass
  static public void setupFixture() throws Exception {
    testingServer = new TestingServer(2182);
    chicago1 = new Chicago();
    chicago1.main(new String[]{"", "src/test/resources/test1.conf"});
    chicago2 = new Chicago();
    chicago2.main(new String[]{"", "src/test/resources/test2.conf"});
    chicago3 = new Chicago();
    chicago3.main(new String[]{"", "src/test/resources/test3.conf"});
    chicago4 = new Chicago();
    chicago4.main(new String[]{"", "src/test/resources/test4.conf"});
    chicagoClientDHT = new ChicagoClient(testingServer.getConnectString());
  }

  @Test
  public void removeSingleNode() throws Exception {

    while (chicagoClientDHT.getNodeList("foo".getBytes()) == null) {
      Thread.sleep(1);
    }

    List<String> before = ImmutableList.copyOf(chicagoClientDHT.getNodeList("foo".getBytes()));

    chicago1.stop();

    Thread.sleep(3000);

    List<String> after = chicagoClientDHT.getNodeList("foo".getBytes());

    assertTrue(chicagoClientDHT.buildNodeList().containsAll(after));

//    assertTrue(Collections.disjoint(before, after));

    chicago1.main(new String[]{"", "src/test/resources/test1.conf"});

    assertEquals(before, chicagoClientDHT.getNodeList("foo".getBytes()));

  }


}