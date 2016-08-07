package com.xjeffrose.chicago;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class InMemDBImplTest {
  private InMemDBImpl inMemDB;

  @Before
  public void setUp() throws Exception {
    this.inMemDB = new InMemDBImpl();
  }

  @After
  public void tearDown() throws Exception {
    inMemDB.close();
  }

  @Test
  public void write() throws Exception {
    for (int i = 0; i < 100000; i++) {
      assertTrue(inMemDB.write("ColFam".getBytes(), ("Key" + i).getBytes(), ("Val" + i).getBytes()));
    }
  }

  @Test
  public void read() throws Exception {
    for (int i = 0; i < 100000; i++) {
      assertTrue(inMemDB.write("ColFam".getBytes(), ("Key" + i).getBytes(), ("Val" + i).getBytes()));
    }

    for (int i = 0; i < 100000; i++) {
      assertEquals(("Val" + i), new String(inMemDB.read("ColFam".getBytes(), ("Key" + i).getBytes())));
    }
  }

  @Test
  public void delete() throws Exception {
    for (int i = 0; i < 100000; i++) {
      assertTrue(inMemDB.write("ColFam".getBytes(), ("Key" + i).getBytes(), ("Val" + i).getBytes()));
    }

    for (int i = 0; i < 100000; i++) {
      assertTrue(inMemDB.delete("ColFam".getBytes(), ("Key" + i).getBytes()));
    }
  }

  @Test
  public void tsWrite() throws Exception {
    for (int i = 0; i < 100000; i++) {
      assertEquals(i, Longs.fromByteArray(inMemDB.tsWrite("ColFam".getBytes(), ("Val" + i).getBytes())));
    }
  }

  @Test
  public void batchWrite() throws Exception {
    for (int i = 0; i < 100000; i++) {
      assertEquals(i, Longs.fromByteArray(inMemDB.batchWrite("ColFam".getBytes(), ("Val" + i).getBytes())));
    }
  }

//  @Test
//  public void stream() throws Exception {
//    //TODO(JR): Fix this test to be more accurate
//    for (int i = 0; i < 100000; i++) {
//      assertEquals(i, Longs.fromByteArray(inMemDB.tsWrite("ColFam".getBytes(), ("Val" + i).getBytes())));
//    }
//
////    for (int i = 0; i < 100000; i++) {
//    final String result = new String(inMemDB.stream("ColFam".getBytes(), Ints.toByteArray(0)));
////    final String[] resultBA = result.split(ChiUtil.delimiter);
//    assertNotNull(result);
////    assertEquals(100000, resultBA.length);
////    assertEquals("Val12345", resultBA[12344]);
////    assertEquals(100000, result.length);
//
////    }
//  }

}