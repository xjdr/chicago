package com.xjeffrose.chicago;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RocksDBImplTest {

  private RocksDBImpl rocksDbImpl;

  @Before
  public void setUp() throws Exception {
    this.rocksDbImpl = new RocksDBImpl();
  }

  @After
  public void tearDown() throws Exception {
    rocksDbImpl.close();
  }

  @Test
  public void write() throws Exception {
    for (int i = 0; i < 100000; i++) {
      assertTrue(rocksDbImpl.write("ColFam".getBytes(), ("Key" + i).getBytes(), ("Val" + i).getBytes()));
    }
  }

  @Test
  public void read() throws Exception {
    for (int i = 0; i < 100000; i++) {
      assertTrue(rocksDbImpl.write("ColFam".getBytes(), ("Key" + i).getBytes(), ("Val" + i).getBytes()));
    }

    for (int i = 0; i < 100000; i++) {
      assertEquals(("Val" + i), new String(rocksDbImpl.read("ColFam".getBytes(), ("Key" + i).getBytes())));
    }
  }

  @Test
  public void delete() throws Exception {
    for (int i = 0; i < 100000; i++) {
      assertTrue(rocksDbImpl.write("ColFam".getBytes(), ("Key" + i).getBytes(), ("Val" + i).getBytes()));
    }

    for (int i = 0; i < 100000; i++) {
      assertTrue(rocksDbImpl.delete("ColFam".getBytes(), ("Key" + i).getBytes()));
    }
  }

  @Test
  public void tsWrite() throws Exception {
    for (int i = 0; i < 100000; i++) {
      assertEquals(i, Longs.fromByteArray(rocksDbImpl.tsWrite("ColFam".getBytes(), ("Val" + i).getBytes())));
    }
  }

  @Test
  public void batchWrite() throws Exception {
    for (int i = 0; i < 100000; i++) {
      assertEquals(i, Longs.fromByteArray(rocksDbImpl.batchWrite("ColFam".getBytes(), ("Val" + i).getBytes())));
    }
  }

  @Test
  public void stream() throws Exception {
    //TODO(JR): Fix this test to be more accurate
    for (int i = 0; i < 100000; i++) {
      assertEquals(i, Longs.fromByteArray(rocksDbImpl.tsWrite("ColFam".getBytes(), ("Val" + i).getBytes())));
    }

//    for (int i = 0; i < 100000; i++) {
    final String result = new String(rocksDbImpl.stream("ColFam".getBytes(), Ints.toByteArray(0)));
//    final String[] resultBA = result.split(ChiUtil.delimiter);
      assertNotNull(result);
//    assertEquals(100000, resultBA.length);
//    assertEquals("Val12345", resultBA[12344]);
//    assertEquals(100000, result.length);

//    }
  }

}