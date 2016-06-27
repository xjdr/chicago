package com.xjeffrose.chicago;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 6/27/16.
 */
public class ChiUtilTest {

  @Test
  public void testOffset(){
    byte[] input = new byte[]{1,2,3,4,64,64,64,2,6,2};
    byte[] delimiter = new byte[]{64,64,64};

    assertEquals(6,ChiUtil.findLastOffsetIndex(input,delimiter));
    assertEquals(-1,ChiUtil.findLastOffsetIndex(input,new byte[]{1,5,6}));
    assertEquals(1,ChiUtil.findLastOffsetIndex(input,new byte[]{1,2}));
    assertEquals(9,ChiUtil.findLastOffsetIndex(input,new byte[]{2}));
    assertEquals(9,ChiUtil.findLastOffsetIndex(input,new byte[]{2,6,2}));
    assertEquals(4,ChiUtil.findLastOffsetIndex(input,new byte[]{3,4,64}));
  }
}
