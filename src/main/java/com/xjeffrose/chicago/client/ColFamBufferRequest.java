package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.xjeffrose.chicago.ChiUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by root on 8/3/16.
 */
public class ColFamBufferRequest {
  private byte[] colFam;
  private List<byte[]> values;
  private final ConcurrentHashMap<String, SettableFuture<byte[]>> futures = new ConcurrentHashMap<>();
  private int size;

  public ColFamBufferRequest(byte[] colFam, List<String> hashList){
      this.colFam = colFam;
      this.values = new ArrayList<>();
      for(String node: hashList){
        futures.put(node, SettableFuture.create());
      }
      this.size=0;
  }

  public byte[] getColFam() {
    return colFam;
  }

  public void setColFam(byte[] colFam) {
    this.colFam = colFam;
  }

  public List<byte[]> getValues() {
    return values;
  }

  public void setValues(List<byte[]> values) {
    this.values = values;
  }

  public void addValue(byte[] val){
    values.add(val);
    size += val.length + ChiUtil.delimiter.getBytes().length;
  }

  public ListenableFuture<List<byte[]>> listListenableFuture(){
    return Futures.successfulAsList(futures.values());
  }

  public List<String> getNodes(){
    return new ArrayList<String>(futures.keySet());
  }

  public  SettableFuture<byte[]> getFuture(String node){
    return futures.get(node);
  }

  public byte[] getConsolidatedValue(){
    byte[] returnVal = new byte[0];
    for(byte[] value : values){
      byte[] _v;
      if(returnVal.length > 0) {
        _v = new byte[returnVal.length + ChiUtil.delimiter.getBytes().length + value.length];
        System.arraycopy(returnVal,0,_v,0,returnVal.length);
        System.arraycopy(ChiUtil.delimiter.getBytes(),0,_v,returnVal.length,ChiUtil.delimiter.getBytes().length);
        System.arraycopy(value,0,_v,returnVal.length+ChiUtil.delimiter.getBytes().length,value.length);
      }else{
        _v = new byte[value.length];
        System.arraycopy(value,0,_v,0,value.length);
      }
      returnVal = _v;
    }
    return returnVal;
  }

  public int getSize() {
    return size;
  }
}
