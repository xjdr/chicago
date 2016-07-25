package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.ChicagoMessage;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by root on 7/22/16.
 */
//Todo: Rename the interface with proper name.
public interface ResponseListener {
  public void onResponse(ChicagoMessage chicagoMessage, ChannelHandlerContext ctx);
}
