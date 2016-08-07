package com.xjeffrose.chicago.server;

import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.chicago.ChicagoObjectEncoder;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class ChicagoDBHandler extends SimpleChannelInboundHandler {
  private static final Logger log = LoggerFactory.getLogger(ChicagoDBHandler.class);
//  private static final int MAX_BUFFER_SIZE = 16000;

  private final DBInterface db;
  private final ChicagoObjectEncoder encoder = new ChicagoObjectEncoder();
//  private final DBLog dbLog;
//  private boolean needsToWrite = false;
//  private byte[] readResponse = null;
//  private boolean status = false;
//  private ChicagoMessage finalMsg = null;


  public ChicagoDBHandler(DBInterface db, DBLog dbLog) {
    this.db = db;
//    this.dbLog = dbLog;
  }

  private ChicagoMessage createErrorMessage() {
    return new DefaultChicagoMessage(UUID.randomUUID(), Op.fromInt(3), "x".getBytes(), Boolean.toString(false).getBytes(), "x".getBytes());
  }

//  private ChicagoMessage createMessage() {
//    if (finalMsg == null) {
//      return createErrorMessage();
//    }
//    return new DefaultChicagoMessage(finalMsg.getId(), Op.fromInt(3), finalMsg.getColFam(), Boolean.toString(status).getBytes(), readResponse);
//  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    log.debug("Connection Active for: " + ctx.channel().localAddress());
    log.debug("Connection Active for: " + ctx.channel().remoteAddress());

    ctx.fireChannelActive();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    log.debug("Connection InActive for: " + ctx.channel().localAddress());
    log.debug("Connection InActive for: " + ctx.channel().remoteAddress());

    ctx.fireChannelInactive();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Object req) throws Exception {
//    ChicagoMessage msg = null;

    if (req instanceof ChicagoMessage) {
//      msg = (ChicagoMessage) req;
//    }

      ChannelFutureListener writeComplete = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) {
          if (!future.isSuccess()) {
            log.error("Server error writing :" + " For UUID" + ((ChicagoMessage) req).getId() + " and key " + new String(((ChicagoMessage) req).getKey()));
          }
        }
      };

//    finalMsg = msg;

//    if (finalMsg == null) {
//      needsToWrite = true;
//    }

      switch (((ChicagoMessage) req).getOp()) {
        case READ:
//        readResponse = rocksDbImpl.read(((ChicagoMessage)req).getColFam(), ((ChicagoMessage)req).getKey());
          ctx.writeAndFlush(new DefaultChicagoMessage(((ChicagoMessage) req).getId(), Op.fromInt(3),
              ((ChicagoMessage) req).getColFam(), Boolean.toString(true).getBytes(),
              db.read(((ChicagoMessage) req).getColFam(),((ChicagoMessage) req).getKey()))).addListener(writeComplete);

          //dbLog.addRead(finalMsg.getColFam(), finalMsg.getKey());

//        if (readResponse != null) {
//          status = true;
//        }
          break;
        case WRITE:
//        status = rocksDbImpl.write(((ChicagoMessage)req).getColFam(), ((ChicagoMessage)req).getKey(), ((ChicagoMessage)req).getVal());
          //dbLog.addWrite(finalMsg.getColFam(), finalMsg.getKey(), finalMsg.getVal());
//        readResponse = new byte[]{(byte) (status ? 1 : 0)};
//        log.debug("  ========================================================== Server wrote :" +
//            status + " For UUID" + ((ChicagoMessage)req).getId() + " and key " + new String(((ChicagoMessage)req).getKey()));

          ctx.writeAndFlush(new DefaultChicagoMessage(((ChicagoMessage) req).getId(), Op.fromInt(3), ((ChicagoMessage) req).getColFam(),
//              Boolean.toString(rocksDbImpl.write(((ChicagoMessage)req).getColFam(), ((ChicagoMessage)req).getKey(), ((ChicagoMessage)req).getVal())).getBytes(),
              Boolean.toString(db.write(((ChicagoMessage)req).getColFam(), ((ChicagoMessage)req).getKey(), encoder.encode((ChicagoMessage)req))).getBytes(),
              null)).addListener(writeComplete);

          break;
        case DELETE:
//        if (((ChicagoMessage)req).getKey().length == 0) {
//          status = rocksDbImpl.delete(((ChicagoMessage)req).getColFam());
//        } else {
//          status = rocksDbImpl.delete(((ChicagoMessage)req).getColFam(), ((ChicagoMessage)req).getKey());
//        }
//        readResponse = new byte[]{(byte) (status ? 1 : 0)};

          if (((ChicagoMessage) req).getKey().length == 0) {
            db.delete(((ChicagoMessage) req).getColFam());
          } else {
            db.delete(((ChicagoMessage) req).getColFam(), ((ChicagoMessage) req).getKey());
          }
//          readResponse = new byte[]{(byte) (status ? 1 : 0)};

          //dbLog.addDelete(finalMsg.getColFam(), finalMsg.getKey());
          break;
        case TS_WRITE:
          if (((ChicagoMessage) req).getKey().length == 0) {
//            String value = new String(((ChicagoMessage) req).getVal());
            if (new String(((ChicagoMessage) req).getVal()).contains(ChiUtil.delimiter)) {

              ctx.writeAndFlush(new DefaultChicagoMessage(((ChicagoMessage) req).getId(), Op.fromInt(3), ((ChicagoMessage) req).getColFam(), Boolean.toString(true).getBytes(),
                  db.batchWrite(((ChicagoMessage) req).getColFam(), ((ChicagoMessage) req).getVal()))).addListener(writeComplete);

//              readResponse = rocksDbImpl.batchWrite(((ChicagoMessage) req).getColFam(), value);
            } else {
//              readResponse = rocksDbImpl.tsWrite(((ChicagoMessage) req).getColFam(), ((ChicagoMessage) req).getVal());
              ctx.writeAndFlush(new DefaultChicagoMessage(((ChicagoMessage) req).getId(), Op.fromInt(3), ((ChicagoMessage) req).getColFam(), Boolean.toString(true).getBytes(),
                  db.tsWrite(((ChicagoMessage) req).getColFam(),encoder.encode((ChicagoMessage)req)))).addListener(writeComplete);
            }
          } else {
//            readResponse = rocksDbImpl.tsWrite(((ChicagoMessage) req).getColFam(), ((ChicagoMessage) req).getKey(), ((ChicagoMessage) req).getVal());
            ctx.writeAndFlush(new DefaultChicagoMessage(((ChicagoMessage) req).getId(), Op.fromInt(3), ((ChicagoMessage) req).getColFam(), Boolean.toString(true).getBytes(),
                db.tsWrite(((ChicagoMessage) req).getColFam(), ((ChicagoMessage) req).getKey(), encoder.encode((ChicagoMessage)req)))).addListener(writeComplete);
          }
//          if (readResponse != null) {
//            status = true;
//          }
          break;
        case STREAM:
//          readResponse = rocksDbImpl.stream(((ChicagoMessage) req).getColFam(), ((ChicagoMessage) req).getVal());
          ctx.writeAndFlush(new DefaultChicagoMessage(((ChicagoMessage) req).getId(), Op.fromInt(3), ((ChicagoMessage) req).getColFam(), Boolean.toString(true).getBytes(),
              db.stream(((ChicagoMessage) req).getColFam(), ((ChicagoMessage) req).getVal()))).addListener(writeComplete);
//          if (readResponse != null) {
//            status = true;
//          }
          break;

        default:
          break;
      }


//    needsToWrite = true;
//    ctx.writeAndFlush(new DefaultChicagoMessage(finalMsg.getId(), Op.fromInt(3), finalMsg.getColFam(), Boolean.toString(status).getBytes(), readResponse)).addListener(writeComplete);
//    ctx.writeAndFlush(new DefaultChicagoMessage(((ChicagoMessage)req).getId(), Op.fromInt(3), ((ChicagoMessage)req).getColFam(), Boolean.toString(status).getBytes(), readResponse), ctx.voidPromise());
    }
  }
}
