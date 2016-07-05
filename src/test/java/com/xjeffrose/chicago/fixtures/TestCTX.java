package com.xjeffrose.chicago.fixtures;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import java.net.SocketAddress;

public class TestCTX implements ChannelHandlerContext{
  @Override
  public Channel channel() {
    return null;
  }

  @Override
  public EventExecutor executor() {
    return new DefaultEventExecutor();
  }


  @Override
  public String name() {
    return null;
  }

  @Override
  public ChannelHandler handler() {
    return null;
  }

  @Override
  public boolean isRemoved() {
    return false;
  }

  @Override
  public ChannelHandlerContext fireChannelRegistered() {
    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelUnregistered() {
    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelActive() {
    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelInactive() {
    return null;
  }

  @Override
  public ChannelHandlerContext fireExceptionCaught(Throwable throwable) {
    return null;
  }

  @Override
  public ChannelHandlerContext fireUserEventTriggered(Object o) {
    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelRead(Object o) {
    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelReadComplete() {
    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelWritabilityChanged() {
    return null;
  }

  @Override
  public ChannelFuture bind(SocketAddress socketAddress) {
    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress socketAddress) {
    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress socketAddress, SocketAddress socketAddress1) {
    return null;
  }

  @Override
  public ChannelFuture disconnect() {
    return null;
  }

  @Override
  public ChannelFuture close() {
    return null;
  }

  @Override
  public ChannelFuture deregister() {
    return null;
  }

  @Override
  public ChannelFuture bind(SocketAddress socketAddress, ChannelPromise channelPromise) {
    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress socketAddress, ChannelPromise channelPromise) {
    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress socketAddress, SocketAddress socketAddress1, ChannelPromise channelPromise) {
    return null;
  }

  @Override
  public ChannelFuture disconnect(ChannelPromise channelPromise) {
    return null;
  }

  @Override
  public ChannelFuture close(ChannelPromise channelPromise) {
    return null;
  }

  @Override
  public ChannelFuture deregister(ChannelPromise channelPromise) {
    return null;
  }

  @Override
  public ChannelHandlerContext read() {
    return null;
  }

  @Override
  public ChannelFuture write(Object o) {
    return null;
  }

  @Override
  public ChannelFuture write(Object o, ChannelPromise channelPromise) {
    return null;
  }

  @Override
  public ChannelHandlerContext flush() {
    return null;
  }

  @Override
  public ChannelFuture writeAndFlush(Object o, ChannelPromise channelPromise) {
    return null;
  }

  @Override
  public ChannelFuture writeAndFlush(Object o) {
    return null;
  }

  @Override
  public ChannelPipeline pipeline() {
    return null;
  }

  @Override
  public ByteBufAllocator alloc() {
    return null;
  }

  @Override
  public ChannelPromise newPromise() {
    return null;
  }

  @Override
  public ChannelProgressivePromise newProgressivePromise() {
    return null;
  }

  @Override
  public ChannelFuture newSucceededFuture() {
    return null;
  }

  @Override
  public ChannelFuture newFailedFuture(Throwable throwable) {
    return null;
  }

  @Override
  public ChannelPromise voidPromise() {
    return null;
  }

  @Override
  public <T> Attribute<T> attr(AttributeKey<T> attributeKey) {
    return null;
  }

  @Override
  public <T> boolean hasAttr(AttributeKey<T> attributeKey) {
    return false;
  }
}
