package com.xjeffrose.chicago.client;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import io.netty.util.internal.PlatformDependent;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RendezvousHash<N> implements NodeListener<N>{
  private static final Logger log = LoggerFactory.getLogger(RendezvousHash.class.getName());

  private final HashFunction hasher;
  private final Funnel<N> nodeFunnel;
    private final int quorum;

  private List<N> nodeList;

    public RendezvousHash(Funnel<N> nodeFunnel, Collection<N> init, int quorum) {
    this.hasher = Hashing.murmur3_128();
    this.nodeFunnel = nodeFunnel;
    this.nodeList = Collections.synchronizedList(new ArrayList<N>(init));
    this.quorum = quorum;
  }

  public boolean remove(N node) {
    return nodeList.remove(node);
  }

  public boolean add(N node) {
    return nodeList.add(node);
  }

  public List<N> get(byte[] key) {
//    while (nodeList.size() < quorum) {
//      try {
//        Thread.sleep(0, 50);
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }
//    }
    Map<Long, N> hashMap = PlatformDependent.newConcurrentHashMap();
    List<N> _nodeList = new ArrayList<>();

      nodeList.stream()
          .filter(xs -> !_nodeList.contains(xs))
          .forEach(xs -> {
            hashMap.put(hasher.newHasher()
                .putBytes(key)
                .putObject(xs, nodeFunnel)
                .hash().asLong(), xs);

    });

    for (int i = 0; i < quorum; i++) {
      try {
        _nodeList.add(hashMap.remove(hashMap.keySet().stream().max(Long::compare).orElse(null)));
      } catch (NullPointerException e) {
        return nodeList;
      }
    }
    return _nodeList;
  }

  @Override
  public void nodeAdded(N node) {
    add(node);
  }

  @Override
  public void nodeRemoved(N node) {
    remove(node);
  }
}
