package com.xjeffrose.chicago.client;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

public class RendezvousHash<N> {

  private final HashFunction hasher;
  private final Funnel<N> nodeFunnel;
  private final ConcurrentSkipListSet<N> nodeList;

  public RendezvousHash(Funnel<N> nodeFunnel, Collection<N> init) {
    this.hasher = Hashing.murmur3_128();
    this.nodeFunnel = nodeFunnel;
    this.nodeList = new ConcurrentSkipListSet<N>(init);
  }

  boolean remove(N node) {
    return nodeList.remove(node);
  }

  boolean add(N node) {
    return nodeList.add(node);
  }

  public List<N> get(byte[] key) {
    HashMap<Long, N> hashMap = new HashMap();
    List<N> _nodeList = new ArrayList<>();

      nodeList.stream()
          .filter(xs -> !_nodeList.contains(xs))
          .forEach(xs -> {
            hashMap.put(hasher.newHasher()
                .putBytes(key)
                .putObject(xs, nodeFunnel)
                .hash().asLong(), xs);

    });

    for (int i = 0; i < 3; i++) {
      _nodeList.add(hashMap.remove(hashMap.keySet().stream().max(Long::compare).orElse(null)));
    }

    return _nodeList;
  }

}