package com.adityamishra.mapreduce.api;

import java.util.List;

import java.util.ArrayList;
import java.util.Collections;

public class Context<K, V> {
  private final List<KeyValue<K, V>> store;

  public Context() {
    this.store = new ArrayList<>();
  }

  public void write(K key, V value) {
    store.add(new KeyValue<>(key, value));
  }

  public List<KeyValue<K, V>> read() {
    return Collections.unmodifiableList(store);
  }
}
