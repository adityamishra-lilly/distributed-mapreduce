package com.adityamishra.mapreduce.api;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class KeyValueTest {

  @Test
  void createsKeyValue() {
    KeyValue<String, Integer> kv = new KeyValue<>("hi", 7);
    assertNotNull(kv);
  }

  @Test
  void returnsKey() {
    KeyValue<String, Integer> kv = new KeyValue<>("hi", 7);
    assertEquals("hi", kv.getKey());
  }

  @Test
  void returnsValue() {
    KeyValue<String, Integer> kv = new KeyValue<>("hi", 7);
    assertEquals(7, kv.getValue());
  }

  @Test
  void returnsKeyAndValue() {
    KeyValue<String, Integer> kv = new KeyValue<>("hi", 7);
    assertAll(
        () -> assertEquals("hi", kv.getKey()),
        () -> assertEquals(7, kv.getValue()));
  }

}
