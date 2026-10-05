package com.adityamishra.mapreduce.api;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ContextTest {
  @Test
  void storesKeyValue() {
    Context<String, Integer> ctx = new Context<>();
    ctx.write("hello", 1);
    ctx.write("hello", 1);
    ctx.write("world", 1);
    ctx.write("world", 1);

    var store = ctx.read();
    assertEquals("hello", store.get(0).getKey());
    assertEquals("hello", store.get(1).getKey());
    assertEquals("world", store.get(2).getKey());
    assertEquals("world", store.get(3).getKey());
  }
}
