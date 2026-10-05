package com.adityamishra.mapreduce.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class MapperTest {
  class TestMapper implements Mapper<Long, String, String, Integer> {
    @Override
    public void map(Long key, String value, Context<String, Integer> ctx) {
      for (String word : value.split("\\s+")) {
        ctx.write(word, 1);
      }
    }
  }

  @Test
  void emitsOnePairPerWord() {
    Mapper<Long, String, String, Integer> mapper = new TestMapper();
    Context<String, Integer> ctx = new Context<>();

    mapper.map(0L, "hello hello world", ctx);

    var out = ctx.read();
    assertEquals(3, out.size());
    assertEquals("hello", out.get(0).getKey());
    assertEquals(1, out.get(0).getValue());
    assertEquals("hello", out.get(1).getKey());
    assertEquals("world", out.get(2).getKey());
  }
}
