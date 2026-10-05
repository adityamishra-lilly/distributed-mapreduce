package com.adityamishra.mapreduce.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.adityamishra.mapreduce.api.Mapper;
import com.adityamishra.mapreduce.api.Context;

public class WordCountMapperTest {
  @Test
  void emitsWordEveryOccurrence() {
    Mapper<Long, String, String, Integer> mapper = new WordCountMapper();
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
