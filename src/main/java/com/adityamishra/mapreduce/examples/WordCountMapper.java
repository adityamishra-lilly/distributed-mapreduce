package com.adityamishra.mapreduce.examples;

import com.adityamishra.mapreduce.api.Mapper;
import com.adityamishra.mapreduce.api.Context;

public class WordCountMapper implements Mapper<Long, String, String, Integer> {
  @Override
  public void map(Long key, String value, Context<String, Integer> ctx) {
    for (String word : value.split("\\s+")) {
      ctx.write(word, 1);
    }
  }
}
