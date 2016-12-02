/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package nl.uva.cpp;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();

  static enum Counters {
    INPUT_WORDS
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString().toLowerCase();
    StringTokenizer itr = new StringTokenizer(line);
    
    while (itr.hasMoreTokens()) {
      String token = itr.nextToken();
      word.set(token);
      context.write(word, one);
      context.getCounter(Counters.INPUT_WORDS).increment(1);
    }
  }
}
