/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package nl.uva.cpp;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {
    int sum = 0;
    int count = 0;
    for (IntWritable val : values) {
      sum += val.get();
      count++;
    }
    /*String value = String.valueOf(count) + "\t" + String.valueOf(sum);
		context.write(key, new Text(value));*/
    context.write(key, new IntWritable(sum));
  }
}
