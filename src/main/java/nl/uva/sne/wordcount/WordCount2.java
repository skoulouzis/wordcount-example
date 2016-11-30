/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package nl.uva.sne.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount2 {

  public static class TokenizerMapper
          extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable ONE = new IntWritable(1);
    private final Text WORD = new Text();

    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        WORD.set(itr.nextToken());
        context.write(WORD, ONE);
      }
    }
  }

  public static class IntSumReducer
          extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable RESULT = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
            Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      RESULT.set(sum);
      context.write(key, RESULT);
    }
  }

  public static void main(String[] args) throws Exception {
    args = new String[2];
    args[0] = System.getProperty("user.home") + "/Downloads/trainData";
    args[1] = System.getProperty("user.home") + "/Downloads/out";

    Configuration conf = new Configuration();

    conf.set("yarn.resourcemanager.address", "hadoop-master:8032");
    //mapred-site.xm
    conf.set("mapreduce.framework.name", "yarn");
    //dfs-site.xml
    conf.set("fs.default.name", "hdfs://hadoop-master:9000/");
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.submit();
//    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
