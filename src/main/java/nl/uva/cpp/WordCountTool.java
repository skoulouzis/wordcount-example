/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package nl.uva.cpp;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class WordCountTool extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    FileInputStream input = new FileInputStream(args[args.length - 1]);
    Properties prop = new Properties();
    prop.load(input);
    Set<Object> keys = prop.keySet();
    for (Object key : keys) {
      String val = prop.getProperty((String) key);
      conf.set((String) key, val);
    }

    Job job = Job.getInstance(conf);
    job.setJarByClass(this.getClass());

    // Set the input and output paths for the job, to the paths given
    // on the command line.
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Use our mapper and reducer classes.
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);

    // Our input file is a text file.
    job.setInputFormatClass(TextInputFormat.class);

    // Our output is a mapping of text to integers. (See the tutorial for
    // some notes about how you could map from text to text instead.)
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
}
