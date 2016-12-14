/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package nl.uva.cpp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class WordCountTool extends Configured implements Tool {

//  private String HADOOP_CONF_BASE_DIR = "/cm/shared/package/hadoop/hadoop-2.5.0/etc/hadoop";
  public Job getJob(String[] args) throws IOException {
    Configuration conf = getConf();
    String[] subset = Arrays.copyOfRange(args, 3, args.length);
    conf = addPropertiesToConf(conf, subset);

//    printProps(conf);
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

    return job;
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = getJob(args);
    System.err.println("JobID: " + job.getJobID());
    return job.waitForCompletion(true) ? 0 : 1;
  }

  private Configuration addPropertiesToConf(Configuration conf, String[] args) {

    if (!args[0].equals("NULL")) {
      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, args[0]);
    }
    if (!args[1].equals("NULL")) {
      conf.set("mapreduce.framework.name", args[1]);
    }
    if (!args[2].equals("NULL")) {
      conf.set("yarn.resourcemanager.address", args[2]);
    }

//    try (FileInputStream input = new FileInputStream(arg)) {
//      Properties prop = new Properties();
//      prop.load(input);
//      Set<Object> keys = prop.keySet();
//      for (Object key : keys) {
//        String val = prop.getProperty((String) key);
//        conf.set((String) key, val);
//      }
//      File etc = new File(prop.getProperty("hadoop.conf.base.dir"));
//      File[] files = etc.listFiles(new FilenameFilter() {
//        @Override
//        public boolean accept(File dir, String name) {
//          return name.toLowerCase().endsWith(".xml");
//        }
//      });
//      if (files != null) {
//        for (File f : files) {
//          conf.addResource(new org.apache.hadoop.fs.Path(f.getAbsolutePath()));
//        }
//      }
//    }
    conf.set("mapreduce.map.class", WordCountMapper.class.getName());
    conf.set("mapreduce.reduce.class", WordCountReducer.class.getName());
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//    conf.set("mapred.jar", jar_Output_Folder+ java.io.File.separator + className+".jar");
    return conf;
  }

  private void printProps(Configuration conf) throws FileNotFoundException {
    String confprop = "";
    for (Map.Entry<String, String> entry : conf) {
      confprop += entry.getKey() + " : " + entry.getValue() + "\n";
    }
    System.err.println("STDERR: " + confprop);

    try (PrintWriter out = new PrintWriter(System.getProperty("user.home") + "/" + this.getClass().getName() + ".log")) {
      out.write(confprop);
    }
  }

  @Override
  public Configuration getConf() {
    Configuration conf = super.getConf();
    if (conf == null) {
      conf = new Configuration();
    }
    return conf;
  }
}
