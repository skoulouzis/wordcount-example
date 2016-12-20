
package nl.uva.cpp;

import java.io.File;
import java.io.FilenameFilter;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class WordCountTool extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    if (args.length > 2 && args[2] != null) {
      conf = addPropertiesToConf(conf, args[2]);
//      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, args[3]);
//      conf.set("mapreduce.framework.name", "yarn");
//      conf.set("yarn.resourcemanager.address", args[4]);
//      conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
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

    // Limit the number of reduce/map classes to what was specified on
    // the command line.
//		int numTasks = Integer.valueOf(args[2]);
//		job.setNumReduceTasks(numTasks);
//		job.getConfiguration().setInt("mapred.max.split.size", 750000 / numTasks);
    // This limits the number of running mappers, but not the total.
    // job.getConfiguration().setInt("mapreduce.job.running.map.limit", numTasks);
    // Run the job!
    return job.waitForCompletion(true) ? 0 : 1;
  }

  private Configuration addPropertiesToConf(Configuration conf, String etcPath) {
    File etc = new File(etcPath);
    File[] files = etc.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.toLowerCase().endsWith(".xml");
      }
    });
    if (files != null) {
      for (File f : files) {
        conf.addResource(new org.apache.hadoop.fs.Path(f.getAbsolutePath()));
      }
      conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    } else {
//      conf.set("mapreduce.framework.name", "local");
//      conf.set("mapred.job.tracker", "local");
//      conf.set("fs.defaultFS", "file:///");
    }
    return conf;
  }
}
