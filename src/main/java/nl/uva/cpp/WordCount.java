
package nl.uva.cpp;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.ToolRunner;

public class WordCount {

  public static void main(String[] args) throws Throwable {

//      example1(args);
//      runJar(args);
    Properties prop = new Properties();
    try (FileInputStream input = new FileInputStream(args[3])) {
      prop.load(input);
    }
    String defaultFS = prop.getProperty(FileSystem.FS_DEFAULT_NAME_KEY, "NULL");
    String mapreduceFramework = prop.getProperty("mapreduce.framework.name", "NULL");
    String yarnResourcemanage = prop.getProperty("yarn.resourcemanager.address", "NULL");

    String[] mrArgs = new String[]{args[0], args[1], args[2], defaultFS, mapreduceFramework, yarnResourcemanage};
    long startRun1 = System.currentTimeMillis();

    runJob(mrArgs);
    long elapsedRun1 = System.currentTimeMillis() - startRun1;

    long startRun2 = System.currentTimeMillis();
    args[1] = "output_WithToolRunner";
    defaultFS = "NULL";// prop.getProperty(FileSystem.FS_DEFAULT_NAME_KEY);
    mapreduceFramework = "NULL";//prop.getProperty("mapreduce.framework.name");
    yarnResourcemanage = "NULL";//prop.getProperty("yarn.resourcemanager.address");
    mrArgs = new String[]{args[0], args[1], args[2], defaultFS, mapreduceFramework, yarnResourcemanage};
    runJobWithToolRunner(mrArgs);
    long elapsedRun2 = System.currentTimeMillis() - startRun2;

    System.err.println("runJob elapsed: " + elapsedRun1 + "ms");
    System.err.println("runJobWithToolRunner elapsed: " + elapsedRun2 + "ms");

  }

  private static void runJobWithToolRunner(String[] args) throws Exception {

//    args = new String[]{System.getProperty("user.home") + "/Downloads/trainData", System.getProperty("user.home") + "/Downloads/out"};
    int retval = 0;

    retval = ToolRunner.run(new Configuration(), new WordCountTool(), args);

  }

  private static void runJar(String[] args) throws Throwable {
    RunJar.main(args);
  }

  private static void runJob(String[] args) throws Exception {
    WordCountTool wc = new WordCountTool();
    wc.run(args);
  }
}
