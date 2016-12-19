
package nl.uva.cpp;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.ToolRunner;

public class WordCount {

  public static void main(String[] args) {

    try {

//      example1(args);
//      runJar(args);
      Properties prop = new Properties();
      try (FileInputStream input = new FileInputStream(args[2])) {
        prop.load(input);
      } catch (FileNotFoundException ex) {
        Logger.getLogger(WordCount.class.getName()).log(Level.SEVERE, null, ex);
      } catch (IOException ex) {
        Logger.getLogger(WordCount.class.getName()).log(Level.SEVERE, null, ex);
      }

      String hadoopConfBaseDir = prop.getProperty("hadoop.conf.base.dir", "");
      String[] mrArgs = new String[]{args[0], args[1], hadoopConfBaseDir};

      long startRun2 = System.currentTimeMillis();
      args[1] = "output_WithToolRunner";
      mrArgs = new String[]{args[0], args[1], hadoopConfBaseDir};
      runJobWithToolRunner(mrArgs);
      long elapsedRun2 = System.currentTimeMillis() - startRun2;

      long startRun1 = System.currentTimeMillis();
      args[1] = "output";
      mrArgs = new String[]{args[0], args[1], hadoopConfBaseDir};
      runJob(mrArgs);
      long elapsedRun1 = System.currentTimeMillis() - startRun1;

      System.err.println("runJob elapsed: " + elapsedRun1 + "ms");
      System.err.println("runJobWithToolRunner elapsed: " + elapsedRun2 + "ms");

    } catch (Exception ex) {
      Logger.getLogger(WordCount.class.getName()).log(Level.SEVERE, null, ex);
    }

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
