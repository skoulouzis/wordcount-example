
package nl.uva.cpp;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.ToolRunner;

public class WordCount {

  public static void main(String[] args) throws Throwable {
    try {

//      example1(args);
      example2(args);

    } catch (Exception ex) {
      Logger.getLogger(WordCount.class.getName()).log(Level.SEVERE, null, ex);
    }

  }

  private static void example1(String[] args) throws Exception {

//      args = new String[]{System.getProperty("user.home") + "/Downloads/trainData", System.getProperty("user.home") + "/Downloads/out"};
    int retval = 0;

    retval = ToolRunner.run(new Configuration(), new WordCountTool(), args);

  }

  private static void example2(String[] args) throws Throwable {
    RunJar.main(args);

  }
}
