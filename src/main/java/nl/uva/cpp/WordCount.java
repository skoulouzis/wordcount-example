
package nl.uva.cpp;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class WordCount {

  public static void main(String[] args) {
    try {
//      args = new String[]{System.getProperty("user.home") + "/Downloads/trainData", System.getProperty("user.home") + "/Downloads/out"};
      int retval = 0;

      retval = ToolRunner.run(new Configuration(), new WordCountTool(), args);
    } catch (Exception ex) {
      Logger.getLogger(WordCount.class.getName()).log(Level.SEVERE, null, ex);
    }

  }
}
