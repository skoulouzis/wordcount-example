
package nl.uva.cpp;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

public class WordCount {

  public static void main(String[] args) throws Exception {
    int retval = ToolRunner.run(new Configuration(), new WordCountTool(), args);

  }
}
