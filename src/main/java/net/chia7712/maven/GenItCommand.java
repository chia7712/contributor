
package net.chia7712.maven;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class GenItCommand {
  private static final String ISSUE = "hbase-16224";
  private static final String PATH = System.getProperty("user.home") + "/Dropbox/hbase-jira/" + ISSUE + "/ittest";
  public static void main(String[] args) throws IOException {
    Set<String> passed = new TreeSet<>();
    for (File f : new File(PATH).listFiles()) {
      passed.addAll(findPassed(f));
    }
    String cmd = genCommand(passed);
    System.out.println(cmd);
  }
  public static String genCommand(Set<String> passed) {
    StringBuilder buf = new StringBuilder();
    buf.append("mvn verify -Dunittest.include=**/Test*.java");
    passed.forEach((s) -> {
      buf.append(",**/")
        .append(s)
        .append(".java");
    });
    return buf.toString();
  }
  public static List<String> findPassed(File f) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
      List<String> rval = new LinkedList<>();
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.contains("Running org.apache.hadoop.hbase")) {
          String[] args = line.split("\\.");
          rval.add(args[args.length - 1]);
        }
      }
      return rval;
    }
  }
}
