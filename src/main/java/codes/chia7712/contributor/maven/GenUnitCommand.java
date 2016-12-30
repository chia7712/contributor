package codes.chia7712.contributor.maven;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class GenUnitCommand {
  private static final List<String> SKIPS = Arrays.asList(
  );
  private static final String EXTRA_OPTS = null;
  private static final String ISSUE = "hbase-17174";
  private static final int LIMIT = -1;
  private static final int PARALLER = 1;
  private static final String HOME = "/Users/chia7712";
  private static final String PATH = HOME + "/Dropbox/hbase-jira/" + ISSUE + "/unittest";
  private static final boolean ALL_TEST = true;
  public static void main(String[] args) throws IOException {
    Map<String, Double> packages = new TreeMap<>();
    File dir = new File(PATH);
    if (dir.exists()) {
      for (File f : dir.listFiles()) {
        packages.putAll(findLargeElapsed(f, LIMIT));
      }
      if (packages.isEmpty()) {
        System.out.println("No found of unittest file from the dir:" + PATH);
      }
    } else {
      System.out.println("No found of unittest dir:" + PATH);
    }
    SKIPS.forEach(v -> packages.put(v, Double.MAX_VALUE));
    packages.forEach((k, v) -> System.out.println(k + "\t" + v));
    System.out.println("Total:" + packages.size());
    System.out.println("----------------------");
    System.out.println(generate(packages));
  }
  public static String generate(Map<String, Double> packages) throws IOException {
    StringBuilder builder = new StringBuilder("mvn clean test -fae -Dtest.exclude.pattern=");
    packages.forEach((v, k) -> builder.append("**/").append(v).append(".java,"));
    builder.deleteCharAt(builder.length() - 1);
    builder.append(" -DsecondPartForkCount=")
           .append(PARALLER);
    if (ALL_TEST) {
      builder.append(" -PrunAllTests");
    }
    if (EXTRA_OPTS != null && EXTRA_OPTS.length() != 0) {
      builder.append(" ")
             .append(EXTRA_OPTS);
    }
    builder.append(" | tee ~/test_")
           .append(ISSUE);
    return builder.substring(0, builder.length());
  }
  private static Map<String, Double> findLargeElapsed(final File f, double limit) throws IOException {
    Map<String, Double> packages = new TreeMap<>();
    final String key1 = "Time elapsed: ";
    final String key2 = " sec - in ";
    try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.contains("Failures: 0, Errors: 0")) {
          continue;
        }
        int index1 = line.indexOf(key1);
        if (index1 == -1) {
          continue;
        }
        int index2 = line.indexOf(key2, index1);
        if (index2 == -1) {
          continue;
        }
        double time = Double.valueOf(line.substring(index1 + key1.length(), index2));
        String clz = line.substring(index2 + key2.length());
        if (time >= limit) {
          String[] arg = clz.split("\\.");
          packages.put(arg[arg.length - 1], time);
        }
      }
    }
    return packages;
  }
}
