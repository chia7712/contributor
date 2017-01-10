package codes.chia7712.contributor.maven;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class GenMultiTests {

  private static final String ISSUE = "hbase-16992";
  private static final String PATH = "D:/Dropbox/hbase-jira/" + ISSUE + "/failedtests";

  public static void main(String[] args) throws IOException {
    List<String> failedTests = new LinkedList<>();
    for (File f : new File(PATH).listFiles()) {
      failedTests.addAll(readLine(f));
    }
    if (failedTests.isEmpty()) {
      System.out.println("No found of any failed tests");
    }
    StringBuilder sb = new StringBuilder("mvn clean test -Dtest=");
    failedTests.forEach(System.out::println);
    failedTests.forEach(s -> sb.append(s).append(","));
    System.out.println(sb.substring(0, sb.length() - 1));

  }

  private static List<String> readLine(File f) throws IOException {
    List<String> failedTests = new LinkedList<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.replace(" ", "");
        if (line.length() == 0 || line.length() < 5) {
          continue;
        }
        failedTests.add(line);
      }
    }
    return failedTests;
  }
}
