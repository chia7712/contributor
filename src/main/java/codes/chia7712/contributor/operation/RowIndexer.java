package codes.chia7712.contributor.operation;

import codes.chia7712.contributor.data.RandomData;
import codes.chia7712.contributor.data.RandomDataFactory;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;


public class RowIndexer {
  private static final RandomData RANDOM = RandomDataFactory.create();
  private static final List<String> KEYS = Arrays.asList(
          "0-",
          "1-",
          "2-",
          "3-",
          "4-",
          "5-",
          "6-",
          "7-",
          "8-",
          "9-");
  public static RandomData getRandomData() {
    return RANDOM;
  }
  public static byte[] createRow(long currentIndex) {
    int index = RANDOM.getInteger(KEYS.size());
    return Bytes.toBytes(KEYS.get(index) + currentIndex + "-" + RANDOM.getLong());
  }
}
