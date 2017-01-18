package codes.chia7712.contributor.operation;

import codes.chia7712.contributor.data.RandomData;
import codes.chia7712.contributor.data.RandomDataFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public interface Slave {

  static final RandomData RANDOM = RandomDataFactory.create();
  static final List<String> KEYS = Arrays.asList(
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

  void work(final Table table, final long rowIndex,
          final Set<byte[]> cfs, Durability durability) throws IOException;

  void complete(final Table table) throws IOException, InterruptedException;

  long getCellCount();
  long getRowCount();
  default byte[] createRow(long currentIndex) {
    int index = RANDOM.getInteger(KEYS.size());
    return Bytes.toBytes(KEYS.get(index) + currentIndex + "-" + RANDOM.getLong());
  }
}
