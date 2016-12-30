
package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public interface Slave {
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
  int work(final Table table, final int rowIndex,
    final byte[] cf, Durability durability) throws IOException, InterruptedException;
  default int finish(final Table table) throws IOException, InterruptedException {
    return 0;
  }
  default byte[] createRow(int currentIndex) {
    int index = (int) (Math.random() * KEYS.size());
    return Bytes.toBytes(KEYS.get(index) + currentIndex);
  }
}
