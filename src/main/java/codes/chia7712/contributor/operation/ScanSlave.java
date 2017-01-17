
package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanSlave implements Slave {
  private final TreeSet<byte[]> rows = new TreeSet<>(Bytes.BYTES_COMPARATOR);
  private final Set<byte[]> columns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
  @Override
  public int work(Table table, long rowIndex, byte[] cf, Durability durability) throws IOException {
    rows.add(createRow(rowIndex));
    columns.add(cf);
    return 0;
  }

  private Scan createScan() {
    Scan scan = new Scan(rows.first(), rows.last());
    columns.forEach(v -> scan.addFamily(v));
    scan.setCaching(rows.size());
    scan.setBatch(Integer.MAX_VALUE);
    return scan;
  }

  @Override
  public int complete(Table table) throws IOException, InterruptedException {
    try (ResultScanner scanner = table.getScanner(createScan())) {
      int count = 0;
      for (Result r : scanner) {
        if (!r.isEmpty()) {
          ++count;
        }
        if (count >= rows.size()) {
          break;
        }
      }
      return count;
    } finally {
      rows.clear();
      columns.clear();
    }
  }
  
}
