package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class PutSlave implements Slave {

  private final List<Put> puts = new ArrayList<>();
  private final int qualifierNumber;
  private long cellCount = 0;
  private long rowCount = 0;
  public PutSlave(final int qualifierNumber) {
    this.qualifierNumber = qualifierNumber;
  }

  @Override
  public void work(Table table, long rowIndex, Set<byte[]> cfs, Durability durability) throws IOException {
    Put put = new Put(createRow(rowIndex));
    put.setDurability(durability);
    byte[] value = Bytes.toBytes(rowIndex);
    ++rowCount;
    for (byte[] cf : cfs) {
      for (int i = 0; i != qualifierNumber; ++i) {
        put.addColumn(cf, Bytes.toBytes(RANDOM.getLong()), value);
        ++cellCount;
      }
    }
    puts.add(put);
  }

  @Override
  public void complete(Table table) throws IOException, InterruptedException {
    try {
      table.put(puts);
    } finally {
      puts.clear();
    }
  }
  @Override
  public long getCellCount() {
    return cellCount;
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }
  
}
