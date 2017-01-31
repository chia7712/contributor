package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Durability;

public class PutSlaveAsync extends PutSlave implements Slave {
  private final AsyncTable table;
  public PutSlaveAsync(final AsyncTable table, final int qualifierNumber) {
    super(qualifierNumber);
    this.table = table;
  }

  @Override
  public void work(long rowIndex, Set<byte[]> cfs, Durability durability) throws IOException {
    prepareData(rowIndex, cfs, durability);
  }

  @Override
  public void complete() throws IOException, InterruptedException {
    try {
      table.put(puts)
           .forEach(v -> v.join());
    } finally {
      puts.clear();
    }
  }

  @Override
  public long getCellCount() {
    return super.getCellCount();
  }

  @Override
  public long getRowCount() {
    return super.getRowCount();
  }
  @Override
  public boolean isAsync() {
    return true;
  }
}
