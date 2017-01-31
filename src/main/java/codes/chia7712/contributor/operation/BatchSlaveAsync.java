package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Durability;

public class BatchSlaveAsync extends BatchSlave implements Slave {
  private final AsyncTable table;
  BatchSlaveAsync(final AsyncTable table, final Supplier<BatchType> types, int qualifierNumber) {
    super(types, qualifierNumber);
    this.table = table;
  }

  @Override
  public void complete() throws IOException, InterruptedException {
    try {
      table.batch(rows).forEach(v -> v.join());
    } finally {
      rows.clear();
    }
  }

  @Override
  public void work(long rowIndex, Set<byte[]> cfs, Durability durability) throws IOException {
    prepareData(rowIndex, cfs, durability);
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
