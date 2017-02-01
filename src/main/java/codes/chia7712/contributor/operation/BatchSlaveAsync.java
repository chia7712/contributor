package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Row;

public class BatchSlaveAsync extends BatchSlave implements Slave {
  private final AsyncTable table;
  private final AtomicInteger count = new AtomicInteger(0);
  BatchSlaveAsync(final AsyncTable table, final Supplier<BatchType> types, int qualifierNumber) {
    super(types, qualifierNumber);
    this.table = table;
  }

  @Override
  public void complete() throws IOException, InterruptedException {
    try {
      List<CompletableFuture<Row>> futs = table.batch(rows);
      count.incrementAndGet();
      CompletableFuture.allOf(futs.toArray(new CompletableFuture[futs.size()]))
              .whenComplete((v, e) -> count.decrementAndGet());
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

  @Override
  public void close() throws IOException {
    while (true) {
      if (count.get() == 0) {
        return;
      }
      try {
        TimeUnit.MICROSECONDS.sleep(100);
      } catch (InterruptedException ex) {
        throw new IOException(ex);
      }
    }
  }
}
