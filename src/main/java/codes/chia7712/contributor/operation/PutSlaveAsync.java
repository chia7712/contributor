package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;

public class PutSlaveAsync extends BatchSlave implements Slave {
  private static final int MAX_BATCH_SIZE = 50;
  private final AsyncTable table;
  private final AtomicInteger count = new AtomicInteger(0);
  public PutSlaveAsync(final AsyncTable table, final int qualifierNumber) {
    super(() -> BatchType.PUT, qualifierNumber);
    this.table = table;
  }

  @Override
  public void work(long rowIndex, Set<byte[]> cfs, Durability durability) throws IOException {
    prepareData(rowIndex, cfs, durability);
  }

  @Override
  public void complete() throws IOException, InterruptedException {
    try {
      List<Put> puts = rows.stream().map(v -> (Put)v).collect(Collectors.toList());
      CompletableFuture fut = table.putAll(puts);
      count.incrementAndGet();
      fut.whenComplete((v, e) -> count.decrementAndGet());
      waitForRunner(MAX_BATCH_SIZE);
    } finally {
      rows.clear();
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

  @Override
  public void close() throws IOException {
    waitForRunner(0);
  }

  private void waitForRunner(int expectedSize) throws IOException {
    while (count.get() > expectedSize) {
      try {
        TimeUnit.MICROSECONDS.sleep(100);
      } catch (InterruptedException ex) {
        throw new IOException(ex);
      }
    }
  }
}
