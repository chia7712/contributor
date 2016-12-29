package net.chia7712.contributor.operation;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Table;

public final class Worker implements Runnable {
  private static final long REPORT_PERIOOD = 5000; //sec
  private static final AtomicInteger ID = new AtomicInteger(0);
  private final int id = ID.getAndIncrement();
  private final int startIndex;
  private final int endIndex;
  private final byte[] cf;
  private final Table table;
  private final Slave slave;
  private final Durability durability;
  private long cellCount = 0;
  private long totalRows = 0;
  Worker(final int startIndex, final int endIndex, byte[] cf,
    final Durability durability, Table table, final Slave slave) {
    this.startIndex = startIndex;
    this.endIndex = endIndex;
    this.cf = cf;
    this.table = table;
    this.durability = durability;
    this.slave = slave;
  }
  public long getCellCount() {
    return cellCount;
  }
  public long getTotalRows() {
    return totalRows;
  }

  @Override
  public void run() {
    long startTime = System.currentTimeMillis();
    try {
      for (int index = startIndex; index != endIndex; ++index) {
        ++totalRows;
        cellCount += slave.work(table, index, cf, durability);
        if (System.currentTimeMillis() - startTime >= REPORT_PERIOOD) {
          System.out.println("#" + id + " "
                + totalRows + "/" + (endIndex - startIndex) + " current/total");
          startTime = System.currentTimeMillis();
        }
      }
      cellCount += slave.finish(table);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    } finally {
      try {
        table.close();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}
