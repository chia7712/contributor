package net.chia7712.contributor.operation;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

class BatchWorker implements Slave {
  private static final String BATCH_KEY = "chia7712.batch.size";
  private static final int BATCH_VALUE = 10;
  private final List<Put> puts = new LinkedList<>();
  private int flush(final Table table) throws IOException {
    if (!puts.isEmpty()) {
      table.put(puts);
      int size = puts.size();
      puts.clear();
      return size;
    }
    return 0;
  }
  @Override
  public int finish(final Table table) throws IOException {
    return flush(table);
  }
  @Override
  public int work(Table table, int rowIndex, byte[] cf, Durability durability) throws IOException {
    final int limit = table.getConfiguration().getInt(BATCH_KEY, BATCH_VALUE);
    byte[] row = createRow(rowIndex);
    Put put = new Put(row);
    put.addImmutable(cf, cf, row);
    put.setDurability(durability);
    puts.add(put);
    if (puts.size() >= limit) {
      return flush(table);
    }
    return 0;
  }
  
}
