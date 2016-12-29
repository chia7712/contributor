package net.chia7712.contributor.operation;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

class BatchWorker implements Slave {
  private static final String BATCH_KEY = "chia7712.batch.size";
  private static final int BATCH_VALUE = 10;
  private final List<Row> rows = new LinkedList<>();
  private final Random rm = new Random();
  private final Map<Type, AtomicInteger> count = new HashMap<>();
  BatchWorker() {
    for (Type type : Type.values()) {
      count.put(type, new AtomicInteger(0));
    }
  }
  private int flush(final Table table) throws IOException, InterruptedException {
    if (!rows.isEmpty()) {
      table.batch(rows);
      int size = rows.size();
      rows.clear();
      return size;
    }
    return 0;
  }
  @Override
  public int finish(final Table table) throws IOException, InterruptedException {
    count.forEach((k, v) -> System.out.println(k + ":" + v));
    return flush(table);
  }
  @Override
  public int work(Table table, int rowIndex, byte[] cf, Durability durability) throws IOException, InterruptedException{
    final int limit = table.getConfiguration().getInt(BATCH_KEY, BATCH_VALUE);
    byte[] row = createRow(rowIndex);
    Type type = Type.values()[rm.nextInt(Type.values().length)];
    switch (type) {
      case PUT:
        Put put = new Put(row);
        put.addImmutable(cf, cf, Bytes.toBytes(rm.nextLong()));
        put.setDurability(durability);
        rows.add(put);
        break;
      case DELETE:
        Delete d = new Delete(row);
        d.addColumn(cf, cf);
        d.setDurability(durability);
        rows.add(d);
        break;
      case INCREMENT:
        Increment i = new Increment(row);
        i.addColumn(cf, cf, rm.nextLong());
        i.setDurability(durability);
        rows.add(i);
        break;
      default:
        throw new RuntimeException("Unsupported type:" + type);
    }
    count.get(type).incrementAndGet();
    if (rows.size() >= limit) {
      return flush(table);
    }
    return 0;
  }

  enum Type {
    DELETE, PUT, INCREMENT;
  }
}
