package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class BatchSlave implements Slave {

  public enum Type {
    PUT, DELETE, GET, INCREMENT;
  }
  private final List<Row> rows = new ArrayList<>();
  private Object[] objs = null;
  private final Supplier<Type> types;
  private final int qualifierNumber;
  private long rowCount = 0;
  private long cellCount = 0;
  BatchSlave(final Supplier<Type> types, int qualifierNumber) {
    this.types = types;
    this.qualifierNumber = qualifierNumber;
    assert qualifierNumber > 0;
  }

  private Object[] getObjects() {
    if (objs == null || objs.length != rows.size()) {
      objs = new Object[rows.size()];
    }
    return objs;
  }

  @Override
  public void complete(final Table table) throws IOException, InterruptedException {
    try {
      table.batch(rows, getObjects());
    } finally {
      rows.clear();
    }
  }

  @Override
  public void work(Table table, long rowIndex, Set<byte[]> cfs, Durability durability) throws IOException {
    Row row = null;
    switch (types.get()) {
      case PUT:
        Put put = new Put(createRow(rowIndex));
        put.setDurability(durability);
        byte[] value = Bytes.toBytes(rowIndex);
        for (byte[] cf : cfs) {
          for (int i = 0; i != qualifierNumber; ++i) {
            put.addColumn(cf, Bytes.toBytes(RANDOM.getLong()), value);
            ++cellCount;
          }
        }
        row = put;
        break;
      case DELETE:
        Delete delete = new Delete(createRow(rowIndex));
        delete.setDurability(durability);
        for (byte[] cf : cfs) {
          for (int i = 0; i != qualifierNumber; ++i) {
            delete.addColumn(cf, Bytes.toBytes(RANDOM.getLong()));
            ++cellCount;
          }
        }
        row = delete;
        break;
      case GET:
        Get get = new Get(createRow(rowIndex));
        for (byte[] cf : cfs) {
          for (int i = 0; i != qualifierNumber; ++i) {
            get.addColumn(cf, Bytes.toBytes(RANDOM.getLong()));
            ++cellCount;
          }
        }
        row = get;
        break;
      case INCREMENT:
        Increment inc = new Increment(createRow(rowIndex));
        inc.setDurability(durability);
        for (byte[] cf : cfs) {
          for (int i = 0; i != qualifierNumber; ++i) {
            inc.addColumn(cf, Bytes.toBytes(RANDOM.getLong()), rowIndex);
            ++cellCount;
          }
        }
        row = inc;
        break;
      default:
        throw new RuntimeException("Why error?");
    }
    ++rowCount;
    rows.add(row);
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
