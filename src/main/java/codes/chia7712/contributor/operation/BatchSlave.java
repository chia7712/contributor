package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
  private final int columnNumber;

  BatchSlave(final Supplier<Type> types, int columnNumber) {
    this.types = types;
    this.columnNumber = columnNumber;
    assert columnNumber > 0;
  }

  private Object[] getObjects() {
    if (objs == null || objs.length != rows.size()) {
      objs = new Object[rows.size()];
    }
    return objs;
  }

  @Override
  public final int complete(final Table table) throws IOException, InterruptedException {
    try {
      table.batch(rows, getObjects());
      return rows.size();
    } finally {
      rows.clear();
    }
  }

  @Override
  public int work(Table table, long rowIndex, byte[] cf, Durability durability) throws IOException {
    Row row = null;
    switch (types.get()) {
      case PUT:
        Put put = new Put(createRow(rowIndex));
        byte[] value = Bytes.toBytes(rowIndex);
        for (int i = 0; i != columnNumber; ++i) {
          put.addColumn(cf, Bytes.toBytes(RANDOM.getLong()), value);
        }
        row = put;
        break;
      case DELETE:
        Delete delete = new Delete(createRow(rowIndex));
        for (int i = 0; i != columnNumber; ++i) {
          delete.addColumn(cf, Bytes.toBytes(RANDOM.getLong()));
        }
        row = delete;
        break;
      case GET:
        Get get = new Get(createRow(rowIndex));
        for (int i = 0; i != columnNumber; ++i) {
          get.addColumn(cf, Bytes.toBytes(RANDOM.getLong()));
        }
        row = get;
        break;
      case INCREMENT:
        Increment inc = new Increment(createRow(rowIndex));
        for (int i = 0; i != columnNumber; ++i) {
          inc.addColumn(cf, Bytes.toBytes(RANDOM.getLong()), rowIndex);
        }
        row = inc;
        break;
      default:
        throw new RuntimeException("Why error?");
    }
    rows.add(row);
    return 0;
  }
}
