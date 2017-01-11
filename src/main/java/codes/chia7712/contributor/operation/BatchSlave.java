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

  BatchSlave(final Supplier<Type> types) {
    this.types = types;
  }

  private Object[] getObjects() {
    if (objs == null || objs.length != rows.size()) {
      objs = new Object[rows.size()];
    }
    return objs;
  }

  @Override
  public final void completePacket(final Table table) throws IOException, InterruptedException {
    table.batch(rows, getObjects());
    rows.clear();
  }

  @Override
  public void work(Table table, long rowIndex, byte[] cf, Durability durability) throws IOException {
    Row row = null;
    byte[] qual = Bytes.toBytes(RANDOM.getLong());
    switch (types.get()) {
      case PUT:
        Put put = new Put(createRow(rowIndex));
        put.addColumn(cf, qual, Bytes.toBytes(rowIndex));
        row = put;
        break;
      case DELETE:
        Delete delete = new Delete(createRow(rowIndex));
        delete.addColumn(cf, qual);
        row = delete;
        break;
      case GET:
        Get get = new Get(createRow(rowIndex));
        get.addColumn(cf, qual);
        row = get;
        break;
      case INCREMENT:
        Increment inc = new Increment(createRow(rowIndex));
        inc.addColumn(cf, qual, rowIndex);
        row = inc;
        break;
      default:
        throw new RuntimeException("Why error?");
    }
    rows.add(row);
  }
}
