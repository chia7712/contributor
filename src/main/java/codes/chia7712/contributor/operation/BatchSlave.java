package codes.chia7712.contributor.operation;

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
import org.apache.hadoop.hbase.util.Bytes;

public abstract class BatchSlave {
  protected final List<Row> rows = new ArrayList<>();
  private final Supplier<BatchType> types;
  private final int qualifierNumber;
  private long rowCount = 0;
  private long cellCount = 0;
  BatchSlave(final Supplier<BatchType> types, int qualifierNumber) {
    this.types = types;
    this.qualifierNumber = qualifierNumber;
    assert qualifierNumber > 0;
  }

  protected void prepareData(long rowIndex, Set<byte[]> cfs, Durability durability) {
    Row row = null;
    switch (types.get()) {
      case PUT:
        Put put = new Put(RowIndexer.createRow(rowIndex));
        put.setDurability(durability);
        byte[] value = Bytes.toBytes(rowIndex);
        for (byte[] cf : cfs) {
          for (int i = 0; i != qualifierNumber; ++i) {
            put.addImmutable(cf, Bytes.toBytes(RowIndexer.getRandomData().getLong()), value);
            ++cellCount;
          }
        }
        row = put;
        break;
      case DELETE:
        Delete delete = new Delete(RowIndexer.createRow(rowIndex));
        delete.setDurability(durability);
        for (byte[] cf : cfs) {
          for (int i = 0; i != qualifierNumber; ++i) {
            delete.addColumn(cf, Bytes.toBytes(RowIndexer.getRandomData().getLong()));
            ++cellCount;
          }
        }
        row = delete;
        break;
      case GET:
        Get get = new Get(RowIndexer.createRow(rowIndex));
        for (byte[] cf : cfs) {
          for (int i = 0; i != qualifierNumber; ++i) {
            get.addColumn(cf, Bytes.toBytes(RowIndexer.getRandomData().getLong()));
            ++cellCount;
          }
        }
        row = get;
        break;
      case INCREMENT:
        Increment inc = new Increment(RowIndexer.createRow(rowIndex));
        inc.setDurability(durability);
        for (byte[] cf : cfs) {
          for (int i = 0; i != qualifierNumber; ++i) {
            inc.addColumn(cf, Bytes.toBytes(RowIndexer.getRandomData().getLong()), rowIndex);
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

  public long getCellCount() {
    return cellCount;
  }

  public long getRowCount() {
    return rowCount;
  }
}
