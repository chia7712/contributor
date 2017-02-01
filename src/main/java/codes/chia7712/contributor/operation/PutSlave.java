package codes.chia7712.contributor.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class PutSlave {

  protected final List<Put> puts = new ArrayList<>();
  private final int qualifierNumber;
  private long cellCount = 0;
  private long rowCount = 0;
  public PutSlave(final int qualifierNumber) {
    this.qualifierNumber = qualifierNumber;
  }

  protected void prepareData(long rowIndex, Set<byte[]> cfs, Durability durability) {
    Put put = new Put(RowIndexer.createRow(rowIndex));
    put.setDurability(durability);
    byte[] value = Bytes.toBytes(rowIndex);
    ++rowCount;
    for (byte[] cf : cfs) {
      for (int i = 0; i != qualifierNumber; ++i) {
        put.addImmutable(cf, Bytes.toBytes(RowIndexer.getRandomData().getLong()), value);
        ++cellCount;
      }
    }
    puts.add(put);  
  }
  public long getCellCount() {
    return cellCount;
  }

  public long getRowCount() {
    return rowCount;
  }
  
}
