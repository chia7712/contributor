package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Table;

public class BatchSlaveSync extends BatchSlave implements Slave {
  private final Table table;
  private Object[] objs = null;
  BatchSlaveSync(final Table table, final Supplier<BatchType> types, int qualifierNumber) {
    super(types, qualifierNumber);
    this.table = table;
  }

  private Object[] getObjects() {
    if (objs == null || objs.length != rows.size()) {
      objs = new Object[rows.size()];
    }
    return objs;
  }

  @Override
  public void complete() throws IOException, InterruptedException {
    try {
      table.batch(rows, getObjects());
    } finally {
      rows.clear();
      table.close();
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
    return false;
  }
}
