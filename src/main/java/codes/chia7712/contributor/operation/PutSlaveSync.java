package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Table;

public class PutSlaveSync extends PutSlave implements Slave {
  private final Table table;
  public PutSlaveSync(Table table, final int qualifierNumber) {
    super(qualifierNumber);
    this.table = table;
  }

  @Override
  public void work(long rowIndex, Set<byte[]> cfs, Durability durability) throws IOException {
    prepareData(rowIndex, cfs, durability);
  }

  @Override
  public void complete() throws IOException, InterruptedException {
    try {
      table.put(puts);
    } finally {
      puts.clear();
      table.close();
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
    return false;
  }

  @Override
  public void close() throws IOException {
    table.close();
  }
  
}
