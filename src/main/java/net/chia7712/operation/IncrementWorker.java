package net.chia7712.operation;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Table;

class IncrementWorker implements Slave {
  @Override
  public int work(Table table, int rowIndex, byte[] cf, Durability durability) throws IOException {
    byte[] row = createRow(rowIndex);
    Increment incr = new Increment(row);
    incr.addColumn(cf, cf, 1);
    incr.setDurability(durability);
    table.increment(incr);
    return incr.size();
  }
  
}
