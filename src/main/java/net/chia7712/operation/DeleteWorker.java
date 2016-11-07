
package net.chia7712.operation;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Table;

class DeleteWorker implements Slave {
  @Override
  public int work(Table table, int rowIndex, byte[] cf, Durability durability) throws IOException {
    byte[] row = createRow(rowIndex);
    Delete del = new Delete(row);
    del.addColumn(cf, cf);
    del.setDurability(durability);
    table.delete(del);
    return del.size();
  }
  
}
