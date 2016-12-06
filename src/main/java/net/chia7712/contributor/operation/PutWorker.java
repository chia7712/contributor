package net.chia7712.contributor.operation;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

class PutWorker implements Slave {
  @Override
  public int work(Table table, int rowIndex, byte[] cf, Durability durability) throws IOException {
    byte[] row = createRow(rowIndex);
    Put put = new Put(row);
    put.addImmutable(cf, cf, row);
    put.setDurability(durability);
    table.put(put);
    return put.size();
  }
}
