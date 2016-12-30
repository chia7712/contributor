package codes.chia7712.contributor.operation;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
class GetWorker implements Slave {
  @Override
  public int work(Table table, int rowIndex, byte[] cf, Durability durability) throws IOException {
    byte[] row = createRow(rowIndex);
    Get get = new Get(row);
    get.setMaxVersions();
    get.addColumn(cf, cf);
    Result result = table.get(get);
    return result.size();
  }
  
}
