package codes.chia7712.contributor.operation;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Table;

class AppendWorker implements Slave {
  @Override
  public int work(Table table, int rowIndex, byte[] cf, Durability durability) throws IOException {
    byte[] row = createRow(rowIndex);
    Append app = new Append(row);
    app.add(cf, cf, row);
    app.setDurability(durability);
    table.append(app);
    return app.size();
  }
  
}
