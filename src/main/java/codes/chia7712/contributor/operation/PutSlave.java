package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class PutSlave implements Slave {

  private final List<Put> puts = new ArrayList<>();
  private final int qualifierNumber;

  public PutSlave(final int qualifierNumber) {
    this.qualifierNumber = qualifierNumber;
  }

  @Override
  public void work(Table table, long rowIndex, byte[] cf, Durability durability) throws IOException {
    Put put = new Put(createRow(rowIndex));
    byte[] value = Bytes.toBytes(rowIndex);
    for (int i = 0; i != qualifierNumber; ++i) {
      put.addColumn(cf, Bytes.toBytes(RANDOM.getLong()), value);
    }
    puts.add(put);
  }

  @Override
  public void completePacket(Table table) throws IOException, InterruptedException {
    table.put(puts);
    puts.clear();
  }

}
