package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.hbase.client.Durability;

public interface Slave {

  void work(final long rowIndex, final Set<byte[]> cfs, Durability durability) throws IOException;

  void complete() throws IOException, InterruptedException;

  long getCellCount();
  long getRowCount();
  boolean isAsync();
}
