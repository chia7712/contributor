package codes.chia7712.contributor.operation;

import java.io.IOException;

public interface Slave extends AutoCloseable {
  void updateRow(RowWork work) throws IOException, InterruptedException;
  ProcessMode getProcessMode();
  RequestMode getRequestMode();
  
}
