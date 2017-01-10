package codes.chia7712.contributor.schedule;

import java.util.Iterator;
import java.util.Optional;

public interface Dispatcher {

  long getDispatchedRows();

  long getCommittedRows();

  Optional<Packet> getPacket();

  public interface Packet extends Iterator<Long> {

    long size();

    void commit();
  }
}
