package codes.chia7712.contributor.operation;

import codes.chia7712.contributor.schedule.Dispatcher;
import codes.chia7712.contributor.schedule.Dispatcher.Packet;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Durability;

public final class Worker implements Runnable {
  private static final Log LOG = LogFactory.getLog(Worker.class);
  private static final AtomicLong IDS = new AtomicLong(0);
  private final long id = IDS.getAndIncrement();
  private final Dispatcher dispatcher;
  private final Set<byte[]> cfs;
  private final Slave slave;
  private final Durability durability;
  private final CountDownLatch end = new CountDownLatch(1);
  private final AtomicLong rowCount = new AtomicLong(0);
  Worker(Set<byte[]> cfs, final Durability durability,
          Dispatcher dispatcher, final Slave slave) {
    this.cfs = cfs;
    this.durability = durability;
    this.slave = slave;
    this.dispatcher = dispatcher;
  }

  WorkResult getWorkResult() {
    return new WorkResult(slave.getRowCount(), slave.getCellCount());
  }

  @Override
  public void run() {
    LOG.info("Start " + (slave.isAsync() ? "ASYNC" : "SYNC") + "#" + id);
    try {
      Optional<Packet> packet;
      while ((packet = dispatcher.getPacket()).isPresent()) {
        int count = 0;
        while (packet.get().hasNext()) {
          long next = packet.get().next();
          slave.work(next, cfs, durability);
        }
        slave.complete();
        packet.get().commit();
        rowCount.addAndGet(count);
      }
    } catch (IOException | InterruptedException ex) {
      throw new RuntimeException(ex);
    } finally {
      closeSlave();
      end.countDown();
      LOG.info("Close "
        + (slave.isAsync() ? "ASYNC" : "SYNC") + "#" + id + ", " + getWorkResult());
    }
  }
  private void closeSlave() {
    try {
      slave.close();
    } catch (IOException ex) {
      LOG.error(ex);
    }
  }
}
