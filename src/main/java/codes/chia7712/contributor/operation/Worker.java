package codes.chia7712.contributor.operation;

import codes.chia7712.contributor.schedule.Dispatcher;
import codes.chia7712.contributor.schedule.Dispatcher.Packet;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Table;

public final class Worker implements Runnable {
  private static final Log LOG = LogFactory.getLog(Worker.class);
  private static final AtomicLong IDS = new AtomicLong(0);
  private final long id = IDS.getAndIncrement();
  private final Dispatcher dispatcher;
  private final Set<byte[]> cfs;
  private final Table table;
  private final Slave slave;
  private final Durability durability;
  private final CountDownLatch end = new CountDownLatch(1);
  private final AtomicLong rowCount = new AtomicLong(0);
  private final AtomicLong cellCount = new AtomicLong(0);
  Worker(Table table, Set<byte[]> cfs, final Durability durability,
          Dispatcher dispatcher, final Slave slave) {
    this.cfs = cfs;
    this.table = table;
    this.durability = durability;
    this.slave = slave;
    this.dispatcher = dispatcher;
  }

  public WorkResult getWorkResult() {
    return new WorkResult(slave.getRowCount(), slave.getCellCount());
  }

  @Override
  public void run() {
    LOG.info("Start #" + id);
    try {
      Optional<Packet> packet;
      while ((packet = dispatcher.getPacket()).isPresent()) {
        int count = 0;
        while (packet.get().hasNext()) {
          long next = packet.get().next();
          slave.work(table, next, cfs, durability);
        }
        slave.complete(table);
        packet.get().commit();
        rowCount.addAndGet(count);
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    } finally {
      try {
        end.countDown();
        table.close();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      } finally {
        LOG.info("Close #" + id + ", " + getWorkResult());
      }
    }
  }
}
