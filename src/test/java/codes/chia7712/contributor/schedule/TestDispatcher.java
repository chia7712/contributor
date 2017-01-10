package codes.chia7712.contributor.schedule;

import codes.chia7712.contributor.schedule.Dispatcher.Packet;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TestDispatcher {

  @Test(timeout = 5000)
  public void testIteratorWithConstantBound() throws InterruptedException {
    testIterator(100, () -> 10, 5);
    testIterator(100, () -> 7, 5);
  }

  @Test(timeout = 5000)
  public void testIteratorWithRandomBound() throws InterruptedException {
    testIterator(100, () -> (int) (Math.random() * 10) + 1, 5);
  }

  private void testIterator(int max, Supplier<Integer> bound, int threads) throws InterruptedException {
    Dispatcher dispatcher = DispatcherFactory.get(max, bound);
    AtomicLong totalRows = new AtomicLong(0);
    ExecutorService service = Executors.newFixedThreadPool(threads);
    IntStream.range(0, threads).forEach(v -> {
      service.execute(() -> {
        Optional<Packet> packet;
        while ((packet = dispatcher.getPacket()).isPresent()) {
          int packetRows = 0;
          while (packet.get().hasNext()) {
            packet.get().next();
            ++packetRows;
            totalRows.incrementAndGet();
          }
          assertEquals(packet.get().size(), packetRows);
        }
      });
    });
    service.shutdown();
    service.awaitTermination(10, TimeUnit.SECONDS);
    assertEquals(max, totalRows.get());
  }

}
