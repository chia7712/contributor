package codes.chia7712.contributor.view;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Progress implements Closeable {

  private static final Log LOG = LogFactory.getLog(Progress.class);
  private final long startTime = System.currentTimeMillis();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ExecutorService service = Executors.newSingleThreadExecutor();
  private final Consumer<String> output;
  public Progress(Supplier<Long> submittedRows, long totalRow) {
    this(submittedRows, totalRow, LOG::info);
  }

  public Progress(Supplier<Long> submittedRows, long totalRow, Consumer<String> output) {
    service.execute(() -> {
      long currentRow = 0;
      try {
        while (!closed.get() && (currentRow = submittedRows.get()) != totalRow) {
          TimeUnit.SECONDS.sleep(1);
          long elapsed = System.currentTimeMillis() - startTime;
          double average = (double) (currentRow * 1000) / (double) elapsed;
          long remaining = (long) ((totalRow - currentRow) / average);
          output.accept(currentRow + "/" + totalRow
                  + ", " + average + " rows/second"
                  + ", " + remaining + " seconds");
        }
      } catch (InterruptedException ex) {
        LOG.error("Breaking the sleep", ex);
      } finally {
        output.accept(submittedRows.get() + "/" + totalRow
                + ", elapsed:" + (System.currentTimeMillis() - startTime));
      }
    });
    this.output = output;
  }

  @Override
  public void close() throws IOException {
    output.accept("elapsed:" + (System.currentTimeMillis() - startTime));
    closed.set(true);
    try {
      service.shutdown();
      service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }
}
