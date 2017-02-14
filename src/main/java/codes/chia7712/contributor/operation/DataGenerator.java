package codes.chia7712.contributor.operation;

import codes.chia7712.contributor.schedule.Dispatcher;
import codes.chia7712.contributor.schedule.DispatcherFactory;
import codes.chia7712.contributor.view.Arguments;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

public class DataGenerator {

  private static final Log LOG = LogFactory.getLog(DataGenerator.class);
  private static final String NUMBER_OF_THREAD = "threads";
  private static final String TABLE_NAME = "table";
  private static final String NUMBER_OF_ROW = "rows";
  private static final String BATCH_SIZE = "batchSize";
  private static final String NUMBER_OF_QUALIFIER = "qualCount";
  private static final String CELL_SIZE = "cellSize";
  private static final String FLUSH_AT_THE_END = "flushtable";
  private static final String LARGE_QUALIFIER = "largequal";

  public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
    Arguments arguments = new Arguments(
            Arrays.asList(
                    NUMBER_OF_THREAD,
                    TABLE_NAME,
                    NUMBER_OF_ROW),
            Arrays.asList(ProcessMode.class.getSimpleName(),
                    RequestMode.class.getSimpleName(),
                    DataType.class.getSimpleName(),
                    Durability.class.getSimpleName(),
                    BATCH_SIZE,
                    NUMBER_OF_QUALIFIER,
                    CELL_SIZE,
                    FLUSH_AT_THE_END,
                    LARGE_QUALIFIER),
            Arrays.asList(
                    getDescription(ProcessMode.class.getSimpleName(), ProcessMode.values()),
                    getDescription(RequestMode.class.getSimpleName(), RequestMode.values()),
                    getDescription(DataType.class.getSimpleName(), DataType.values()),
                    getDescription(Durability.class.getSimpleName(), Durability.values()))
    );
    arguments.validate(args);
    final int threads = arguments.getInt(NUMBER_OF_THREAD);
    final TableName tableName = TableName.valueOf(arguments.get(TABLE_NAME));
    final int totalRows = arguments.getInt(NUMBER_OF_ROW);
    final Optional<ProcessMode> processMode = ProcessMode.find(arguments.get(ProcessMode.class.getSimpleName()));
    final Optional<RequestMode> requestMode = RequestMode.find(arguments.get(RequestMode.class.getSimpleName()));
    final Optional<DataType> dataType = DataType.find(arguments.get(DataType.class.getSimpleName()));
    final Durability durability = arguments.get(Durability.class, Durability.USE_DEFAULT);
    final int batchSize = arguments.getInt(BATCH_SIZE, 100);
    final int qualCount = arguments.getInt(NUMBER_OF_QUALIFIER, 1);
    final Set<byte[]> families = findColumn(tableName);
    final int cellSize = arguments.getInt(CELL_SIZE, -1);
    final boolean needFlush = arguments.getBoolean(FLUSH_AT_THE_END, true);
    final boolean largeQual = arguments.getBoolean(LARGE_QUALIFIER, false);
    ExecutorService service = Executors.newFixedThreadPool(threads,
            Threads.newDaemonThreadFactory("-" + DataGenerator.class.getSimpleName()));
    Dispatcher dispatcher = DispatcherFactory.get(totalRows, batchSize);
    DataStatistic statistic = new DataStatistic();
    try (ConnectionWrap conn = new ConnectionWrap(processMode, requestMode,
            needFlush ? tableName : null)) {
      List<CompletableFuture> slaves = new ArrayList<>(threads);
      Map<SlaveCatalog, AtomicInteger> slaveCatalog = new TreeMap<>();
      for (int i = 0; i != threads; ++i) {
        Slave slave = conn.createSlave(tableName, statistic, batchSize);
        LOG.info("Starting #" + i + " " + slave);
        slaveCatalog.computeIfAbsent(new SlaveCatalog(slave), k -> new AtomicInteger(0))
                .incrementAndGet();
        CompletableFuture fut = CompletableFuture.runAsync(() -> {
          Optional<Dispatcher.Packet> packet;
          RowWork.Builder builder = RowWork.newBuilder()
                  .setDurability(durability)
                  .setFamilies(families)
                  .setCellSize(cellSize)
                  .setQualifierCount(qualCount)
                  .setLargeQualifier(largeQual);
          try {
            while ((packet = dispatcher.getPacket()).isPresent()) {
              while (packet.get().hasNext()) {
                long next = packet.get().next();
                slave.updateRow(builder
                        .setBatchType(getDataType(dataType))
                        .setRowIndex(next)
                        .build());
              }
              packet.get().commit();
            }
          } catch (IOException | InterruptedException ex) {
            LOG.error(ex);
          } finally {
            try {
              slave.close();
            } catch (Exception ex) {
              LOG.error(ex);
            }
          }

        }, service);
        slaves.add(fut);
      }
      AtomicBoolean stop = new AtomicBoolean(false);
      CompletableFuture logger = CompletableFuture.runAsync(() -> {
        final long startTime = System.currentTimeMillis();
        try {
          while (!stop.get()) {
            log(statistic, totalRows, startTime);
            TimeUnit.SECONDS.sleep(2);
          }
        } catch (InterruptedException ex) {
          LOG.error(ex);
        } finally {
          log(statistic, totalRows, startTime);
        }
      });
      slaves.forEach(CompletableFuture::join);
      stop.set(true);
      logger.join();
      LOG.info("threads:" + threads
              + ", tableName:" + tableName
              + ", totalRows:" + totalRows
              + ", " + ProcessMode.class.getSimpleName() + ":" + processMode
              + ", " + RequestMode.class.getSimpleName() + ":" + requestMode
              + ", " + DataType.class.getSimpleName() + ":" + dataType
              + ", " + Durability.class.getSimpleName() + ":" + durability
              + ", batchSize:" + batchSize
              + ", qualCount:" + qualCount);
      slaveCatalog.forEach((k, v) -> LOG.info(k + " " + v));
    }
  }

  private static class SlaveCatalog implements Comparable<SlaveCatalog> {

    private final ProcessMode processMode;
    private final RequestMode requestMode;

    SlaveCatalog(Slave slave) {
      this(slave.getProcessMode(), slave.getRequestMode());
    }

    SlaveCatalog(final ProcessMode processMode, RequestMode requestMode) {
      this.processMode = processMode;
      this.requestMode = requestMode;
    }

    @Override
    public int compareTo(SlaveCatalog o) {
      int rval = processMode.compareTo(o.processMode);
      if (rval != 0) {
        return rval;
      }
      return requestMode.compareTo(o.requestMode);
    }

    @Override
    public String toString() {
      return processMode + "/" + requestMode;
    }
  }

  private static void log(DataStatistic statistic, int totalRows, long startTime) {
    long elapsed = System.currentTimeMillis() - startTime;
    LOG.info("------------------------");
    LOG.info("total rows:" + totalRows);
    LOG.info("elapsed(ms):" + elapsed);
    LOG.info("commit:" + statistic.getCommittedRows());
    LOG.info("processing:" + statistic.getProcessingRows());
    statistic.consume((r, i) -> LOG.info(r.toString() + ":" + i));
  }

  private static DataType getDataType(Optional<DataType> type) {
    if (type.isPresent()) {
      return type.get();
    } else {
      int index = (int) (Math.random() * DataType.values().length);
      return DataType.values()[index];
    }
  }

  private static Set<byte[]> findColumn(TableName tableName) throws IOException {
    try (Connection conn = ConnectionFactory.createConnection();
            Admin admin = conn.getAdmin()) {
      HTableDescriptor desc = admin.getTableDescriptor(tableName);
      Set<byte[]> columns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      for (HColumnDescriptor col : desc.getColumnFamilies()) {
        columns.add(col.getName());
      }
      return columns;
    }
  }

  public static String getDescription(String name, Enum[] ops) {
    StringBuilder builder = new StringBuilder(name + ":");
    for (Enum op : ops) {
      builder.append(op.name())
              .append(",");
    }
    builder.deleteCharAt(builder.length() - 1);
    return builder.toString();
  }

  private static class ConnectionWrap implements Closeable {

    private final ProcessMode processMode;
    private final RequestMode requestMode;
    private final AsyncConnection asyncConn;
    private final Connection conn;
    private final TableName nameToFlush;

    ConnectionWrap(Optional<ProcessMode> processMode,
            Optional<RequestMode> requestMode, TableName nameToFlush) throws IOException {
      this.processMode = processMode.orElse(null);
      this.requestMode = requestMode.orElse(null);
      this.nameToFlush = nameToFlush;
      if (processMode.isPresent()) {
        switch (processMode.get()) {
          case SYNC:
            asyncConn = null;
            conn = ConnectionFactory.createConnection();
            break;
          case ASYNC:
            conn = nameToFlush != null ? ConnectionFactory.createConnection() : null;
            asyncConn = ConnectionFactory.createAsyncConnection();
            break;
          default:
            throw new IllegalArgumentException("Unknown type:" + processMode.get());
        }
      } else {
        asyncConn = ConnectionFactory.createAsyncConnection();
        conn = ConnectionFactory.createConnection();
      }
    }

    private ProcessMode getProcessMode() {
      if (processMode != null) {
        return processMode;
      } else {
        int index = (int) (Math.random() * ProcessMode.values().length);
        return ProcessMode.values()[index];
      }
    }

    private RequestMode getRequestMode() {
      if (requestMode != null) {
        return requestMode;
      } else {
        int index = (int) (Math.random() * RequestMode.values().length);
        return RequestMode.values()[index];
      }
    }

    Slave createSlave(final TableName tableName, final DataStatistic statistic, final int batchSize) throws IOException {
      ProcessMode p = getProcessMode();
      RequestMode r = getRequestMode();
      switch (p) {
        case SYNC:
          switch (r) {
            case BATCH:
              return new BatchSlaveSync(conn.getTable(tableName), statistic, batchSize);
            case NORMAL:
              return new NormalSlaveSync(conn.getTable(tableName), statistic, batchSize);
          }
          break;
        case ASYNC:
          switch (r) {
            case BATCH:
              return new BatchSlaveAsync(asyncConn.getTable(tableName, ForkJoinPool.commonPool()), statistic, batchSize);
            case NORMAL:
              return new NormalSlaveAsync(asyncConn.getTable(tableName, ForkJoinPool.commonPool()), statistic, batchSize);
          }
          break;
      }
      throw new RuntimeException("Failed to find the suitable slave. ProcessMode:" + p + ", RequestMode:" + r);
    }

    @Override
    public void close() throws IOException {
      flush();
      safeClose(conn);
      safeClose(asyncConn);
    }

    private void flush() {
      if (nameToFlush == null) {
        return;
      }
      try (Admin admin = conn.getAdmin()) {
        admin.flush(nameToFlush);
      } catch (IOException ex) {
        LOG.error(ex);
      }
    }

    private static void safeClose(Closeable obj) {
      if (obj != null) {
        try {
          obj.close();
        } catch (IOException ex) {
          LOG.error(ex);
        }
      }
    }
  }

}
