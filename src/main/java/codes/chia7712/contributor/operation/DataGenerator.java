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
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.BufferedMutator;
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
  private static final String LOG_INTERVAL = "logInterval";
  private static final String RANDOM_ROW = "randomRow";
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
                    LARGE_QUALIFIER,
                    LOG_INTERVAL,
                    RANDOM_ROW),
            Arrays.asList(
                    getDescription(ProcessMode.class.getSimpleName(), ProcessMode.values()),
                    getDescription(RequestMode.class.getSimpleName(), RequestMode.values()),
                    getDescription(DataType.class.getSimpleName(), DataType.values()),
                    getDescription(Durability.class.getSimpleName(), Durability.values()))
    );
    arguments.validate(args);
    final int threads = arguments.getInt(NUMBER_OF_THREAD);
    final TableName tableName = TableName.valueOf(arguments.get(TABLE_NAME));
    final long totalRows = arguments.getLong(NUMBER_OF_ROW);
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
    final int logInterval = arguments.getInt(LOG_INTERVAL, 5);
    final boolean randomRow = arguments.getBoolean(RANDOM_ROW, false);
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
                  .setLargeQualifier(largeQual)
                  .setRandomRow(randomRow);
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
        long maxThroughput = 0;
        try {
          while (!stop.get()) {
            maxThroughput = log(statistic, totalRows, startTime, maxThroughput);
            TimeUnit.SECONDS.sleep(logInterval);
          }
        } catch (InterruptedException ex) {
          LOG.error(ex);
        } finally {
          log(statistic, totalRows, startTime, maxThroughput);
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

  private static long log(DataStatistic statistic, long totalRows,
        long startTime, long maxThroughput) {
    long elapsed = (System.currentTimeMillis() - startTime) / 1000;
    long committedRows = statistic.getCommittedRows();
    if (elapsed <= 0 || committedRows <= 0) {
      return maxThroughput;
    }
    long throughput = committedRows / elapsed;
    if (throughput <= 0) {
      return maxThroughput;
    }
    maxThroughput = Math.max(maxThroughput, throughput);
    LOG.info("------------------------");
    LOG.info("total rows:" + totalRows);
    LOG.info("max throughput(rows/s):" + maxThroughput);
    LOG.info("throughput(rows/s):" + throughput);
    LOG.info("remaining(s):" + (totalRows - committedRows) / throughput);
    LOG.info("elapsed(s):" + elapsed);
    LOG.info("committed(rows):" + committedRows);
    LOG.info("processing(rows):" + statistic.getProcessingRows());
    statistic.consume((r, i) -> LOG.info(r.toString() + ":" + i));
    return maxThroughput;
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
    private AsyncTable asyncTable;

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
            asyncConn = ConnectionFactory.createAsyncConnection().join();
            break;
          case BUFFER:
            asyncConn = null;
            conn = ConnectionFactory.createConnection();
            break;
          default:
            throw new IllegalArgumentException("Unknown type:" + processMode.get());
        }
      } else {
        asyncConn = ConnectionFactory.createAsyncConnection().join();
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

    private AsyncTable getAsyncTable(TableName tableName) {
      if (asyncTable == null) {
        asyncTable = asyncConn.getTable(tableName, ForkJoinPool.commonPool());
      }
      return asyncTable;
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
              return new BatchSlaveAsync(getAsyncTable(tableName), statistic, batchSize);
            case NORMAL:
              return new NormalSlaveAsync(getAsyncTable(tableName), statistic, batchSize);
          }
          break;
        case BUFFER:
          return new BufferSlaveSync(conn.getBufferedMutator(tableName), statistic, batchSize);
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
