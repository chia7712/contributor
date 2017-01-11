package codes.chia7712.contributor.jdbc;

import codes.chia7712.contributor.schedule.DispatcherFactory;
import codes.chia7712.contributor.schedule.Dispatcher;
import codes.chia7712.contributor.view.Progress;
import codes.chia7712.contributor.data.RandomData;
import codes.chia7712.contributor.data.RandomDataFactory;
import codes.chia7712.contributor.schedule.Dispatcher.Packet;
import codes.chia7712.contributor.view.Arguments;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.sql.Date;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javafx.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

/**
 * Puts some data to specified database.
 */
public final class DataGenerator {

  /**
   * Log.
   */
  private static final Log LOG = LogFactory.getLog(DataGenerator.class);

  /**
   * Write thread. End with commiting all rows.
   */
  public static final class WriteThread implements Runnable, Closeable {
    private static final Log LOG = LogFactory.getLog(WriteThread.class);
    private static final AtomicLong IDS = new AtomicLong(0);
    private final long id = IDS.getAndIncrement();
    private final Dispatcher dispatcher;
    /**
     * Database connection.
     */
    private final Connection conn;
    /**
     * Upsert sql.
     */
    private final String upsertSQL;
    private final List<DataWriter> writers;

    /**
     * Constructs a write thread.
     *
     * @param url Targeted db url
     * @param upsertSQL Upsert sql
     * @param progress Count the committed data
     * @param rowCount
     * @throws SQLException If failed to establish db connection
     */
    private WriteThread(final String url, final String upsertSQL,
            final Dispatcher dispatcher, final List<DataWriter> writers) throws SQLException {
      this.dispatcher = dispatcher;
      this.conn = DriverManager.getConnection(url);
      this.conn.setAutoCommit(false);
      this.upsertSQL = upsertSQL;
      this.writers = writers;
    }

    @Override
    public void close() {
      try {
        conn.close();
      } catch (SQLException ex) {
        LOG.error("Failed to close db connection", ex);
      }
    }

    private void flush(PreparedStatement stat) throws SQLException {
      stat.executeBatch();
      conn.commit();
      stat.clearBatch();
    }

    @Override
    public void run() {
      LOG.info("Start #" + id);
      try (PreparedStatement stat = conn.prepareStatement(upsertSQL)) {
        Optional<Packet> packet;
        while ((packet = dispatcher.getPacket()).isPresent()) {
          while (packet.get().hasNext()) {
            packet.get().next();
            int index = 1;
            for (DataWriter writer : writers) {
              writer.setData(stat, index);
              ++index;
            }
            stat.addBatch();
          }
          flush(stat);
          packet.get().commit();
        }
      } catch (SQLException ex) {
        LOG.error("Failed to manipulate database", ex);
      } finally {
        LOG.info("Close #" + id);
      }
    }
  }

  /**
   * Runs the putter process. 1) create the targeted table 2) create INSERT
   * query and submit 3) create the loader config to run loader
   *
   * @param args db connection, table name, column and insert count
   * @throws Exception If any error
   */
  public static void main(final String[] args) throws Exception {
    Arguments arguments = new Arguments(
            Arrays.asList(
                    "url",
                    "table",
                    "rows",
                    "threads"),
            Arrays.asList("batchsize")
    );
    arguments.validate(args);
    final String url = arguments.get("url");
    final TableName tableName = new TableName(arguments.get("table"));
    final DBType dbType = DBType.pickup(url).orElseThrow(() -> new IllegalArgumentException("No suitable db"));
    final int threadCount = arguments.getInt("threads");
    final long totalRows = arguments.getInt("rows");
    final int batchSize = arguments.getInt("batchsize", 100);
    final Pair<String, List<DataWriter>> queryAndWriters = createWriter(url, dbType, tableName);
    ExecutorService service = Executors.newFixedThreadPool(threadCount, Threads.newDaemonThreadFactory("-" + WriteThread.class.getSimpleName()));
    List<WriteThread> writeThreads = new LinkedList<>();
    Dispatcher dispatcher = DispatcherFactory.get(totalRows, () -> batchSize);
    try (Progress progress = new Progress(dispatcher::getCommittedRows, totalRows)) {
      for (int i = 0; i < threadCount; ++i) {
        WriteThread writeThread = new WriteThread(url, queryAndWriters.getKey(),
                dispatcher, queryAndWriters.getValue());
        writeThreads.add(writeThread);
        service.execute(writeThread);
      }
      service.shutdown();
      service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      writeThreads.forEach(t -> t.close());
    }
  }

  private static Pair<String, List<DataWriter>> createWriter(final String jdbcUrl,
          final DBType dbType, final TableName fullname) throws SQLException {
    try (Connection con = DriverManager.getConnection(jdbcUrl)) {
      return createWriter(con, dbType, fullname);
    }
  }

  private static Pair<String, List<DataWriter>> createWriter(final Connection con,
          final DBType dbType, final TableName fullname) throws SQLException {
    DatabaseMetaData meta = con.getMetaData();
    try (ResultSet rset = meta.getColumns(null, fullname.getSchema(null), fullname.getName(), null)) {
      List<DataWriter> writers = new LinkedList<>();
      RandomData rn = RandomDataFactory.create();
      StringBuilder queryBuilder = new StringBuilder();
      switch (dbType) {
        case PHOENIX:
          queryBuilder.append("UPSERT INTO ");
          break;
        default:
          queryBuilder.append("INSERT INTO ");
          break;
      }
      queryBuilder.append(fullname)
              .append("(");
      int columnCount = 0;
      while (rset.next()) {
        String columnName = rset.getString(4);
        queryBuilder.append('\"')
                .append(columnName)
                .append('\"')
                .append(",");
        int type = rset.getInt(5);
        ++columnCount;
        switch (type) {
          case Types.BINARY:
            writers.add((stat, index) -> stat.setBinaryStream(index, new ByteArrayInputStream(String.valueOf(rn.getLong()).getBytes())));
            break;
          case Types.BIGINT:
            writers.add((stat, index) -> stat.setLong(index, rn.getLong()));
            break;
          case Types.BIT:
            writers.add((stat, index) -> stat.setBoolean(index, rn.getBoolean()));
            break;
          case Types.BOOLEAN:
            writers.add((stat, index) -> stat.setBoolean(index, rn.getBoolean()));
            break;
          case Types.DATE:
            writers.add((stat, index) -> stat.setDate(index, new Date(rn.getCurrentTimeMs())));
            break;
          case Types.DECIMAL:
            writers.add((stat, index) -> stat.setBigDecimal(index, new BigDecimal(rn.getLong())));
            break;
          case Types.DOUBLE:
            writers.add((stat, index) -> stat.setDouble(index, rn.getDouble()));
            break;
          case Types.FLOAT:
            writers.add((stat, index) -> stat.setFloat(index, rn.getFloat()));
            break;
          case Types.INTEGER:
            writers.add((stat, index) -> stat.setInt(index, rn.getInteger()));
            break;
          case Types.SMALLINT:
            writers.add((stat, index) -> stat.setShort(index, (short) rn.getInteger()));
            break;
          case Types.TIME:
            writers.add((stat, index) -> stat.setTime(index, new Time(rn.getCurrentTimeMs())));
            break;
          case Types.TIMESTAMP:
            writers.add((stat, index) -> stat.setTimestamp(index, new Timestamp(rn.getCurrentTimeMs())));
            break;
          case Types.TINYINT:
            writers.add((stat, index) -> stat.setByte(index, (byte) rn.getInteger()));
            break;
          case Types.VARBINARY:
            writers.add((stat, index) -> stat.setBytes(index, Bytes.toBytes(rn.getLong())));
            break;
          case Types.VARCHAR:
            writers.add((stat, index) -> stat.setString(index, rn.getStringWithRandomSize(15)));
            break;
          default:
            throw new RuntimeException("Unsupported type : " + type);
        }
      }
      if (columnCount == 0) {
        throw new RuntimeException("No found of any column for " + fullname.getFullName());
      }
      queryBuilder.deleteCharAt(queryBuilder.length() - 1)
              .append(") VALUES(");
      for (int i = 0; i != columnCount; ++i) {
        queryBuilder.append("?,");
      }
      queryBuilder.deleteCharAt(queryBuilder.length() - 1)
              .append(")");
      return new Pair<>(queryBuilder.toString(), writers);
    }
  }

  @FunctionalInterface
  interface DataWriter {

    void setData(final PreparedStatement stat, final int index) throws SQLException;
  }

  /**
   * Can't be instantiated with this ctor.
   */
  private DataGenerator() {
  }
}
