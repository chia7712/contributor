
package codes.chia7712.contributor.jdbc;

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
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javafx.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Puts some data to specified database.
 */
public final class DataGenerator {

  /**
   * Log.
   */
  private static final Log LOG = LogFactory.getLog(DataGenerator.class);
  /**
   * The write buffer.
   */
  private static final int BUFFER_LIMIT = 30;

  /**
   * Write thread. End with commiting all rows.
   */
  public static final class WriteThread implements Runnable, Closeable {

    /**
     * Count the completed data.
     */
    private final AtomicLong progress;
    /**
     * Total cols.
     */
    private final long rowCount;
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
            final AtomicLong progress, final long rowCount, final List<DataWriter> writers) throws SQLException {
      this.progress = progress;
      this.conn = DriverManager.getConnection(url);
      this.conn.setAutoCommit(false);
      this.upsertSQL = upsertSQL;
      this.rowCount = rowCount;
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

    @Override
    public void run() {
      try (PreparedStatement stat = conn.prepareStatement(upsertSQL)) {
        long bufferCount = 0;
        for (int row = 0; row != rowCount; ++row) {
          int index = 1;
          for (DataWriter writer : writers) {
            writer.setData(stat, index);
            ++index;
          }
          stat.addBatch();
          ++bufferCount;
          if (bufferCount % BUFFER_LIMIT == 0) {
            stat.executeBatch();
            conn.commit();
            stat.clearBatch();
            progress.addAndGet(bufferCount);
            bufferCount = 0;
          }
        }
        if (bufferCount != 0) {
          stat.executeBatch();
          conn.commit();
          stat.clearBatch();
          progress.addAndGet(bufferCount);
        }
      } catch (SQLException ex) {
        LOG.error("Failed to manipulate database", ex);
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
    if (args.length != 3) {
      System.out.println("[Usage]: <jdbc url> <table name> <row count>");
      System.exit(0);
    }
    final String url = args[0];
    final TableName tableName = new TableName(args[1]);
    final DBType dbType = DBType.pickup(url).orElseThrow(() -> new IllegalArgumentException("No suitable db"));
    final int threadCount = 10;
    final long rowsEachThread = Long.valueOf(args[2]) / threadCount;
    final long rowCount = rowsEachThread * threadCount;
    final AtomicLong progress = new AtomicLong(0);
    final Pair<String, List<DataWriter>> queryAndWriters = createWriter(url, dbType, tableName);
    ExecutorService service = Executors.newFixedThreadPool(threadCount + 1);
    List<WriteThread> writeThreads = new LinkedList<>();
    service.execute(() -> {
      try {
        final long startTime = System.currentTimeMillis();
        while (progress.get() != rowCount) {
          TimeUnit.SECONDS.sleep(1);
          long elapsed = System.currentTimeMillis() - startTime;
          double average = (double) (progress.get() * 1000) / (double) elapsed;
          long remaining = (long) ((rowCount - progress.get()) / average);
          System.out.print("\r" + progress.get() + "/" + rowCount
                  + ", " + average + " rows/second"
                  + ", " + remaining + " seconds");
        }
      } catch (InterruptedException ex) {
        LOG.error("Breaking the sleep", ex);
      } finally {
        System.out.println("\r" + progress.get() + "/" + rowCount);
      }
    });
    final long startTime = System.currentTimeMillis();
    for (int i = 0; i < threadCount; ++i) {
      WriteThread writeThread = new WriteThread(url, queryAndWriters.getKey(), progress,
              rowsEachThread, queryAndWriters.getValue());
      writeThreads.add(writeThread);
      service.execute(writeThread);
    }
    service.shutdown();
    service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    writeThreads.forEach(t -> t.close());
    System.out.println("Total rows : " + rowCount);
    System.out.println("Elapsed : "
            + (System.currentTimeMillis() - startTime) + " milliseconds");
  }

  private static Pair<String, List<DataWriter>> createWriter(final String jdbcUrl, final DBType dbType, final TableName fullname) throws SQLException {
    try (Connection con = DriverManager.getConnection(jdbcUrl)) {
      return createWriter(con, dbType, fullname);
    }
  }

  private static Pair<String, List<DataWriter>> createWriter(final Connection con, final DBType dbType, final TableName fullname) throws SQLException {
    DatabaseMetaData meta = con.getMetaData();
    try (ResultSet rset = meta.getColumns(null, fullname.getSchema(null), fullname.getName(), null)) {
      List<DataWriter> writers = new LinkedList<>();
      Random rn = new Random();
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
            writers.add((stat, index) -> stat.setBinaryStream(index, new ByteArrayInputStream(String.valueOf(rn.nextLong()).getBytes())));
            break;
          case Types.BIGINT:
            writers.add((stat, index) -> stat.setLong(index, rn.nextLong()));
            break;
          case Types.BIT:
            writers.add((stat, index) -> stat.setBoolean(index, rn.nextBoolean()));
            break;
          case Types.BOOLEAN:
            writers.add((stat, index) -> stat.setBoolean(index, rn.nextBoolean()));
            break;
          case Types.DATE:
            writers.add((stat, index) -> stat.setDate(index, new Date(System.currentTimeMillis())));
            break;
          case Types.DECIMAL:
            writers.add((stat, index) -> stat.setBigDecimal(index, new BigDecimal(rn.nextLong())));
            break;
          case Types.DOUBLE:
            writers.add((stat, index) -> stat.setDouble(index, rn.nextDouble()));
            break;
          case Types.FLOAT:
            writers.add((stat, index) -> stat.setFloat(index, rn.nextFloat()));
            break;
          case Types.INTEGER:
            writers.add((stat, index) -> stat.setInt(index, rn.nextInt()));
            break;
          case Types.SMALLINT:
            writers.add((stat, index) -> stat.setShort(index, (short) rn.nextInt()));
            break;
          case Types.TIME:
            writers.add((stat, index) -> stat.setTime(index, new Time(System.currentTimeMillis())));
            break;
          case Types.TIMESTAMP:
            writers.add((stat, index) -> stat.setTimestamp(index, new Timestamp(System.currentTimeMillis())));
            break;
          case Types.TINYINT:
            writers.add((stat, index) -> stat.setByte(index, (byte) rn.nextInt()));
            break;
          case Types.VARBINARY:
            writers.add((stat, index) -> stat.setBytes(index, Bytes.toBytes(rn.nextLong())));
            break;
          case Types.VARCHAR:
            writers.add((stat, index) -> stat.setString(index, String.valueOf(rn.nextLong())));
            break;
          default:
            throw new RuntimeException(
                    "Unsupported type : " + type);
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
