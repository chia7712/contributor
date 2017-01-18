package codes.chia7712.contributor.operation;

import codes.chia7712.contributor.schedule.Dispatcher;
import codes.chia7712.contributor.schedule.DispatcherFactory;
import codes.chia7712.contributor.view.Arguments;
import codes.chia7712.contributor.view.Progress;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

public class DataGenerator {
  private static final Log LOG = LogFactory.getLog(DataGenerator.class);
  public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
    Arguments arguments = new Arguments(
            Arrays.asList(
                    "threads",
                    "table",
                    "rows",
                    "op"),
            Arrays.asList("durability", "batch_size", "qualifier_number"),
            Arrays.asList(getDescription("op", Operation.values()),
            getDescription("durability", Durability.values()))
    );
    arguments.validate(args);
    final int threads = arguments.getInt("threads");
    final TableName tableName = TableName.valueOf(arguments.get("table"));
    final Set<byte[]> cfs = findColumn(tableName);
    final int totalRows = arguments.getInt("rows");
    final Operation op = Operation.valueOf(arguments.get("op").toUpperCase());
    final Durability durability = Durability.valueOf(arguments.get("durability", Durability.USE_DEFAULT.name()).toUpperCase());
    final int batchSize = arguments.getInt("batch_size", 100);
    final int qualifierNumber = arguments.getInt("qualifier_number", 1);
    try (Connection con = ConnectionFactory.createConnection()) {
      ExecutorService service = Executors.newFixedThreadPool(threads, Threads.newDaemonThreadFactory("-" + op.name()));
      List<Worker> workers = new ArrayList<>(threads);
      Dispatcher dispatcher = DispatcherFactory.get(totalRows, batchSize);
      LOG.info("Generator " + threads + " threads");
      for (int i = 0; i != threads; ++i) {
        workers.add(new Worker(con.getTable(tableName), cfs, durability, dispatcher, op.newSlave(qualifierNumber)));
      }
      try (Progress progress = new Progress(dispatcher::getCommittedRows, totalRows)) {
        LOG.info("submit " + threads + " threads");
        workers.forEach(service::execute);
        service.shutdown();
        service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      }
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

}
