
package net.chia7712.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;


public class TestOperation {
  public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
    if (args.length != 6) {
      System.out.println("[USAGE] <threads> <table> <cf> <rows> <op> <durability>");
      return;
    }
    final int threads = Integer.valueOf(args[0]);
    final TableName tb = TableName.valueOf(args[1]);
    final byte[] cf = Bytes.toBytes(args[2]);
    final int rows = Integer.valueOf(args[3]);
    final Operation op = Operation.valueOf(args[4]);
    final Durability durability = Durability.valueOf(args[5]);
    run(threads, tb, rows, cf, op, durability);
  }
  private static List<byte[]> split(int regions) {
    List<byte[]> result = new LinkedList<>();
    if (regions <= 1) {
      return result;
    }
    for (int start = 2; result.size() < (regions - 1)
        && start <=9; ++start) {
      System.out.println("add key:" + start);
      result.add(Bytes.toBytes(String.valueOf(start)));
    }
    return result;
  }
  private static void run(int threadCount, TableName tableName, int rowCount,
    byte[] cf, Operation op, final Durability durability) throws IOException, InterruptedException, ExecutionException {
    try (Connection con = ConnectionFactory.createConnection()) {
      ExecutorService service = Executors.newFixedThreadPool(threadCount,Threads.newDaemonThreadFactory("-" + op.name()));
      List<Worker> workers = new ArrayList<>(threadCount);
      for (int i = 0; i != threadCount; ++i) {
        workers.add(new Worker(i * rowCount,
          (i + 1) * rowCount, cf, durability, con.getTable(tableName), op.newSlave()));
      }
      final long startTime = System.currentTimeMillis();
      workers.forEach(service::execute);
      service.shutdown();
      service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      final long endTime = System.currentTimeMillis();
      final long totalRows = workers.stream().mapToLong(v -> v.getTotalRows()).sum();
      final long cellCount = workers.stream().mapToLong(v -> v.getCellCount()).sum();
      System.out.println("Total:" + totalRows
        + ", cell:" + cellCount
        + ", elapsed:" + (endTime - startTime));
    }
  }
}
