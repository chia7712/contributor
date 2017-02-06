package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;

public class NormalSlaveAsync extends BatchSlave {

  private final List<Put> puts;
  private final List<Delete> deletes;
  private final List<Get> gets;
  private final List<Increment> incrs;
  private final AsyncTable table;

  public NormalSlaveAsync(final AsyncTable table, final DataStatistic statistic, final int batchSize) {
    super(statistic, batchSize);
    this.puts = new ArrayList<>(batchSize);
    this.deletes = new ArrayList<>(batchSize);
    this.gets = new ArrayList<>(batchSize);
    this.incrs = new ArrayList<>(batchSize);
    this.table = table;
  }

  @Override
  public void updateRow(RowWork work) throws IOException, InterruptedException {
    Row row = prepareRow(work);
    switch (work.getDataType()) {
      case GET:
        gets.add((Get) row);
        break;
      case PUT:
        puts.add((Put) row);
        break;
      case DELETE:
        deletes.add((Delete) row);
        break;
      case INCREMENT:
        incrs.add((Increment) row);
        break;
    }
    if (needFlush()) {
      flush();
    }
  }

  private void flush() {
    innerFlushMutation(DataType.PUT, table.put(puts), puts);
    innerFlushMutation(DataType.DELETE, table.delete(deletes), deletes);
    innerFlush(DataType.GET, table.get(gets), gets);
    innerFlush(DataType.INCREMENT, table.batch(incrs), incrs);
  }

  private void innerFlush(DataType type, List<CompletableFuture<Result>> results, List<? extends Row> rows) {
    try {
      results.forEach(CompletableFuture::join);
    } finally {
      finishRows(type, rows.size());
      rows.clear();
    }

  }

  private void innerFlushMutation(DataType type, List<CompletableFuture<Void>> results, List<? extends Row> rows) {
    try {
      results.forEach(CompletableFuture::join);
    } finally {
      finishRows(type, rows.size());
      rows.clear();
    }

  }

  @Override
  public ProcessMode getProcessMode() {
    return ProcessMode.ASYNC;
  }

  @Override
  public RequestMode getRequestMode() {
    return RequestMode.NORMAL;
  }

  @Override
  public void close() throws IOException {
    flush();
  }

}
