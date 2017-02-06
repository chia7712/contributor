package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Row;

public class BatchSlaveAsync extends BatchSlave {

  private final AsyncTable table;
  private final List<Row> rows;

  BatchSlaveAsync(final AsyncTable table, final DataStatistic statistic, final int batchSize) {
    super(statistic, batchSize);
    this.rows = new ArrayList<>(batchSize);
    this.table = table;
  }

  @Override
  public void updateRow(RowWork work) throws IOException, InterruptedException {
    rows.add(prepareRow(work));
    if (needFlush()) {
      flush();
    }
  }

  private void flush() throws IOException, InterruptedException {
    try {
      List<CompletableFuture<Row>> futs = table.batch(rows);
      futs.forEach(CompletableFuture::join);
    } finally {
      finishRows(rows);
      rows.clear();
    }
  }


  @Override
  public void close() throws IOException, InterruptedException {
    flush();
  }

  @Override
  public ProcessMode getProcessMode() {
    return ProcessMode.ASYNC;
  }

  @Override
  public RequestMode getRequestMode() {
    return RequestMode.BATCH;
  }
}
