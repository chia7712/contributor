package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;

public class BatchSlaveSync extends BatchSlave {
  private final List<Row> rows;
  private final Table table;
  private Object[] objs = null;

  BatchSlaveSync(final Table table, final DataStatistic statistic, final int batchSize) {
    super(statistic, batchSize);
    this.rows = new ArrayList<>(batchSize);
    this.table = table;
  }

  private Object[] getObjects() {
    if (objs == null || objs.length != rows.size()) {
      objs = new Object[rows.size()];
    }
    return objs;
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
      table.batch(rows, getObjects());
    } finally {
      finishRows(rows);
      rows.clear();
      table.close();
    }
  }

  @Override
  public void close() throws IOException, InterruptedException {
    flush();
    table.close();
  }

  @Override
  public ProcessMode getProcessMode() {
    return ProcessMode.SYNC;
  }

  @Override
  public RequestMode getRequestMode() {
    return RequestMode.BATCH;
  }
}
