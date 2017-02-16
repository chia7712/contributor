package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;

public class NormalSlaveSync extends BatchSlave {

  private final List<Put> puts;
  private final List<Delete> deletes;
  private final List<Get> gets;
  private final List<Increment> incrs;
  private final Table table;
  private Object[] objs = null;

  public NormalSlaveSync(Table table, final DataStatistic statistic, final int batchSize) {
    super(statistic, batchSize);
    this.puts = new ArrayList<>(batchSize);
    this.deletes = new ArrayList<>(batchSize);
    this.gets = new ArrayList<>(batchSize);
    this.incrs = new ArrayList<>(batchSize);
    this.table = table;
  }

  private Object[] getObjects() {
    if (objs == null || objs.length != incrs.size()) {
      objs = new Object[incrs.size()];
    }
    return objs;
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

  private void innerFlush(List<?> data, TableAction f, DataType type) throws IOException, InterruptedException {
    if (data.isEmpty()) {
      return;
    }
    try {
      int size = data.size();
      f.run(table);
      finishRows(type, size);
    } finally {
      data.clear();
    }
  }
  private void flush() throws IOException, InterruptedException {
    innerFlush(puts, t -> t.put(puts), DataType.PUT);
    innerFlush(deletes, t -> t.delete(deletes), DataType.DELETE);
    innerFlush(gets, t -> t.get(gets), DataType.GET);
    innerFlush(incrs, t -> t.batch(incrs, getObjects()), DataType.INCREMENT);
  }

  @Override
  public ProcessMode getProcessMode() {
    return ProcessMode.SYNC;
  }

  @Override
  public RequestMode getRequestMode() {
    return RequestMode.NORMAL;
  }

  @Override
  public void close() throws IOException, InterruptedException {
    flush();
    table.close();
  }

  @FunctionalInterface
  interface TableAction {
    void run(Table table) throws IOException, InterruptedException;
  }
}
