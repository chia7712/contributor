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

  private List<Put> puts;
  private List<Delete> deletes;
  private List<Get> gets;
  private List<Increment> incrs;
  private final Table table;
  private Object[] objs = null;
  private final int batchSize;

  public NormalSlaveSync(Table table, final DataStatistic statistic, final int batchSize) {
    super(statistic, batchSize);
    this.table = table;
    this.batchSize = batchSize;
  }

  private List<Put> getPutBuffer() {
    if (puts == null) {
      puts = new ArrayList<>(batchSize);
    }
    return puts;
  }

  private List<Get> getGetBuffer() {
    if (gets == null) {
      gets = new ArrayList<>(batchSize);
    }
    return gets;
  }

  private List<Delete> getDeleteBuffer() {
    if (deletes == null) {
      deletes = new ArrayList<>(batchSize);
    }
    return deletes;
  }

  private List<Increment> getIncrementBuffer() {
    if (incrs == null) {
      incrs = new ArrayList<>(batchSize);
    }
    return incrs;
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
        getGetBuffer().add((Get) row);
        break;
      case PUT:
        getPutBuffer().add((Put) row);
        break;
      case DELETE:
        getDeleteBuffer().add((Delete) row);
        break;
      case INCREMENT:
        getIncrementBuffer().add((Increment) row);
        break;
      default:
        throw new IllegalArgumentException("Unsupported type:" + work.getDataType());
    }
    if (needFlush()) {
      flush();
    }
  }

  private void innerFlush(List<?> data, TableAction f, DataType type) throws IOException, InterruptedException {
    if (data == null || data.isEmpty()) {
      return;
    }
    int size = data.size();
    try {
      f.run(table);
    } finally {
      finishRows(type, size);
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
