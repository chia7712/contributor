package codes.chia7712.contributor.operation;

import codes.chia7712.contributor.data.RandomData;
import codes.chia7712.contributor.data.RandomDataFactory;
import codes.chia7712.contributor.operation.DataStatistic.Record;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.IndividualBytesFieldCell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class BatchSlave implements Slave {

//  private static final int MAX_VALUE_SIZE = 1024;
//  private static final int MIN_VALUE_SIZE = 100;
//  private static final byte[] RANDOM_STRING_BYTES = Bytes.toBytes(
//          RandomDataFactory.create().getString(
//          Math.max((int) (Math.random() * MAX_VALUE_SIZE), MIN_VALUE_SIZE)));
  private static final RandomData RANDOM = RandomDataFactory.create();
  private static final List<String> KEYS = Arrays.asList(
          "0-",
          "1-",
          "2-",
          "3-",
          "4-",
          "5-",
          "6-",
          "7-",
          "8-",
          "9-");
  private static final List<byte[]> KEYS_BYTES = KEYS.stream().map(Bytes::toBytes).collect(Collectors.toList());
  private static final byte[] DELIMITER = Bytes.toBytes("-");
  private final LongAdder processingRows = new LongAdder();
  private final DataStatistic statistic;
  private final int batchRows;
  private final ConcurrentMap<DataType, Record> recordCache = new ConcurrentHashMap<>();
  public BatchSlave(final DataStatistic statistic, final int batchRows) {
    this.statistic = statistic;
    this.batchRows = batchRows;
  }

  private static boolean isNormalCell(RowWork work) {
    return work.getCellSize()<= 0;
  }
  protected boolean needFlush() {
    return getProcessingRows() >= batchRows;
  }

  private void addNewRows(Record record, int delta) {
    assert delta >= 0;
    statistic.addNewRows(record, delta);
    processingRows.add(delta);
  }

  private Map<DataType, Record> getCache() {
    if (recordCache.size() == DataType.values().length) {
      return recordCache;
    }
    for (DataType type : DataType.values()) {
      recordCache.computeIfAbsent(type, k -> new Record(getProcessMode(), getRequestMode(), k));
    }
    return recordCache;
  }

  protected void finishRows(List<? extends Row> rows) {
    Map<DataType, Record> cache = getCache();
    processingRows.add(-rows.size());
    for (Row row : rows) {
      for (DataType type : DataType.values()) {
        if (type.isInstance(row)) {
          statistic.finishRows(cache.get(type), 1);
          break;
        }
      }
    }
  }

  protected void finishRows(DataType expectedType, int delta) {
    if (delta <= 0) {
      return;
    }
    Map<DataType, Record> cache = getCache();
    processingRows.add(-delta);
    statistic.finishRows(cache.get(expectedType), delta);
  }

  private long getProcessingRows() {
    return processingRows.longValue();
  }

  protected Row prepareRow(RowWork work) {
    Row row;
    switch (work.getDataType()) {
      case PUT:
        row = createRandomPut(work);
        break;
      case DELETE:
        row = createRandomDelete(work);
        break;
      case GET:
        row = createRandomGet(work);
        break;
      case INCREMENT:
        row = createRandomIncrement(work);
        break;
      default:
        throw new RuntimeException("Unknown type:" + work.getDataType());
    }
    addNewRows(new Record(getProcessMode(), getRequestMode(), work.getDataType()), 1);
    return row;
  }

  @Override
  public String toString() {
    return this.getProcessMode() + "/" + this.getRequestMode();
  }

  @VisibleForTesting
  static byte[] createRow(long rowIndex) {
    byte[] key = KEYS_BYTES.get((int) (Math.random() * KEYS_BYTES.size()));
    byte[] rowIndexBytes = Bytes.toBytes(String.valueOf(Math.abs(rowIndex)));
    byte[] timeBytes = Bytes.toBytes(String.valueOf(System.currentTimeMillis()));
    byte[] randomIndex = Bytes.toBytes(String.valueOf(Math.abs(RANDOM.getLong())));
    byte[] buf = new byte[key.length + timeBytes.length + DELIMITER.length
        + randomIndex.length + DELIMITER.length + rowIndexBytes.length];
    int offset = 0;
    offset = Bytes.putBytes(buf, offset, key, 0, key.length);
    offset = Bytes.putBytes(buf, offset, timeBytes, 0, timeBytes.length);
    offset = Bytes.putBytes(buf, offset, DELIMITER, 0, DELIMITER.length);
    offset = Bytes.putBytes(buf, offset, randomIndex, 0, randomIndex.length);
    offset = Bytes.putBytes(buf, offset, DELIMITER, 0, DELIMITER.length);
    offset = Bytes.putBytes(buf, offset, rowIndexBytes, 0, rowIndexBytes.length);
    return buf;
  }

  private static Put createRandomPut(RowWork work) {
    byte[] row = createRow(work.getRowIndex());
    SimplePut put = new SimplePut(row);
    put.setDurability(work.getDurability());
    CellRewriter rewriter = null;
    byte[] largeData = isNormalCell(work) ? null : RANDOM.getBytes(work.getCellSize());
    for (byte[] family : work.getFamilies()) {
      for (int i = 0; i != work.getQualifierCount(); ++i) {
        Cell cell;
        byte[] normalData = Bytes.toBytes(work.getRowIndex() + i);
        if (rewriter == null) {
          cell = new IndividualBytesFieldCell(row, family,
                  normalData, HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put, normalData);
          rewriter = CellRewriter.newCellRewriter(cell);
        } else {
          byte[] value = largeData == null ? normalData : largeData;
          if (work.getLargeQualifier()) {
            cell = rewriter.rewrite(CellRewriter.Field.QUALIFIER, value)
                           .rewrite(CellRewriter.Field.VALUE, normalData)
                           .getAndReset();
          } else {
            cell = rewriter.rewrite(CellRewriter.Field.QUALIFIER, normalData)
                           .rewrite(CellRewriter.Field.VALUE, value)
                           .getAndReset();
          }

        }
        put.add(family, cell, work.getQualifierCount());
      }
    }
    return put;
  }

  private static Get createRandomGet(RowWork work) {
    byte[] row = createRow(work.getRowIndex());
    Get get = new Get(row);
    switch (RANDOM.getInteger(1)) {
      case 0:
        for (byte[] family : work.getFamilies()) {
          for (int i = 0; i != work.getQualifierCount(); ++i) {
            get.addColumn(family, Bytes.toBytes(RANDOM.getLong()));
          }
        }
        break;
      default:
        for (byte[] family : work.getFamilies()) {
          get.addFamily(family);
        }
        break;
    }
    return get;
  }

  private static Delete createRandomDelete(RowWork work) {
    byte[] row = createRow(work.getRowIndex());
    SimpleDelete delete = new SimpleDelete(row);
    delete.setDurability(work.getDurability());
    CellRewriter rewriter = null;
    for (byte[] family : work.getFamilies()) {
      for (int i = 0; i != work.getQualifierCount(); ++i) {
        Cell cell;
        byte[] normalData = Bytes.toBytes(work.getRowIndex() + i);
        if (rewriter == null) {
          cell = new IndividualBytesFieldCell(row, family,
                normalData, HConstants.LATEST_TIMESTAMP, KeyValue.Type.Delete, null);
          rewriter = CellRewriter.newCellRewriter(cell);
        } else {
          byte[] largeData = isNormalCell(work) ? normalData : RANDOM.getBytes(work.getCellSize());
          cell = rewriter.rewrite(CellRewriter.Field.QUALIFIER, largeData)
                         .getAndReset();
        }
        delete.add(family, cell, work.getQualifierCount());
      }
    }
    return delete;
  }

  private static Increment createRandomIncrement(RowWork work) {
    long randomIndex = RANDOM.getLong();
    byte[] row = createRow(randomIndex);
    SimpleIncrement inc = new SimpleIncrement(row);
    inc.setDurability(work.getDurability());
    CellRewriter rewriter = null;
    for (byte[] family : work.getFamilies()) {
      for (int i = 0; i != work.getQualifierCount(); ++i) {
        Cell cell;
        byte[] normalData = Bytes.toBytes(randomIndex + i);
        if (rewriter == null) {
          cell = new IndividualBytesFieldCell(row, family,
                normalData, HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put, normalData);
          rewriter = CellRewriter.newCellRewriter(cell);
        } else {
          byte[] largeData = isNormalCell(work) ? normalData : RANDOM.getBytes(work.getCellSize());
          cell = rewriter.rewrite(CellRewriter.Field.QUALIFIER, largeData)
                       .getAndReset();
        }
        inc.add(family, cell, work.getQualifierCount());
      }
    }
    return inc;
  }

  private static class SimplePut extends Put {
    private Long heapSize;
    private SimplePut(byte[] row) {
      super(row);
    }

    @Override
    public long heapSize() {
      if (heapSize == null) {
        heapSize = super.heapSize();
      }
      return heapSize;
    }

    private SimplePut add(byte[] family, Cell cell, int expectedSize) {
      List<Cell> cells = familyMap.get(family);
      if (cells == null) {
        cells = new ArrayList<>(expectedSize);
        familyMap.put(family, cells);
      }
      cells.add(cell);
      return this;
    }
  }

  private static class SimpleDelete extends Delete {
    
    private SimpleDelete(byte[] row) {
      super(row);
    }

    private SimpleDelete add(byte[] family, Cell cell, int expectedSize) {
      List<Cell> cells = familyMap.get(family);
      if (cells == null) {
        cells = new ArrayList<>(expectedSize);
        familyMap.put(family, cells);
      }
      cells.add(cell);
      return this;
    }
  }

  private static class SimpleIncrement extends Increment {

    private SimpleIncrement(byte[] row) {
      super(row);
    }

    private SimpleIncrement add(byte[] family, Cell cell, int expectedSize) {
      List<Cell> cells = familyMap.get(family);
      if (cells == null) {
        cells = new ArrayList<>(expectedSize);
        familyMap.put(family, cells);
      }
      cells.add(cell);
      return this;
    }
  }
}
