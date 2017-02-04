package codes.chia7712.contributor.operation;

import codes.chia7712.contributor.data.RandomData;
import codes.chia7712.contributor.data.RandomDataFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.IndividualBytesFieldCell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class RowIndexer {
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
  public static RandomData getRandomData() {
    return RANDOM;
  }
  public static byte[] createRow(long currentIndex) {
    int index = RANDOM.getInteger(KEYS.size());
    return Bytes.toBytes(KEYS.get(index) + currentIndex + "-" + RANDOM.getLong());
  }
  public static Put createRandomPut(long rowIndex, Durability durability,
      Set<byte[]> families, int qualifierNumber) {
    long randomIndex = getRandomData().getLong();
    byte[] row = Bytes.toBytes(randomIndex);
    SimplePut put = new SimplePut(row);
    put.setDurability(durability);
    for (byte[] family : families) {
      for (int i = 0; i != qualifierNumber; ++i) {
        Cell cell = new IndividualBytesFieldCell(row, family,
          Bytes.toBytes(randomIndex + i), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put, row);
        put.add(family, cell, qualifierNumber);
      }
    }
    return put;
  }
  public static Delete createRandomDelete(long rowIndex, Durability durability,
      Set<byte[]> families, int qualifierNumber) {
    long randomIndex = getRandomData().getLong();
    byte[] row = Bytes.toBytes(randomIndex);
    SimpleDelete delete = new SimpleDelete(row);
    delete.setDurability(durability);
    for (byte[] family : families) {
      for (int i = 0; i != qualifierNumber; ++i) {
        Cell cell = new IndividualBytesFieldCell(row, family,
          Bytes.toBytes(randomIndex + i), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Delete, null);
        delete.add(family, cell, qualifierNumber);
      }
    }
    return delete;
  }
  public static Increment createRandomIncrement(long rowIndex, Durability durability,
      Set<byte[]> families, int qualifierNumber) {
    long randomIndex = getRandomData().getLong();
    byte[] row = Bytes.toBytes(randomIndex);
    SimpleIncrement inc = new SimpleIncrement(row);
    inc.setDurability(durability);
    for (byte[] family : families) {
      for (int i = 0; i != qualifierNumber; ++i) {
        Cell cell = new IndividualBytesFieldCell(row, family,
          Bytes.toBytes(randomIndex + i), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put, row);
        inc.add(family, cell, qualifierNumber);
      }
    }
    return inc;
  }

  private static class SimplePut extends Put {
    
    private SimplePut(byte[] row) {
      super(row);
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
