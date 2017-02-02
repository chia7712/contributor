package codes.chia7712.contributor.operation;

import codes.chia7712.contributor.data.RandomData;
import codes.chia7712.contributor.data.RandomDataFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
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
    Put put = new Put(row);
    put.setDurability(durability);
    for (byte[] family : families) {
      for (int i = 0; i != qualifierNumber; ++i) {
        Cell cell = new SimpleCell(row, family, Bytes.toBytes(randomIndex + i), KeyValue.Type.Put, row);
        try {
          put.add(cell);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
    return put;
  }
  public static Delete createRandomDelete(long rowIndex, Durability durability,
      Set<byte[]> families, int qualifierNumber) {
    long randomIndex = getRandomData().getLong();
    byte[] row = Bytes.toBytes(randomIndex);
    Delete delete = new Delete(row);
    delete.setDurability(durability);
    for (byte[] family : families) {
      for (int i = 0; i != qualifierNumber; ++i) {
        Cell cell = new SimpleCell(row, family, Bytes.toBytes(randomIndex + i), KeyValue.Type.Delete, null);
        try {
          delete.addDeleteMarker(cell);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
    return delete;
  }
  public static Increment createRandomIncrement(long rowIndex, Durability durability,
      Set<byte[]> families, int qualifierNumber) {
    long randomIndex = getRandomData().getLong();
    byte[] row = Bytes.toBytes(randomIndex);
    Increment inc = new Increment(row);
    inc.setDurability(durability);
    for (byte[] family : families) {
      for (int i = 0; i != qualifierNumber; ++i) {
        Cell cell = new SimpleCell(row, family, Bytes.toBytes(randomIndex + i), KeyValue.Type.Put, row);
        try {
          inc.add(cell);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
    return inc;
  }
  private static class SimpleCell implements Cell {
    private final byte[] row;
    private final byte[] family;
    private final byte[] qual;
    private final KeyValue.Type type;
    private final long ts = HConstants.LATEST_TIMESTAMP;
    private final byte[] value;
    private final byte[] tag = new byte[0];
    private final long seqId = 0;
    SimpleCell(final byte[] row, final byte[] family, final byte[] qual,
      final KeyValue.Type type, final byte[] value) {
      this.row = getOrEmpty(row);
      this.family = getOrEmpty(family);
      this.qual = getOrEmpty(qual);
      this.type = type;
      this.value = getOrEmpty(value);
    }
    private static byte[] getOrEmpty(byte[] ori) {
      return ori == null ? new byte[0] : ori;
    }
    @Override
    public byte[] getRowArray() {
      return row;
    }

    @Override
    public int getRowOffset() {
      return 0;
    }

    @Override
    public short getRowLength() {
      return (short) row.length;
    }

    @Override
    public byte[] getFamilyArray() {
      return family;
    }

    @Override
    public int getFamilyOffset() {
      return 0;
    }

    @Override
    public byte getFamilyLength() {
      return (byte) family.length;
    }

    @Override
    public byte[] getQualifierArray() {
      return qual;
    }

    @Override
    public int getQualifierOffset() {
      return 0;
    }

    @Override
    public int getQualifierLength() {
      return qual.length;
    }

    @Override
    public long getTimestamp() {
      return ts;
    }

    @Override
    public byte getTypeByte() {
      return type.getCode();
    }

    @Override
    public long getSequenceId() {
      return seqId;
    }

    @Override
    public byte[] getValueArray() {
      return value;
    }

    @Override
    public int getValueOffset() {
      return 0;
    }

    @Override
    public int getValueLength() {
      return value.length;
    }

    @Override
    public byte[] getTagsArray() {
      return tag;
    }

    @Override
    public int getTagsOffset() {
      return 0;
    }

    @Override
    public int getTagsLength() {
      return tag.length;
    }
  }
}
