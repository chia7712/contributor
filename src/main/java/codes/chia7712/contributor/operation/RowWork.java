package codes.chia7712.contributor.operation;

import java.util.Set;
import org.apache.hadoop.hbase.client.Durability;

public class RowWork {
  public static Builder newBuilder() {
    return new Builder();
  }
  public static class Builder {
    private DataType type;
    private long rowIndex;
    private Set<byte[]> families;
    private int qualCount;
    private Durability durability;
    private boolean largeCell;
    public Builder setLargeCell(boolean largeCell) {
      this.largeCell = largeCell;
      return this;
    }
    public Builder setBatchType(DataType type) {
      this.type = type;
      return this;
    }

    public Builder setRowIndex(long rowIndex) {
      this.rowIndex = rowIndex;
      return this;
    }

    public Builder setFamilies(Set<byte[]> families) {
      this.families = families;
      return this;
    }

    public Builder setQualifierCount(int qualCount) {
      this.qualCount = qualCount;
      return this;
    }

    public Builder setDurability(Durability durability) {
      this.durability = durability;
      return this;
    }
    public RowWork build() {
      return new RowWork(type, rowIndex, families, qualCount, durability, largeCell);
    }
    private Builder(){}
  }
  private final DataType type;
  private final long rowIndex;
  private final Set<byte[]> families;
  private final int qualCount;
  private final Durability durability;
  private final boolean largeCell;
  private RowWork(DataType type, long rowIndex, Set<byte[]> families, int qualCount,
        Durability durability, boolean largeCell) {
    this.type = type;
    this.rowIndex = rowIndex;
    this.families = families;
    this.qualCount = qualCount;
    this.durability = durability;
    this.largeCell = largeCell;
  }

  public boolean getLargeCell() {
    return largeCell;
  }
  public DataType getDataType() {
    return type;
  }

  public long getRowIndex() {
    return rowIndex;
  }

  public Set<byte[]> getFamilies() {
    return families;
  }

  public int getQualifierCount() {
    return qualCount;
  }

  public Durability getDurability() {
    return durability;
  }
}
