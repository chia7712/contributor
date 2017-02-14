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
    private int cellSize;
    private boolean largeQualifier;
    public Builder setLargeQualifier(boolean largeQualifier) {
      this.largeQualifier = largeQualifier;
      return this;
    }
    public Builder setCellSize(int cellSize) {
      this.cellSize = cellSize;
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
      return new RowWork(type, rowIndex, families, qualCount, durability, cellSize, largeQualifier);
    }
    private Builder(){}
  }
  private final DataType type;
  private final long rowIndex;
  private final Set<byte[]> families;
  private final int qualCount;
  private final Durability durability;
  private final int cellSize;
  private final boolean largeQualifier;
  private RowWork(final DataType type, final long rowIndex, final Set<byte[]> families,
    final int qualCount, final Durability durability, final int cellSize, final boolean largeQualifier) {
    this.type = type;
    this.rowIndex = rowIndex;
    this.families = families;
    this.qualCount = qualCount;
    this.durability = durability;
    this.cellSize = cellSize;
    this.largeQualifier = largeQualifier;
  }

  public boolean getLargeQualifier() {
    return largeQualifier;
  }

  public int getCellSize() {
    return cellSize;
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
