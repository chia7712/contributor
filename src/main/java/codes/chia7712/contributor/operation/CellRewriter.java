package codes.chia7712.contributor.operation;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

public class CellRewriter {

  public static CellRewriter newCellRewriter(final Cell baseCell) {
    return new CellRewriter(baseCell);
  }
  private final Cell baseCell;
  private Cell current;

  private CellRewriter(final Cell baseCell) {
    this.baseCell = baseCell;
  }

  public CellRewriter rewrite(Field rewirtedField, byte[] rewritedArray) {
    return rewrite(rewirtedField, rewritedArray, 0, rewritedArray.length);
  }

  public CellRewriter rewrite(Field rewirtedField, byte[] rewritedArray,
          final int rewritedOffset, final int rewritedLength) {
    if (current == null) {
      current = new RewriteCell(baseCell, rewirtedField, rewritedArray,
              rewritedOffset, rewritedLength);
    } else {
      current = new RewriteCell(current, rewirtedField, rewritedArray,
              rewritedOffset, rewritedLength);
    }
    return this;
  }

  public Cell getAndReset() {
    Cell rval = current;
    current = null;
    return rval;
  }

  public enum Field {
    ROW, FAMILY, QUALIFIER, VALUE, TAG;
  }

  private static class RewriteCell implements Cell {

    private final Field rewirtedField;
    private final Cell cell;
    private final byte[] rewritedArray;
    private final int rewritedOffset;
    private final int rewritedLength;

    public RewriteCell(Cell cell, final Field rewirtedField, byte[] rewritedArray) {
      this(cell, rewirtedField, rewritedArray, 0, rewritedArray.length);
    }

    public RewriteCell(Cell cell, final Field rewirtedField, byte[] rewritedArray,
            final int rewritedOffset, final int rewritedLength) {
      assert rewritedArray != null;
      this.cell = cell;
      this.rewritedArray = rewritedArray;
      this.rewirtedField = rewirtedField;
      this.rewritedOffset = rewritedOffset;
      this.rewritedLength = rewritedLength;
    }

    private byte[] getFieldArray(Field field) {
      if (field == rewirtedField) {
        return rewritedArray;
      }
      switch (field) {
        case ROW:
          return cell.getRowArray();
        case FAMILY:
          return cell.getFamilyArray();
        case QUALIFIER:
          return cell.getQualifierArray();
        case VALUE:
          return cell.getValueArray();
        default:
          return cell.getTagsArray();
      }
    }

    private int getFieldOffset(Field field) {
      if (field == rewirtedField) {
        return rewritedOffset;
      }
      switch (field) {
        case ROW:
          return cell.getRowOffset();
        case FAMILY:
          return cell.getFamilyOffset();
        case QUALIFIER:
          return cell.getQualifierOffset();
        case VALUE:
          return cell.getValueOffset();
        default:
          return cell.getTagsOffset();
      }
    }

    private int getFieldLength(Field field) {
      if (field == rewirtedField) {
        return rewritedLength;
      }
      switch (field) {
        case ROW:
          return cell.getRowLength();
        case FAMILY:
          return cell.getFamilyLength();
        case QUALIFIER:
          return cell.getQualifierLength();
        case VALUE:
          return cell.getValueLength();
        default:
          return cell.getTagsLength();
      }
    }

    @Override
    public byte[] getRowArray() {
      return getFieldArray(Field.ROW);
    }

    @Override
    public int getRowOffset() {
      return getFieldOffset(Field.ROW);
    }

    @Override
    public short getRowLength() {
      return (short) getFieldLength(Field.ROW);
    }

    @Override
    public byte[] getFamilyArray() {
      return getFieldArray(Field.FAMILY);
    }

    @Override
    public int getFamilyOffset() {
      return getFieldOffset(Field.FAMILY);
    }

    @Override
    public byte getFamilyLength() {
      return (byte) getFieldLength(Field.FAMILY);
    }

    @Override
    public byte[] getQualifierArray() {
      return getFieldArray(Field.QUALIFIER);
    }

    @Override
    public int getQualifierOffset() {
      return getFieldOffset(Field.QUALIFIER);
    }

    @Override
    public int getQualifierLength() {
      return getFieldLength(Field.QUALIFIER);
    }

    @Override
    public long getTimestamp() {
      return cell.getTimestamp();
    }

    @Override
    public byte getTypeByte() {
      return cell.getTypeByte();
    }

    @Override
    public long getSequenceId() {
      return cell.getSequenceId();
    }

    @Override
    public byte[] getValueArray() {
      return getFieldArray(Field.VALUE);
    }

    @Override
    public int getValueOffset() {
      return getFieldOffset(Field.VALUE);
    }

    @Override
    public int getValueLength() {
      return getFieldLength(Field.VALUE);
    }

    @Override
    public byte[] getTagsArray() {
      return getFieldArray(Field.TAG);
    }

    @Override
    public int getTagsOffset() {
      return getFieldOffset(Field.TAG);
    }

    @Override
    public int getTagsLength() {
      return (int) getFieldLength(Field.TAG);
    }

    @Override
    public long getMvccVersion() {
      return this.getSequenceId();
    }

    @Override
    public byte[] getValue() {
      return CellUtil.cloneValue(this);
    }

    @Override
    public byte[] getFamily() {
      return CellUtil.cloneFamily(this);
    }

    @Override
    public byte[] getQualifier() {
      return CellUtil.cloneQualifier(this);
    }

    @Override
    public byte[] getRow() {
      return CellUtil.cloneRow(this);
    }
  }
}
