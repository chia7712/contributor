package codes.chia7712.contributor.operation;

import org.apache.hadoop.hbase.Cell;

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
    if (current == null) {
      current = new RewriteCell(baseCell, rewirtedField, rewritedArray);
    } else {
      current = new RewriteCell(current, rewirtedField, rewritedArray);
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

    public RewriteCell(Cell cell, final Field rewirtedField, byte[] rewritedArray) {
      assert rewritedArray != null;
      this.cell = cell;
      this.rewritedArray = rewritedArray;
      this.rewirtedField = rewirtedField;
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
        return 0;
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
        return rewritedArray.length;
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
  }
}
