package codes.chia7712.contributor.operation;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;

@InterfaceAudience.Private
public class IndividualBytesFieldCell implements Cell {

  // The following fields are backed by individual byte arrays
  private byte[] row;
  private byte[] family;
  private byte[] qualifier;
  private byte[] value;
  private byte[] tags;  // A byte array, rather than an array of org.apache.hadoop.hbase.Tag

  // Other fields
  private long timestamp;
  private byte type;  // A byte, rather than org.apache.hadoop.hbase.KeyValue.Type
  private long seqId;

  public IndividualBytesFieldCell(byte[] row, byte[] family, byte[] qualifier,
          long timestamp, KeyValue.Type type, byte[] value) {
    this(row, family, qualifier, timestamp, type, 0L /* sequence id */, value, null /* tags */);
  }

  public IndividualBytesFieldCell(byte[] row, byte[] family, byte[] qualifier,
          long timestamp, KeyValue.Type type, long seqId, byte[] value, byte[] tags) {

    // Check timestamp
    if (timestamp < 0) {
      throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + timestamp);
    }

    // No local copy is made, but reference to the input directly
    this.row = row;
    this.family = family;
    this.qualifier = qualifier;
    this.value = value;
    this.tags = tags;

    // Set others
    this.timestamp = timestamp;
    this.type = type.getCode();
    this.seqId = seqId;
  }

  /**
   * Implement Cell interface
   */
  // 1) Row
  @Override
  public byte[] getRowArray() {
    // If row is null, the constructor will reject it, by {@link KeyValue#checkParameters()},
    // so it is safe to return row without checking.
    return row;
  }

  @Override
  public int getRowOffset() {
    return 0;
  }

  @Override
  public short getRowLength() {
    // If row is null or row.length is invalid, the constructor will reject it, by {@link KeyValue#checkParameters()},
    // so it is safe to call row.length and make the type conversion.
    return (short) (row.length);
  }

  // 2) Family
  @Override
  public byte[] getFamilyArray() {
    // Family could be null
    return (family == null) ? HConstants.EMPTY_BYTE_ARRAY : family;
  }

  @Override
  public int getFamilyOffset() {
    return 0;
  }

  @Override
  public byte getFamilyLength() {
    // If family.length is invalid, the constructor will reject it, by {@link KeyValue#checkParameters()},
    // so it is safe to make the type conversion.
    // But need to consider the condition when family is null.
    return (family == null) ? 0 : (byte) (family.length);
  }

  // 3) Qualifier
  @Override
  public byte[] getQualifierArray() {
    // Qualifier could be null
    return (qualifier == null) ? HConstants.EMPTY_BYTE_ARRAY : qualifier;
  }

  @Override
  public int getQualifierOffset() {
    return 0;
  }

  @Override
  public int getQualifierLength() {
    // Qualifier could be null
    return (qualifier == null) ? 0 : qualifier.length;
  }

  // 4) Timestamp
  @Override
  public long getTimestamp() {
    return timestamp;
  }

  //5) Type
  @Override
  public byte getTypeByte() {
    return type;
  }

  //6) Sequence id
  @Override
  public long getSequenceId() {
    return seqId;
  }

  //7) Value
  @Override
  public byte[] getValueArray() {
    // Value could be null
    return (value == null) ? HConstants.EMPTY_BYTE_ARRAY : value;
  }

  @Override
  public int getValueOffset() {
    return 0;
  }

  @Override
  public int getValueLength() {
    // Value could be null
    return (value == null) ? 0 : value.length;
  }

  // 8) Tags
  @Override
  public byte[] getTagsArray() {
    // Tags can could null
    return (tags == null) ? HConstants.EMPTY_BYTE_ARRAY : tags;
  }

  @Override
  public int getTagsOffset() {
    return 0;
  }

  @Override
  public int getTagsLength() {
    // Tags could be null
    return (tags == null) ? 0 : tags.length;
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
