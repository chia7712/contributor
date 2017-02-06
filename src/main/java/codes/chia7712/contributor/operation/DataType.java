
package codes.chia7712.contributor.operation;

import codes.chia7712.contributor.util.EnumUtil;
import java.util.Optional;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

public enum DataType {
  PUT(Put.class),
  DELETE(Delete.class),
  GET(Get.class),
  INCREMENT(Increment.class);
  DataType(Class<? extends Row> clz) {
    this.clz = clz;
  }
  private final Class<? extends Row> clz;
  public boolean isInstance(Row row) {
    return clz.isInstance(row);
  }
  public static Optional<DataType> find(String value) {
    return EnumUtil.find(value, DataType.class);
  }
}
