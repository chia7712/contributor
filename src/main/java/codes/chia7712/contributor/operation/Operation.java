package codes.chia7712.contributor.operation;

import java.util.function.Supplier;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Table;

public enum Operation {
  NORMAL_PUT, BATCH_PUT, BATCH_DELETE, BATCH_INCREMENT, BATCH_GET, BATCH_RANDOM, SCAN;

  public static Slave newSlaveSync(Operation op, Table table, int columnNumber) {
    switch (op) {
      case SCAN:
        return new ScanSlaveSync(table);
      case NORMAL_PUT:
        return new PutSlaveSync(table, columnNumber);
      case BATCH_PUT:
        return new BatchSlaveSync(table, () -> BatchType.PUT, columnNumber);
      case BATCH_DELETE:
        return new BatchSlaveSync(table, () -> BatchType.DELETE, columnNumber);
      case BATCH_INCREMENT:
        return new BatchSlaveSync(table, () -> BatchType.INCREMENT, columnNumber);
      case BATCH_GET:
        return new BatchSlaveSync(table, () -> BatchType.GET, columnNumber);
      case BATCH_RANDOM:
        Supplier<BatchType> randomType = () -> {
          return BatchType.values()[(int) (Math.random() * BatchType.values().length)];
        };
        return new BatchSlaveSync(table, randomType, columnNumber);
      default:
    }
    throw new RuntimeException("Unsupported operation:" + op);
  }

  public static Slave newSlaveAsync(Operation op, AsyncTable table, int columnNumber) {
    switch (op) {
      case SCAN:
        break;
      case NORMAL_PUT:
        return new PutSlaveAsync(table, columnNumber);
      case BATCH_PUT:
        return new BatchSlaveAsync(table, () -> BatchType.PUT, columnNumber);
      case BATCH_DELETE:
        return new BatchSlaveAsync(table, () -> BatchType.DELETE, columnNumber);
      case BATCH_INCREMENT:
        return new BatchSlaveAsync(table, () -> BatchType.INCREMENT, columnNumber);
      case BATCH_GET:
        return new BatchSlaveAsync(table, () -> BatchType.GET, columnNumber);
      case BATCH_RANDOM:
        Supplier<BatchType> randomType = () -> {
          return BatchType.values()[(int) (Math.random() * BatchType.values().length)];
        };
        return new BatchSlaveAsync(table, randomType, columnNumber);
      default:
    }
    throw new RuntimeException("Unsupported operation:" + op);
  }
}
