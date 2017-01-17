package codes.chia7712.contributor.operation;

public enum Operation {
  NORMAL_PUT, BATCH_PUT, BATCH_DELETE, BATCH_INCREMENT, BATCH_GET, BATCH_RANDOM, SCAN;

  public static String getDescription() {
    StringBuilder builder = new StringBuilder("op:");
    for (Operation op : Operation.values()) {
      builder.append(op.name())
              .append(",");
    }
    builder.deleteCharAt(builder.length() - 1);
    return builder.toString();
  }

  public Slave newSlave(int columnNumber) {
    switch (this) {
      case SCAN:
        return new ScanSlave();
      case NORMAL_PUT:
        return new PutSlave(columnNumber);
      case BATCH_PUT:
        return new BatchSlave(() -> BatchSlave.Type.PUT, columnNumber);
      case BATCH_DELETE:
        return new BatchSlave(() -> BatchSlave.Type.DELETE, columnNumber);
      case BATCH_INCREMENT:
        return new BatchSlave(() -> BatchSlave.Type.INCREMENT, columnNumber);
      case BATCH_GET:
        return new BatchSlave(() -> BatchSlave.Type.GET, columnNumber);
      case BATCH_RANDOM:
        return new BatchSlave(() -> {
          int index = (int) (Math.random() * BatchSlave.Type.values().length);
          return BatchSlave.Type.values()[index];
        }, columnNumber);
      default:
        throw new RuntimeException("Unsupported operation:" + this);
    }
  }
}
