package codes.chia7712.contributor.operation;

public enum Operation {
  NORMAL_PUT, BATCH_PUT, BATCH_DELETE, BATCH_INCREMENT, BATCH_GET, BATCH_RANDOM;

  public static String getDescription() {
    StringBuilder builder = new StringBuilder("op:");
    for (Operation op : Operation.values()) {
      builder.append(op.name())
              .append(",");
    }
    builder.deleteCharAt(builder.length() - 1);
    return builder.toString();
  }

  public Slave newSlave(int qualifier) {
    switch (this) {
      case NORMAL_PUT:
        return new PutSlave(qualifier);
      case BATCH_PUT:
        return new BatchSlave(() -> BatchSlave.Type.PUT, qualifier);
      case BATCH_DELETE:
        return new BatchSlave(() -> BatchSlave.Type.DELETE, qualifier);
      case BATCH_INCREMENT:
        return new BatchSlave(() -> BatchSlave.Type.INCREMENT, qualifier);
      case BATCH_GET:
        return new BatchSlave(() -> BatchSlave.Type.GET, qualifier);
      case BATCH_RANDOM:
        return new BatchSlave(() -> {
          int index = (int) (Math.random() * BatchSlave.Type.values().length);
          return BatchSlave.Type.values()[index];
        }, qualifier);
      default:
        throw new RuntimeException("Unsupported operation:" + this);
    }
  }
}
