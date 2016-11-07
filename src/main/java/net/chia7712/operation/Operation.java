
package net.chia7712.operation;

public enum Operation {
  PUT, DELETE, INCREMENT, APPEND, GET, BATCH;
  public Slave newSlave() {
    switch (this) {
      case PUT:
        return new PutWorker();
      case DELETE:
        return new DeleteWorker();
      case INCREMENT:
        return new IncrementWorker();
      case APPEND:
        return new AppendWorker();
      case GET:
        return new GetWorker();
      case BATCH:
        return new BatchWorker();
      default:
        throw new RuntimeException("Unsupported operation:" + this);
    }
  }
}
