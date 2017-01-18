package codes.chia7712.contributor.operation;

class WorkResult {

  private final long rowCount;
  private final long cellCount;

  WorkResult(final long rowCount, final long cellCount) {
    this.rowCount = rowCount;
    this.cellCount = cellCount;
  }

  long getRowCount() {
    return rowCount;
  }

  long getCellCount() {
    return cellCount;
  }

  @Override
  public String toString() {
    return "rows:" + rowCount
            + ", cells:" + cellCount;
  }
}
