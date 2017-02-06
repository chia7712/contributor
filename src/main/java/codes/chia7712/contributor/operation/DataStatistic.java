package codes.chia7712.contributor.operation;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;

public class DataStatistic {

  private final LongAdder processingRows = new LongAdder();
  private final LongAdder committedRows = new LongAdder();
  private final ConcurrentMap<Record, LongAdder> statistic = new ConcurrentSkipListMap<>();

  public void addNewRows(Record record, long delta) {
    assert delta >= 0;
    processingRows.add(delta);
  }

  public void finishRows(Record record, long delta) {
    assert delta >= 0;
    statistic.computeIfAbsent(record, k -> new LongAdder())
            .add(delta);
    processingRows.add(-delta);
    committedRows.add(delta);
  }

  public long getCommittedRows() {
    return committedRows.longValue();
  }

  public long getProcessingRows() {
    return processingRows.longValue();
  }

  public void consume(BiConsumer<Record, LongAdder> f) {
    statistic.forEach(f::accept);
  }

  public static class Record implements Comparable<Record> {

    private final ProcessMode processMode;
    private final RequestMode requestMode;
    private final DataType dataType;
    private final String name;

    public Record(final ProcessMode processMode, final RequestMode requestMode,
            final DataType dataType) {
      this.processMode = processMode;
      this.requestMode = requestMode;
      this.dataType = dataType;
      this.name = new StringBuilder(processMode.name())
              .append("/")
              .append(requestMode.name())
              .append("/")
              .append(dataType.name())
              .toString();
    }

    public ProcessMode getProcessMode() {
      return processMode;
    }

    public RequestMode getRequestMode() {
      return requestMode;
    }

    public DataType getDataType() {
      return dataType;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || !(o instanceof Record)) {
        return false;
      }
      return compareTo((Record) o) == 0;
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 71 * hash + Objects.hashCode(this.processMode);
      hash = 71 * hash + Objects.hashCode(this.requestMode);
      hash = 71 * hash + Objects.hashCode(this.dataType);
      return hash;
    }

    @Override
    public int compareTo(Record o) {
      int rval = processMode.compareTo(o.getProcessMode());
      if (rval != 0) {
        return rval;
      }
      rval = requestMode.compareTo(o.getRequestMode());
      if (rval != 0) {
        return rval;
      }
      return dataType.compareTo(o.getDataType());
    }

    @Override
    public String toString() {
      return name;
    }

  }
}
