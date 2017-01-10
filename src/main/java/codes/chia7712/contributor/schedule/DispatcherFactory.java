package codes.chia7712.contributor.schedule;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public final class DispatcherFactory {

  public static Dispatcher get(final long max, int constant) {
    return get(max, () -> constant);
  }

  public static Dispatcher get(final long max, Supplier<Integer> bounder) {
    return new SimpleDispatcher(max, bounder);
  }

  private DispatcherFactory() {

  }

  private static class SimpleDispatcher implements Dispatcher {

    private final long max;
    private final Supplier<Integer> bounder;
    private final AtomicLong dispatchedRows = new AtomicLong(0);
    private final AtomicLong committedRows = new AtomicLong(0);

    SimpleDispatcher(final long max, Supplier<Integer> bounder) {
      assert max > 0;
      this.max = max;
      this.bounder = bounder;
    }

    private void commit(int rows) {
      committedRows.addAndGet(rows);
    }

    @Override
    public long getDispatchedRows() {
      return dispatchedRows.get();
    }

    @Override
    public long getCommittedRows() {
      return committedRows.get();
    }

    @Override
    public Optional<Packet> getPacket() {
      final int expectedRows = bounder.get();
      assert expectedRows > 0;
      final long startIndex = dispatchedRows.getAndAdd(expectedRows);
      long endIndex;
      if (startIndex >= max) {
        return Optional.empty();
      } else if (startIndex + expectedRows <= max) {
        endIndex = startIndex + expectedRows;
      } else {
        dispatchedRows.set(max);
        endIndex = max;
      }
      return Optional.of(new Packet() {
        private final long size = endIndex - startIndex;
        private long index = startIndex;
        private int uncommittedRows = 0;

        @Override
        public void commit() {
          SimpleDispatcher.this.commit(uncommittedRows);
          uncommittedRows = 0;
        }

        @Override
        public boolean hasNext() {
          return index < endIndex;
        }

        @Override
        public Long next() {
          if (index >= endIndex) {
            throw new IllegalArgumentException("The uncommittedRows shouldn't be bigger than totalRows");
          }
          ++index;
          ++uncommittedRows;
          return index;
        }

        @Override
        public long size() {
          return size;
        }
      });
    }
  }

}
