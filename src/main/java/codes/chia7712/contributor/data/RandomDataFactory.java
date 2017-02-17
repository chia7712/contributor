package codes.chia7712.contributor.data;

import java.util.Arrays;
import java.util.Random;

public class RandomDataFactory {

  private RandomDataFactory() {
  }

  public static RandomData create() {
    return new SimpleRandomData();
  }

  private static class SimpleRandomData implements RandomData {

    private final Random rn = new Random();

    @Override
    public long getLong() {
      return rn.nextLong();
    }

    @Override
    public long getCurrentTimeMs() {
      return System.currentTimeMillis();
    }

    @Override
    public int getInteger() {
      return rn.nextInt();
    }

    @Override
    public short getShort() {
      return (short) rn.nextInt();
    }

    @Override
    public double getDouble() {
      return rn.nextDouble();
    }

    @Override
    public float getFloat() {
      return rn.nextFloat();
    }

    @Override
    public String getStringWithRandomSize(int limit) {
      return getString(Math.max(1, Math.abs(rn.nextInt(limit))));
    }

    @Override
    public String getString(int size) {
      assert size >= 0;
      char[] buf = new char[size];
      Arrays.fill(buf, 'a');
      return String.copyValueOf(buf);
    }

    @Override
    public byte[] getBytes(int length) {
      byte[] b = new byte[length];
      rn.nextBytes(b);
      return b;
    }

    @Override
    public boolean getBoolean() {
      return rn.nextBoolean();
    }

    @Override
    public int getInteger(int bound) {
      return rn.nextInt(bound);
    }
  }
}
