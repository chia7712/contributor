
package codes.chia7712.contributor.jdbc;

public interface RandomData {
  boolean getBoolean();
  long getLong();
  long getCurrentTimeMs();
  int getInteger();
  short getShort();
  double getDouble();
  float getFloat();
  String getStringWithRandomSize(int limit);
  String getString(int size);
}
