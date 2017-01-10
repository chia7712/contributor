package codes.chia7712.contributor.jdbc;

import java.util.Optional;

/**
 * Used for parseing the table name and schema in JDBC.
 */
public final class TableName implements Comparable<TableName> {

  /**
   * Schema. Nullable.
   */
  private final Optional<String> schema;
  /**
   * Table name.
   */
  private final String name;

  /**
   * Constructs a TableName with specified str. For example: RunSummary ->
   * schema = null, name = RUNSUMMARY CDC.RunSummary -> schema = CDC, name =
   * RUNSUMMARY cdc.RunSummary -> schema = CDC, name = RUNSUMMARY
   * "CDC.RunSummary" -> schema = null, name = "CDC.RunSummary" "RunSummary" ->
   * schema = null, name = "RunSummary" CDC."RunSummary" -> schema = CDC, name =
   * "RunSummary" "cDc".RunSummary -> schema = "cDc", name = "RUNSUMMARY
   * "cDc"."RunSummary" -> schema = "cDc", name = "RunSummary"
   *
   * @param str The string to parse
   */
  public TableName(final String str) {
    StringBuilder strToUpper = new StringBuilder();
    boolean toUpper = true;
    for (int i = 0; i != str.length(); ++i) {
      char c = str.charAt(i);
      if (c == '\"') {
        toUpper = !toUpper;
        strToUpper.append('\"');
      } else if (toUpper) {
        strToUpper.append(Character.toUpperCase(c));
      } else {
        strToUpper.append(c);
      }
    }
    StringBuilder tableNameBuf = new StringBuilder();
    StringBuilder schemaBuf = new StringBuilder();
    boolean inQuote = false;
    boolean findDot = false;
    for (int i = 0; i != strToUpper.length(); ++i) {
      char c = strToUpper.charAt(i);
      if (findDot) {
        tableNameBuf.append(c);
        continue;
      }
      if (c == '\"') {
        inQuote = !inQuote;
      }
      if (c == '.' && !inQuote) {
        findDot = true;
        continue;
      }
      schemaBuf.append(c);
    }
    if (tableNameBuf.length() == 0) {
      name = schemaBuf.toString();
      schema = Optional.empty();
    } else {
      name = tableNameBuf.toString();
      schema = Optional.of(schemaBuf.toString());
    }
  }

  /**
   * @return Table name
   */
  public String getName() {
    return name;
  }

  /**
   * @param defaultValue
   * @return Schema
   */
  public String getSchema(final String defaultValue) {
    return schema.orElse(defaultValue);
  }

  /**
   * @return Schema
   */
  public Optional<String> getSchema() {
    return schema;
  }

  /**
   * @return {Scheam}.{Table name}
   */
  public String getFullName() {
    if (schema.isPresent()) {
      return schema.get() + "." + name;
    } else {
      return name;
    }
  }

  @Override
  public String toString() {
    return getFullName();
  }

  @Override
  public int compareTo(TableName o) {
    return getFullName().compareTo(o.getFullName());
  }
}
