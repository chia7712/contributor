
package codes.chia7712.contributor.jdbc;

import java.util.Optional;

/**
 * Defines the database type.
 */
public enum DBType {
  /**
   * Oracle sql server.
   */
  ORACLE("oracle"),
  /**
   * Microsoft sql server.
   */
  SQL_SERVER("sqlserver"),
  /**
   * Apache phoenix sql.
   */
  PHOENIX("phoenix"),
  /**
   * My SQL.
   */
  MY_SQL("mysql");
  /**
   * DB description.
   */
  private final String description;

  /**
   * Constructs the DBType with the description.
   *
   * @param desc DB description
   */
  DBType(final String desc) {
    description = desc;
  }

  /**
   * @return DB description
   */
  public String getDescription() {
    return description;
  }

  /**
   * Picks up a database type for specified keyword. The DBType will return if
   * the keyword contains the description of database. For example: keyword =
   * xxxxphoenix, it gets the phoenix type
   *
   * @param keyword The keyword that contains the db description
   * @return DBType
   */
  public static Optional<DBType> pickup(final String keyword) {
    for (DBType type : DBType.values()) {
      if (keyword.toLowerCase().contains(type.getDescription())) {
        return Optional.of(type);
      }
    }
    return Optional.empty();
  }
}
