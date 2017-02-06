
package codes.chia7712.contributor.util;

import java.util.Optional;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public final class EnumUtil {
  private static final Log LOG = LogFactory.getLog(EnumUtil.class);
  public static <T extends Enum> Optional<T> find(String value, Class<T> clz) {
    if (value == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(Enum.valueOf(clz, value.toUpperCase()));
    } catch (IllegalArgumentException e) {
      LOG.error(e);
      return Optional.empty();
    }
  }
  private EnumUtil(){}
}
