package codes.chia7712.contributor.operation;

import codes.chia7712.contributor.util.EnumUtil;
import java.util.Optional;



public enum RequestMode {
  BATCH, NORMAL;
  public static Optional<RequestMode> find(String value) {
    return EnumUtil.find(value, RequestMode.class);
  }
}
