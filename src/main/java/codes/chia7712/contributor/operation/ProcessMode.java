package codes.chia7712.contributor.operation;

import codes.chia7712.contributor.util.EnumUtil;
import java.util.Optional;


public enum ProcessMode {
  SYNC, BUFFER;
  public static Optional<ProcessMode> find(String value) {
    return EnumUtil.find(value, ProcessMode.class);
  }
}
