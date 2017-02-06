package codes.chia7712.contributor.view;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class Arguments {

  private final List<String> options;
  private final List<String> keys;
  private final List<String> otherMsg;
  private final Map<String, String> input = new TreeMap<>();

  public Arguments(final List<String> keys) {
    this(keys, Collections.EMPTY_LIST);
  }

  public Arguments(final List<String> keys, final List<String> options) {
    this(keys, options, Collections.EMPTY_LIST);
  }

  public Arguments(final List<String> keys, final List<String> options, final List<String> otherMsg) {
    this.keys = new ArrayList<>(keys.size());
    keys.forEach(v -> this.keys.add(format(v)));
    this.options = new ArrayList<>(options.size());
    options.forEach(v -> this.options.add(format(v)));
    this.otherMsg = new ArrayList<>(otherMsg.size());
    this.otherMsg.addAll(otherMsg);
  }

  public String get(final String key, final String defaultValue) {
    return input.getOrDefault(format(key), defaultValue);
  }

  public <T extends Enum> T get(final Class<T> key, T defaultValue) {
    String v = getValue(key.getClass().getSimpleName());
    if (v == null) {
      return defaultValue;
    } else {
      try {
        return (T) key.getMethod("valueOf", String.class).invoke(null, v);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      } 
    }
  }

  public String get(final String key) {
    return getValue(key);
  }

  public int getInt(final String key) {
    return Integer.valueOf(getValue(key));
  }

  public long getLong(final String key) {
    return Long.valueOf(getValue(key));
  }

  public int getInt(final String key, final int defaultValue) {
    String value = getValue(key);
    if (value != null) {
      return Integer.valueOf(value);
    } else {
      return defaultValue;
    }
  }
  public boolean getBoolean(final String key, final boolean defaultValue) {
    String value = getValue(key);
    if (value != null) {
      return Boolean.valueOf(value);
    } else {
      return defaultValue;
    }
  }
  public long getLong(final String key, final long defaultValue) {
    String value = getValue(key);
    if (value != null) {
      return Long.valueOf(value);
    } else {
      return defaultValue;
    }
  }

  private String getValue(String key) {
    return input.get(format(key));
  }

  public void validate(final String[] args) {
    for (String arg : args) {
      String[] splits = arg.split("=");
      if (splits.length != 2) {
        throw new IllegalArgumentException("The \"" + arg + "\" is invalid");
      }
      input.put(format(splits[0]), splits[1]);
    }
    List<String> missedKeys = keys.stream().filter(v -> !input.containsKey(v)).collect(Collectors.toList());
    if (!missedKeys.isEmpty()) {
      throw new IllegalArgumentException(generateErrorMsg(missedKeys));
    }
  }

  private static String format(String s) {
    return s.toLowerCase();
  }
  private String generateErrorMsg(List<String> missedKeys) {
    StringBuilder builder = new StringBuilder("These arguments is required: ");
    missedKeys.forEach(v -> builder.append("<").append(v).append("> "));
    if (!options.isEmpty()) {
      builder.append("\nThese arguments is optional: ");
      options.forEach(v -> builder.append("<").append(v).append("> "));
    }
    if (otherMsg != null) {
      builder.append("\nothers:\n");
      otherMsg.forEach((s) -> builder.append(s).append("\n"));
    }
    return builder.toString();
  }
}
