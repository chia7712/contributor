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
  private final String otherMsg;
  private final Map<String, String> input = new TreeMap<>();

  public Arguments(final List<String> keys) {
    this(keys, Collections.EMPTY_LIST, null);
  }

  public Arguments(final List<String> keys, final List<String> options) {
    this(keys, options, null);
  }

  public Arguments(final List<String> keys, final List<String> options, final String otherMsg) {
    this.keys = new ArrayList<>(keys.size());
    keys.forEach(v -> this.keys.add(v.toLowerCase()));
    this.options = new ArrayList<>(options.size());
    options.forEach(v -> this.options.add(v.toLowerCase()));
    this.otherMsg = otherMsg;
  }

  public String get(final String key, final String defaultValue) {
    return input.getOrDefault(key, defaultValue);
  }

  public String get(final String key) {
    return input.get(key);
  }

  public int getInt(final String key) {
    return Integer.valueOf(input.get(key));
  }

  public long getLong(final String key) {
    return Long.valueOf(input.get(key));
  }

  public int getInt(final String key, final int defaultValue) {
    String value = input.get(key);
    if (value != null) {
      return Integer.valueOf(value);
    } else {
      return defaultValue;
    }
  }

  public long getLong(final String key, final long defaultValue) {
    String value = input.get(key);
    if (value != null) {
      return Long.valueOf(value);
    } else {
      return defaultValue;
    }
  }

  public void validate(final String[] args) {
    for (String arg : args) {
      String[] splits = arg.split("=");
      if (splits.length != 2) {
        throw new IllegalArgumentException("The \"" + arg + "\" is invalid");
      }
      input.put(splits[0].toLowerCase(), splits[1]);
    }
    List<String> missedKeys = keys.stream().filter(v -> !input.containsKey(v)).collect(Collectors.toList());
    if (!missedKeys.isEmpty()) {
      throw new IllegalArgumentException(generateErrorMsg(missedKeys));
    }
  }

  private String generateErrorMsg(List<String> missedKeys) {
    StringBuilder builder = new StringBuilder("These arguments is required: ");
    missedKeys.forEach(v -> builder.append("<").append(v).append("> "));
    if (!options.isEmpty()) {
      builder.append("\nThese arguments is optional: ");
      options.forEach(v -> builder.append("<").append(v).append("> "));
    }
    if (otherMsg != null) {
      builder.append("\nothers: ")
              .append(otherMsg);
    }
    return builder.toString();
  }
}
