
package codes.chia7712.contributor.view;

import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;


public class TestArguments {

  @Test
  public void testValidate() {
    String[] args = new String[]{"table=t", "cf=cccc"};
    Arguments arguments = new Arguments(Arrays.asList("table", "cf"));
    arguments.validate(args);
    assertEquals("t", arguments.get("table"));
    assertEquals("cccc", arguments.get("cf"));
  }

  @Test
  public void testFailValidate() {
    String[] args = new String[]{"table=t"};
    Arguments arguments = new Arguments(Arrays.asList("table", "cf"));
    try {
      arguments.validate(args);
      fail("Where is the IllegalArgumentException?");
    } catch (IllegalArgumentException e) {
    }
  }
}
