package contrail.stages;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

public class TestParameterDefinition {
  protected CommandLine parseCommandLine(
      ParameterDefinition def, String[] application_args) {
    Options options = new Options();
    options.addOption(def.getOption());

    CommandLineParser parser = new GnuParser();
    CommandLine line = null;
    try {
      line = parser.parse(options, application_args);
    }
    catch( ParseException exp )
    {
      // oops, something went wrong
      System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
      System.exit(1);
    }
    return line;
  }

  @Test
  public void testBoolean() {
    // Test that we can parse a boolean option correctly when its default
    // value is true or false.
    boolean[] deafults = {true, false};
    for (boolean paramDefault : deafults) {
      ParameterDefinition cleanup = new ParameterDefinition(
          "cleanup", "description", Boolean.class, paramDefault);
      {
        String args[] = {"--cleanup=true"};
        CommandLine line = parseCommandLine(cleanup, args);
        Object value = cleanup.parseCommandLine(line);
        assertFalse(value == null);
        assertTrue((Boolean) (value));
      }

      {
        String args[] = {"--cleanup=false"};
        CommandLine line = parseCommandLine(cleanup, args);
        Object value = cleanup.parseCommandLine(line);
        assertFalse(value == null);
        assertFalse((Boolean) (value));
      }
    }
  }
}
