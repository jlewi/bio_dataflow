package contrail.stages;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.mapred.JobConf;

/**
 * An abstract class for defining a contrail parameter.
 *
 * For each parameter in contrail we define an instance of this class. This
 * instance doesn't store the value of the parameter but rather provides
 * all of the handling for that parameter such as code to parse that parameter
 * from the command line or a hadoop configuration.
 */
public class ParameterDefinition {
  protected String name_;
  protected String description_;
  protected Class type_;
  protected Object default_value_;

  protected String short_name_;
  /**
   * Construct the parameter.
   * @param name: Name for the parameter.
   * @param description: Description for the parameter.
   * @param type: The type for the parameter.
   * @param default_value: Default value for the parameter.
   *   Set this to null if there is no default value.
   */
  public ParameterDefinition (
      String name, String description, Class type, Object default_value) {
   this.name_ = name;
   this.description_ = description;
   this.type_ = type;
   this.default_value_ = default_value;
   this.short_name_ = name;
  }

  public Option getOption() {
    String description = description_;
    if (default_value_ != null) {
      description = description + "(default: " + default_value_.toString() +
          ")";
    }
    if (type_.equals(Boolean.class)) {
      // Boolean arguments don't require an argument.
      return new Option(short_name_, name_, false, description_);
    } else {
      return new Option(short_name_, name_, true, description_);
    }
  }

  public String getName() {
    return name_;
  }

  /**
   * A short name for the parameter.
   * @param short_name
   * @return
   */
  public void setShortName(String short_name) {
    short_name_ = short_name;
  }

  public Class getType() {
    return type_;
  }

  /**
   * Parse out the value of the parameter from the command line.
   * @param line
   * @return
   */
  public Object parseCommandLine(CommandLine line) {
    String value = null;
    if (line.hasOption(name_)) {
      value = line.getOptionValue(name_);
    } else if (line.hasOption(short_name_)) {
     value = line.getOptionValue(short_name_);
    } else {
      // TODO(jlewi): Should we return null or throw an exception if
      // the parameter is missing?
      return null;
    }
    return fromString(value);
  }


  public Object getDefault() {
    // The allowed types are immutable so we don't have to worry about
    // the caller being able to change the value.
    return default_value_;
  }

  /**
   * Add a value for this parameter to the job configuration.
   */
  public void addToJobConf(JobConf conf, Object value) {
    if (!value.getClass().equals(type_)) {
      throw new RuntimeException(
          "The value isn't the proper type for parameter: " + name_ + " " +
          "The expected type is: " + type_.getName() + " The actual type is:" +
          value.getClass().getName());
    }

    if (type_.equals(String.class)) {
      conf.set(name_, (String)value);
    } else if (type_.equals(Long.class)) {
      conf.setLong(name_, (Long)(value));
    } else if (type_.equals(Boolean.class)) {
      conf.setBoolean(name_, (Boolean)(value));
    } else if (type_.equals(Integer.class)) {
      conf.setInt(name_, (Integer) value);
    } else if (type_.equals(Float.class)) {
      conf.setFloat(name_, (Float) value);
    } else {
      throw new RuntimeException("No handler for this type of parameter");
    }
  }

  /**
   * Parse the value of the parameter from a job configuration file.
   */
  public Object parseJobConf(JobConf conf) {
    String value = conf.get(name_);
    if (value == null) {
      return default_value_;
    }
    return fromString(value);
  }

  /**
   * Convert a string representation of this parameter to an instance of
   * the appropriate type.
   * @param value
   * @return
   */
  protected Object fromString(String value) {
    if (type_.equals(String.class)) {
      return value;
    } else if (type_.equals(Long.class)) {
      return Long.parseLong(value);
    } else if (type_.equals(Boolean.class)) {
      return Boolean.parseBoolean(value);
    } else if (type_.equals(Integer.class)) {
      return Integer.parseInt(value);
    } else if (type_.equals(Float.class)) {
      return Float.parseFloat(value);
    } else {
      throw new RuntimeException("No handler for this type of parameter");
    }
  }
}
