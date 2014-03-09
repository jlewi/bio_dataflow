package contrail.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import contrail.scaffolding.Library;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.AvroFileUtil;
import contrail.util.ContrailLogger;
import contrail.util.FileHelper;

/**
 * A simple command line utility for adding information about a read
 * library to a file.
 *
 * Information about the library is passed as command line arguments. The
 * data is encoded as an Array of contrail.scaffolding.Library records.
 */
public class AddLibraryToFile extends NonMRStage {
  private static ContrailLogger sLogger = ContrailLogger.getLogger(
      AddLibraryToFile.class);

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    Map<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    ParameterDefinition reads = new ParameterDefinition(
        "reads", "A comma separated list of globs matching the read files.",
        String.class,
        null);

    ParameterDefinition minSize = new ParameterDefinition(
        "min_size", "The minimum size of the reads.",
        Integer.class,
        null);

    ParameterDefinition maxSize = new ParameterDefinition(
        "max_size", "The maximum size of the reads.",
        Integer.class,
        null);

    ParameterDefinition name = new ParameterDefinition(
        "name", "The name for the library.",
        String.class,
        null);

    ParameterDefinition libraryFile = new ParameterDefinition(
        "library_file",
        "The path to the file where the library should be writtend",
        String.class,
        null);

    for (ParameterDefinition def: new ParameterDefinition[] {
          reads, minSize, maxSize, name, libraryFile}) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  /**
   * Load any libraries already in the output.
   */
  private Map<String, Library> loadExisting() {
    String libraryFile = (String) this.stage_options.get("library_file");
    HashMap<String, Library> libraries = new HashMap<String, Library>();

    Path path = new Path(libraryFile);
    FileSystem fs = null;
    List<Library> records = null;
    try {
      fs = path.getFileSystem(getConf());
      if (!fs.exists(path)) {
        return libraries;
      }
      records = AvroFileUtil.readJsonArray(getConf(), path, Library.SCHEMA$);
      for (Library lib : records) {
        if (libraries.containsKey(lib.getName().toString())) {
          sLogger.fatal(String.format(
              "More than one library was found named %s in %s.", lib.getName(),
              libraryFile));
        }
        libraries.put(lib.getName().toString(), lib);
      }
    } catch(IOException e) {
      sLogger.fatal(
          "There was problem loading existing libraries from: " + libraryFile, e);
    }

    return libraries;
  }

  private static class NameComparator implements Comparator<Library> {
    @Override
    public int compare(Library o1, Library o2) {
      return o1.getName().toString().compareTo(o2.getName().toString());
    }
  }

  @Override
  protected void stageMain() {
    Map<String, Library> libraries = loadExisting();

    Library lib = new Library();
    lib.setName((String) stage_options.get("name"));
    lib.setMaxSize((Integer) stage_options.get("max_size"));
    lib.setMinSize((Integer) stage_options.get("min_size"));
    String readsGlob = (String) stage_options.get("reads");
    lib.setFiles(new ArrayList<CharSequence>());
    ArrayList<Path> matches =
        FileHelper.matchListOfGlobsWithDefault(getConf(), readsGlob, "");
    if (matches.isEmpty()) {
      sLogger.fatal("No files matched: " + readsGlob);
    }
    for (Path p : matches) {
      lib.getFiles().add(p.toString());
    }

    if (libraries.containsKey(lib.getName())) {
      sLogger.fatal(String.format(
          "Cannot add library %s; there is already a library with that name.",
          lib.getName()));
    }

    libraries.put(lib.getName().toString(), lib);

    String libraryFile = (String) stage_options.get("library_file");

    // Sort the libraries by name.
    List<Library> sortedLibs = new ArrayList<Library>();
    sortedLibs.addAll(libraries.values());
    Collections.sort(sortedLibs, new NameComparator());
    AvroFileUtil.prettyPrintJsonArray(
        getConf(), new Path(libraryFile), sortedLibs);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new AddLibraryToFile(), args);
    System.exit(res);
  }
}
