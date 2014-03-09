package contrail.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.scaffolding.Library;
import contrail.sequences.FastUtil;
import contrail.sequences.FastaRecord;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

public class TestAddLibraryToFile {
  @Test
  public void testAddLibraryToFile() throws IOException {
    File tempDir = FileHelper.createLocalTempDir();
    tempDir.deleteOnExit();

    List<Library> expectedLibs = new ArrayList<Library>();
    Random generator = new Random();

    String libraryPath = FilenameUtils.concat(
        tempDir.getAbsolutePath(), String.format("libraries.json"));

    for (int i = 0; i < 2; ++i) {
      Library lib = new Library();
      lib.setName(String.format("lib_%d", i));
      lib.setMaxSize(generator.nextInt());
      lib.setMinSize(generator.nextInt());
      lib.setFiles(new ArrayList<CharSequence>());
      for (int j = 0; j < 2; ++j) {
        String path = FilenameUtils.concat(
            tempDir.getAbsolutePath(),
            String.format("lib_%d_read_%d.fasta", i, j));
        lib.getFiles().add(path);

        // Create the file because AddLibraryToFIle checks it exists.
        FastaRecord record = new FastaRecord();
        record.setId("some id");
        record.setRead("ACTGC");

        FileOutputStream writer = new FileOutputStream(path);
        FastUtil.writeFastARecord(writer, record);
        writer.close();
      }

      expectedLibs.add(lib);

      AddLibraryToFile stage = new AddLibraryToFile();
      stage.setConf(new Configuration());

      // Write the library.
      stage.setParameter("name", lib.getName());
      stage.setParameter("min_size", lib.getMinSize());
      stage.setParameter("max_size", lib.getMaxSize());
      stage.setParameter("reads", StringUtils.join(lib.getFiles(), ","));
      stage.setParameter("library_file", libraryPath);

      assertTrue(stage.execute());

      // Read the records back and validate them.
      List<Library> actualLibs = AvroFileUtil.readJsonArray(
          stage.getConf(), new Path(libraryPath), lib.getSchema());

      assertEquals(expectedLibs, actualLibs);
    }
  }
}
