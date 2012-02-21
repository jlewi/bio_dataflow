package contrail.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import contrail.graph.NodeMerger.MergeInfo;
import contrail.sequences.Alphabet;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

public class TestNodeMerger extends NodeMerger {

  // Random number generator.
  private Random generator;
  @Before 
  public void setUp() {
    // Create a random generator so we can make a test repeatable
    generator = new Random(103);
  }
  
  /**
   * Return a random sequence of characters of the specified length
   * using the given alphabet.
   * 
   * @param length
   * @param alphabet
   * @return
   */
  public static String randomString(
      Random generator, int length, Alphabet alphabet) {
    // Generate a random sequence of the indicated length;
    char[] letters = new char[length];
    for (int pos = 0; pos < length; pos++) {
      // Randomly select the alphabet
      int rnd_int = generator.nextInt(alphabet.size()); 
      letters[pos] = alphabet.validChars()[rnd_int];        
    }
    return String.valueOf(letters);
  }
  
  /**
   * Container for the data used to test merging sequences.
   *
   */
  protected static class SequenceTestCase {
    public Sequence canonical_src;
    public Sequence canonical_dest;
    public StrandsForEdge strands;
    public int overlap;
    
    public String merged_sequence;
    
    public List<R5Tag> src_r5tags;
    public List<R5Tag> dest_r5tags;
    /**
     * We store the suffixes corresponding to the r5tags assigned 
     * to each sequence. This way we can test whether the alignment is correct
     * after the merge.
     */
    HashMap<String, String> r5prefix;
    
    
    /**
     * Create some R5Tags for this sequence.  
     * @param readid: Id for the read. Used so we can match up the aligned
     *   value with this read.
     * @param ntags: The number of tags.
     * @param canonical: The canonical sequence.
     * @param r5tags: List to add the tags to.
     * @param prefixes: The prefixes of the reads that are aligned with this
     *   sequence.
     */
    public static void createR5Tags(
        Random generator, String readid, int ntags, Sequence canonical, 
        List<R5Tag> r5tags, HashMap<String, String> prefixes) {
      for (int i = 0; i < ntags; i++) {
        // Randomly select the offset and strand.
        R5Tag tag = new R5Tag();
        r5tags.add(tag);
        tag.setTag(readid + ":" + i);
        tag.setOffset(generator.nextInt(canonical.size()));
        tag.setStrand(DNAStrandUtil.random(generator));
        
        String prefix = R5TagUtil.prefixForTag(canonical,tag).toString();
        
        prefixes.put(tag.getTag().toString(), prefix);                
      }
    }
    /**
     * Generate a random test case for the merging of two sequences.
     * @return
     */
    public static SequenceTestCase random(
        Random generator, int src_length, int dest_length, int overlap, 
        int num_tags) {
      
      SequenceTestCase testcase = new SequenceTestCase();
                        
      DNAStrand src_strand;
      {
        Sequence src;
        // Randomly generate a sequence for the source.
        src_strand = DNAStrandUtil.random(generator);        
        String src_str = randomString(
            generator, src_length, DNAAlphabetFactory.create());
        src = new Sequence(src_str, DNAAlphabetFactory.create());
        
        testcase.canonical_src = DNAUtil.canonicalseq(src);        
        testcase.merged_sequence = 
            DNAUtil.canonicalToDir(testcase.canonical_src, src_strand).toString();
      }
      
      DNAStrand dest_strand;
      // Construct the destination sequence.
      {
        // Overlap by at most size(src) - 1 and at least 1.
        testcase.overlap = overlap;
        // Make a copy of canonical_src before we modify it.
        Sequence dest = new Sequence(testcase.canonical_src); 
        dest = DNAUtil.canonicalToDir(dest, src_strand); 
        dest = dest.subSequence(dest.size() - testcase.overlap, dest.size());
        
        String non_overlap = randomString(
            generator, dest_length - overlap, DNAAlphabetFactory.create());
        
        testcase.merged_sequence = testcase.merged_sequence + non_overlap;
        Sequence seq_nonoverlap = new Sequence(
            non_overlap, DNAAlphabetFactory.create());
        
        dest.add(seq_nonoverlap);
        
        dest_strand = DNAUtil.canonicaldir(dest); 
        testcase.canonical_dest = DNAUtil.canonicalseq(dest);
      }            
      
      testcase.strands = StrandsUtil.form(src_strand, dest_strand);
      
      // Generate the R5Tags.
      testcase.src_r5tags = new ArrayList<R5Tag>();
      testcase.dest_r5tags = new ArrayList<R5Tag>();
      testcase.r5prefix = new HashMap<String, String>();
      
      // The validation of the tag alignment depends on the tags for the R5Tags 
      // beginning with "src" and "dest" respectively. 
      createR5Tags(
          generator, "src", num_tags, testcase.canonical_src, 
          testcase.src_r5tags, testcase.r5prefix);
      
      createR5Tags(
          generator, "dest", num_tags, testcase.canonical_dest, 
          testcase.dest_r5tags, testcase.r5prefix);
      return testcase;
    }
    
    /**
     * Generate a random test case for the merging of two sequences.
     * @return
     */
    public static SequenceTestCase random(Random generator) {
      int MAXLENGTH = 100;
      int MINLENGTH = 5;
      
      int src_length = generator.nextInt(MAXLENGTH - MINLENGTH) + MINLENGTH;        
      
      int overlap = generator.nextInt(src_length - 2) + 1;
      int dest_length = generator.nextInt(MAXLENGTH - overlap) + overlap + 1;

      int num_tags = generator.nextInt(15) + 2;
      
      return SequenceTestCase.random(
          generator, src_length, dest_length, overlap, num_tags);
    }
  }
    
  /**
   * Check that NodeMerger.alignTags returns the correct result.
   * @param testcase
   * @param merge_info
   */
  protected void checkAlignTags(
      SequenceTestCase testcase, MergeInfo merge_info) {
    
    // We check the aligned tags.
    List<R5Tag> aligned = alignTags(
        merge_info, testcase.src_r5tags, testcase.dest_r5tags);
    
    int true_size = testcase.src_r5tags.size() + testcase.dest_r5tags.size();
    assertEquals(true_size, aligned.size());
        
    for (R5Tag tag: aligned) {
      String prefix = R5TagUtil.prefixForTag(
          merge_info.canonical_merged, tag).toString();
      
      // The prefix returned from the merge sequence could be longer
      // than the original prefix, so we only compare the length of
      // the original prefix.
      assertTrue(testcase.r5prefix.containsKey(tag.getTag().toString()));
      String true_prefix = testcase.r5prefix.get(tag.getTag().toString());
      
      assertTrue(prefix.startsWith(true_prefix));
    }
  }
  @Test
  public void testMergeSequences() {
    int ntrials = 20;
    for (int trial = 0; trial < ntrials; trial++) {
      SequenceTestCase testcase = SequenceTestCase.random(generator);
      MergeInfo merge_info;
      {
        // Make copies of the sequences so that merge won't modify them.
        Sequence src = new Sequence(testcase.canonical_src);
        Sequence dest = new Sequence(testcase.canonical_dest);
        merge_info = 
            mergeSequences(src, dest, testcase.strands, testcase.overlap);
      }
      
      Sequence merged_sequence = DNAUtil.canonicalToDir(
          merge_info.canonical_merged, merge_info.merged_strand);
      assertEquals(testcase.merged_sequence, merged_sequence.toString());
      
      
      checkAlignTags(testcase, merge_info);
    }        
  }
}
