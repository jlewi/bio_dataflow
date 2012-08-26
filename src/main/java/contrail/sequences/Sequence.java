package contrail.sequences;

import contrail.sequences.Alphabet;
import contrail.util.ByteUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Provides a wrapper class for accessing the sequence compactly encoded in an array of integers.
 *
 * TODO(jlewi): We should be using long's when computing the number of bits
 * in a sequence to avoid any potential issues with really long sequences
 * (e.g the human genome has 2.9 bp) and inefficient coding schemes.
 * @author jlewi
 */
public class Sequence implements Comparable<Sequence> {

  final int BITSPERITEM = 32;

  // Value to fill the array with;
  final int FILL_VALUE = 0;

  /**
   * The alphabet this sequence is using to encode the data.
   */
  private Alphabet alphabet;
  /**
   * The data we are pointing to
   * @param data
   */
  private int[] data;

  /**
   * Length of the sequence
   */
  private int length;

  /**
   * Empty constructor. This allows a single object to be reused
   * using the read methods to read into the sequence.
   */
  public Sequence(Alphabet alphabet) {
    length = 0;
    // Allocate a zero size array so that data is never null.
    data = new int[0];

    this.alphabet = alphabet;
  }

  /**
   * Initialize an empty sequence with space for the given number of letters.
   */
  public Sequence(Alphabet alphabet, int num_letters) {
    length = 0;

    int num_ints = (int)Math.ceil((alphabet.bitsPerLetter() * num_letters) /
                                  ((float)BITSPERITEM));

    // Allocate a zero size array so that data is never null.
    data = new int[num_ints];
    Arrays.fill(data, FILL_VALUE);
    this.alphabet = alphabet;
  }

  /**
   * Copy constructor.
   * TODO(jlewi): Add a unittest.
   */
  public Sequence(Sequence seq) {
    this.alphabet = seq.alphabet;
    this.length = seq.length;
    this.data = Arrays.copyOf(seq.data, seq.data.length);
  }
  /**
   * Construct a sequence for the given array of bytes. We steal a reference to this byte array
   * so make a copy if you want to keep the original.
   *
   * @param data
   * @param length - length of the sequence stored in data.
   * @param alphabet - Alphabet to use.
   */
  public Sequence(int[] data, int length, Alphabet alphabet) {
    this.data = data;
    this.alphabet = alphabet;
    this.length = length;

    if (alphabet.bitsPerLetter() > BITSPERITEM){
      throw new RuntimeException("Currently we only support encodings which use no more than 8 bits per character");
    }
  }

  /**
   * The actual size of the sequence encoded in the array.
   * @return
   */
  public int size() {
    return length;
  }

  /**
   * The number of characters that could be stored in the current byte array.
   * The actual length of the sequence is probably less
   * @return
   */
  public int capacity () {
    return (int)(Math.floor((double)(data.length * BITSPERITEM) / alphabet.bitsPerLetter()));
  }

  //Min3 gives minimum between 3 elements
  private int Min3(int a, int b, int c)    {
    a = Math.min(a, b);
    return Math.min(a,c);
  }

  /**
   * Calculate the edit distance (Levenshtein Distance)  between two sequences.
   *
   * see:
   * http://en.wikipedia.org/wiki/String_metric
   * http://www.ling.ohio-state.edu/~cbrew/795M/string-distance.html
   * */
  public int computeEditDistance(Sequence sequence2) {
    Sequence sequence1 = this;
    int len1 = sequence1.size();
    int len2 = sequence2.size();

    // The cost of the various errors.
    final int DEL_COST = 1;
    final int INS_COST = 1;
    final int SUB_COST = 1;

    int[][] d = new int[len1+1][len2+1];
    d[0][0] = 0;
    for (int i = 0; i < len1; i++)   {
      d[i+1][0] = d[i][0] + DEL_COST;
    }
    for (int j = 0; j < len2; j++)   {
      d[0][j+1] = d[0][j] + INS_COST;
    }
    for (int row = 1; row <= len1; ++row)   {
      char w1 = sequence1.at(row - 1);
      for (int col = 1; col <= len2; ++col)   {
        char w2 = sequence2.at(col-1);
        int e = (w1 == w2) ? 0 : SUB_COST;
        d[row][col] = Min3(d[row-1][col] + 1, d[row][col-1] + 1,
                           d[row-1][col-1]+e);
      }
    }
    return d[len1][len2];
  }

  /**
   * Set the character at the indicated position.
   *
   * @param pos    - The position in the sequence [0,size) to set.
   * @param letter - The value of the letters that we want to set
   *
   *
   * TODO (jeremy@lewi.us): What error checking should we do.
   */
  public void setAt(int pos, int[] letters) {
    // Determine the starting byte for this character.
    int byte_start = (int) Math.floor((double)(pos * alphabet.bitsPerLetter()) / BITSPERITEM);

    // Determine the first bit in the byte that belongs to this character.
    int bit_start = pos * alphabet.bitsPerLetter() - byte_start * BITSPERITEM;

    int letter;

    for (int lindex = 0; lindex < letters.length; lindex++) {
      letter = letters[lindex];
      // Shift the bits left so that it starts in the right position
      int start_val = letter << bit_start;

      // Create a bit mask which we can use to zero out the bits we
      // are interested in
      int set_mask = alphabet.letterMask() << bit_start;

      data[byte_start] = data[byte_start] & ~set_mask;
      data[byte_start] = data[byte_start] | start_val;

      // Set the next byte if necessary.
      int remaining_bits = alphabet.bitsPerLetter() - (BITSPERITEM-bit_start);
      if (remaining_bits > 0) {
        int right_shift = alphabet.bitsPerLetter() - remaining_bits;

        // zero out the first remaining_bits in the next byte
        set_mask = alphabet.letterMask() >>> right_shift;

        // zero out the bits we are about to set.
        data[byte_start+1] = data[byte_start+1] & ~set_mask;

        data[byte_start+1] = data[byte_start+1] | (letter >>> right_shift);
      }

      bit_start += alphabet.bitsPerLetter();
      if (bit_start >= BITSPERITEM) {
        bit_start = bit_start - BITSPERITEM;
        byte_start += 1;
      }
    }
  }

  /**
   * Set the character at the indicated position.
   *
   * @param pos    - The position in the sequence [0,size) to set.
   * @param letter - The value of the letter that we want to set
   *
   *
   * TODO (jeremy@lewi.us): What error checking should we do.
   */
  public void setAt(int pos, int letter) {
    setAt(pos, new int[] {letter});
  }

  /**
   * Set the character at the indicated position.
   *
   * @param letter - The letter in the alphabet to set at the indicated
   *                 position
   * @param pos    - The position in the sequence [0,size) to set.
   *
   * TODO (jeremy@lewi.us): What error checking should we do.
   */
  public void setAt(int pos, char letter) {
    setAt(pos, alphabet.letterToInt(letter));
  }

  /**
   * Set the character at the indicated position.
   *
   * @param letter - The letter in the alphabet to set at the indicated
   *                 position
   * @param pos    - The position in the sequence [0,size) to set.
   *
   * IMPORTANT: you must separatly adjust the length of the sequence.
   * TODO(jlewi): Should we automatically increment the size of the sequence
   *   if it is exapnded?
   * TODO (jeremy@lewi.us): What error checking should we do.
   */
  public void setAt(int pos, String letter) {
    setAt(pos, alphabet.letterToInt(letter));
  }

  /**
   * Construct a sequence from the given string representing the sequence of characters.
   * The string should not contain the end of sequence character. The length of the sequence is just the length of
   * the string.
   */
  public Sequence(char[] letters, Alphabet alphabet) {
    this.alphabet = alphabet;

    // Determine the number of bytes.
    int num_bytes = (int)(Math.ceil(((double)(letters.length * alphabet.bitsPerLetter()) / BITSPERITEM)));

    data = new int[num_bytes];


    boolean has_eos = alphabet.hasEOS();

    for (int pos = 0; pos < letters.length; pos++){
      if (has_eos && (letters[pos] == alphabet.EOS())) {
        String error = "Sequence should not contain the null/end of sequence character (" + alphabet.EOS ()
                       + "). Sequence is: " + new String(letters);
        throw new RuntimeException(error);
      }
      setAt(pos, letters[pos]);
    }
    length = letters.length;

    // Set null characters for all remaining characters in the array.
    int max_length = capacity();
    for (int pos = letters.length; pos < max_length; pos++){
      setAt(pos, alphabet.EOS());
    }
  }

  public Sequence(String letters, Alphabet alphabet) {
    this(letters.toCharArray(), alphabet);
  }

  /**
   * Return a pointer to the byte array for this sequence.
   */
  public int[] bytes(){
    return data;
  }

  /**
   * Return the character at the indicated position.
   *
   * @param - The position [0,length) of the character to return.
   */
  public char at(int pos) {
    int val = valAt(pos);

    return alphabet.intToLetter(val);
  }

  /**
   * Return the byte associated with the letter at the indicated position
   * All bits except the first alphabet.bitsPerLetter are 0.
   *
   * @param pos
   * @return
   */
  public int valAt(int pos){
    // Determine the starting byte for this character.
    int byte_start = (int) (Math.floor((double)(pos * alphabet.bitsPerLetter()) / BITSPERITEM));

    // Determine the first bit in the byte that belongs to this character.
    int bit_start = pos * alphabet.bitsPerLetter() - byte_start * BITSPERITEM;

    // Shift the bits in the start byte to the right so that it starts in the right position.
    int val = data[byte_start] >>> bit_start;

    // Now get the bits from the next byte
    int remaining_bits = alphabet.bitsPerLetter() - (BITSPERITEM-bit_start);
    if (remaining_bits > 0) {
      int next_int = data[byte_start+1];

      // Shift it left by the number of existing bits;
      next_int =  next_int << (alphabet.bitsPerLetter()-remaining_bits);

      val = val | next_int;
    }

    // Finally zero out all but the least significant bitsPerLetter.
    val = val & alphabet.letterMask();

    return val;
  }

  /**
   * Read a record containing the compressed sequence.
   */
  public void readCompressedSequence(CompressedSequence data) {
    readPackedBytes(data.getDna().array(), data.getLength());
  }

  /**
   * Read the data in from an array of bytes encoding the data in UTF8 with one byte
   * per character in the alphabet.  We read the data into an existing object
   * to avoid overhead of allocating a new object.
   *
   * @param utf_letters - The byte buffer to read
   * @param length      - How many bytes to read
   */
  public void readUTF8(byte[] utf_letters, int length) {
    this.length = length;
    int min_ints = (int) Math.ceil((double)(length * alphabet.bitsPerLetter()) / BITSPERITEM);

    if ((data == null) || (min_ints > data.length)) {
      data = new int[min_ints];
    }

    int[] letters =  alphabet.utf8ToInt(utf_letters, length);
    setAt(0, letters);
  }

  /**
   * Read the data in from an array of bytes encoding the data in UTF8 with one byte
   * per character in the alphabet.  We read the data into an existing object
   * to avoid overhead of allocating a new object.  We allocate a new array of ints
   * if the existing array is too small. We stop reading if we encounter a null character
   * (value of zero).
   */
  public void readUTF8(byte[] utf_letters) {
    readUTF8(utf_letters, utf_letters.length);
  }

  /**
   * Read in an array of packed bytes. The data is packed into the bytes such that
   * alphabet.bitsPerLetter() are used to encode each character.
   *
   * @param bytes - the buffer of bytes to read the data from.
   * @param length - the length of the sequence.
   */
  public void readPackedBytes(byte[] bytes, int length) {
    this.length = length;

   	// Allocate a larger array if necessary.
    if (bytes.length > data.length*4) {
      data = new int[(int)Math.ceil(bytes.length/4.0)];
    }
    ByteUtil.bytesToInt(bytes, data);
  }

  /**
   * Return a new subsequence.
   *
   * The returned subsequence is [start, end). Thus the length
   * of the sequences is end-start.
   */
  public Sequence subSequence(int start, int end) {
    int length = end - start;
    int num_ints = (int) Math.ceil(((double)(length * alphabet.bitsPerLetter())) / BITSPERITEM);

    int[] buffer = new int[num_ints];
    Arrays.fill(buffer, FILL_VALUE);
    int int_start = (int) Math.floor((double)(start*alphabet.bitsPerLetter()) / BITSPERITEM);

    int bit_start = start*alphabet.bitsPerLetter() - (int_start*BITSPERITEM);

    int bit_shift = bit_start;
    int index = 0;
    while (index < num_ints) {
      int data_index = int_start + index;

      int new_val = (data[data_index] >>> bit_shift);

      if ((data_index + 1 < data.length) && (bit_shift > 0)) {
        // Get the most significant bits of this integer using the
        // next int. We potentially copy characters we don't want
        // but we zero them out later.
        new_val = new_val |  (data[data_index + 1] << (BITSPERITEM - bit_shift));
      }

      buffer[index] = new_val;
      index++;
    }
    // Zero out any bits in the last int that shouldn't have been set.
    long num_unset_bits = buffer.length * BITSPERITEM - length * alphabet.bitsPerLetter();
    int mask = 0xffffffff;
    mask = mask >>> num_unset_bits;
    buffer[num_ints - 1] = buffer[num_ints - 1] & mask;
    return new Sequence(buffer, length, alphabet);
  }

  /**
   * @return A compressed sequence representing this sequence.
   */
  public CompressedSequence toCompressedSequence() {
    CompressedSequence output = new CompressedSequence();
    output.setDna(ByteBuffer.wrap(toPackedBytes(), 0, numPackedBytes()));
    output.setLength(size());
    return output;
  }

  /**
   * Return an array of packed bytes representing the data. The sequence is encoded using alphabet.bitsPerLetter()
   * for each character in the sequence. The size of the array is a multiple of 4.
   */
  public byte[] toPackedBytes() {
    byte[] bytes = ByteUtil.intsToBytes(data);
    return bytes;
  }

  /**
   * Return the minimum number of bytes in the array returned by packed bytes needed
   * to encode the data.
   */
  public int numPackedBytes() {
    int num_bytes =  (int)Math.ceil((alphabet.bitsPerLetter() *size())/ 8.0);
    return num_bytes;
  }

  /**
   * Return a pointer to the alphabet.
   */
  public Alphabet getAlphabet() {
    return this.alphabet;
  }


  /**
   * Compare to sequences.
   *
   * TODO(jlewi): How should we handle sequences with different alphabets.
   * TODO(jlewi): Can we speed this up? Unfortunately, we can't just
   *   convert the integers because we start filling in the integers
   *   starting with the least significant bit. Thus, if we have two sequences
   *   of equal length, the most significant bit in the int is
   *   actually the least significant bit with regards to the comparison.
   * @param seq
   * @return
   */
  public int compareTo(Sequence other) {
    Sequence seq = (other);
    if (!this.alphabet.equals(seq.alphabet)) {
      throw new RuntimeException(
          "Two sequences must use the same alphabet to be comparable");
    }
    // Determine which sequence is shorter.
    int min_length = this.size() < seq.size() ? this.size() : seq.size();

    for (int i = 0; i < min_length; i++) {
      if (this.valAt(i) < seq.valAt(i)) {
        return -1;
      } else if (this.valAt(i) > seq.valAt(i)){
        return 1;
      }
    }
    // Check if one sequence is a prefix of the other.
    if (this.size() < seq.size()) {
      return -1;
    } else if (this.size() > seq.size()) {
      return 1;
    }
    // sequences are the same.
    return 0;
  }

  /**
   * Check if two alphabets are equal
   */
  public boolean equals(Object other) {
    if (!(other instanceof Sequence)) {
      throw new RuntimeException(
          "Can only compare Sequences to other sequences");
    }
    return this.compareTo((Sequence)other) == 0 ? true : false;
  }
  /**
   * Construct a string representing the sequence.
   */
  public String toString() {
    StringBuilder builder = new StringBuilder(size());
    for (int i = 0; i < size(); i++) {
      builder.append(at(i));
    }

    return builder.toString();
  }

  /**
   * Grow the capacity of the sequence.
   *
   * If size is less than the current size, this function has no effect.
   * We return a reference to the buffer so if a new buffer is allocated
   * the caller can update any shared references.
   *
   * @param newsize - The new capacity (number of letters) that the sequence should
   *   be able to represent
   */
  @SuppressWarnings("unused")
  public int[] growCapacity(int newsize) {
    if (newsize < this.capacity()) {
      return this.data;
    }
    int oldlength = this.data.length;
    int new_numints=  numItemsForSize(newsize);
    this.data = Arrays.copyOf(this.data, new_numints);

    // Fill the newdata with the FILL_VALUE. copyOf should pad with zeros by default.
    if (FILL_VALUE != 0) {
      Arrays.fill(this.data, oldlength, new_numints, FILL_VALUE);
    }
    return this.data;
  }

  /**
   * Add the sequence to this sequence.
   *
   * We grow the buffer if needed to contain the new sequence if necessary.
   * We return a reference to the new buffer so the caller can update references.
   *
   * @param other - The sequence to add to this one
   * @return - Pointer to the buffer inside this Sequence which stores the sum.
   */
  public int[] add(Sequence other) {
    // Edge case: zero length sequences.
    if (other.size() == 0) {
      return this.data;
    }
    if (this.size() == 0) {
      this.data = Arrays.copyOf(other.data, other.data.length);
      this.length = other.length;
      return this.data;
    }

    // Make sure all unset bits are set to zero because the processing
    // depends on it.
    zeroOutUnsetBits();

    growCapacity(this.size() + other.size());

    final int this_old_size = this.size();
    final int this_new_size = this_old_size + other.size();
    // Fill in the last partially filled item with letters from the other sequence.
    int write_bit_num =  alphabet.bitsPerLetter() * this_old_size;

    // The write position in this.data.
    int write_pos = numItemsForSize(this.size()) - 1;

    // The offset in the next write where we should continue.
    int write_int_offset = write_bit_num - write_pos * BITSPERITEM;

    if (write_int_offset == BITSPERITEM) {
      write_int_offset = 0;
      write_pos +=1;
    }

    // The position in other.data to read.
    int read_pos = 0;

    // The number of ints to read; including partial ints.
    final int MAX_INTS_READ = numItemsForSize(other.size());

    // The offset in the read.
    int read_int_offset = 0;

    // Number of bits in the last int in other that need to be read.
    final long num_bits_last_int = (long)other.size() * alphabet.bitsPerLetter() -
                             (long)(MAX_INTS_READ-1) * BITSPERITEM;

    // If write_int_offset is 0 than we can dispense with bitshifts
    // and copy whole ints.
    if (write_int_offset != 0) {
      // Stopping conditions: 1. we reach the last int in other (MAX_INTS_READ)
      // or 2. read_int_offset is > number of set bits in the last int in other.
      while (read_pos < MAX_INTS_READ) {
        // If we're reading the last int; check if we've already
        // read all the bits in this int.
        if (read_pos + 1 == MAX_INTS_READ) {
          if (read_int_offset >= num_bits_last_int) {
            break;
          }
        }

        // Drop the read_int_offset bits from the read byte.
        int val = other.data[read_pos] >>> read_int_offset;
        // Now shift it left into the write_int_offset most significant bit
        val = val << write_int_offset;

        int newval = this.data[write_pos] | val;
        this.data[write_pos] = newval;

        // Update the locations.
        // How many bits were actually read written.
        int num_bits_read_write = Math.min(BITSPERITEM - write_int_offset, BITSPERITEM - read_int_offset);
        write_int_offset +=  num_bits_read_write;
        read_int_offset += num_bits_read_write;

        if (write_int_offset >= BITSPERITEM) {
          write_pos +=1;
          write_int_offset -= BITSPERITEM;
        }

        if (read_int_offset >= BITSPERITEM) {
          read_pos +=1;
          read_int_offset -= BITSPERITEM;
        }

        // Break out of the loop if we are now aligned with the boundaries.
        if ((write_int_offset == 0) && (read_int_offset == 0)) {
          break;
        }
      }
    }

    // The write and read bit offsets are now aligned with the item boundaries
    // so we don't need to any more bit shifting.
    // Check if there is more data to read.
    int num_letters_read = (read_pos*BITSPERITEM + read_int_offset) / alphabet.bitsPerLetter();
    if (num_letters_read < other.size()) {
      // Make a copy of all complete items.
      while (read_pos < MAX_INTS_READ) {
        this.data[write_pos] = other.data[read_pos];
        write_pos++;
        read_pos++;
      }
    }

    // Set the new length.
    this.length = this_new_size;
    return this.data;
  }
  /**
   * Return the number of items needed for sequences of the given length.
   *
   * TODO (jlewi): It may be faster to inline this function call in many places
   * where the code is repeatedly called
   *
   * @param num_letters - Number of letters we want to encode.
   * @return - The minimum number of items to encode sequences of the given length.
   */
  public int numItemsForSize(int num_letters) {
    return (int)Math.ceil((double)(alphabet.bitsPerLetter() * num_letters) /
                           BITSPERITEM);
  }

  /**
   * Sets the size for the current sequence.
   * @param length
   */
  public void setSize(int length) {
    if (length > capacity()) {
      throw new RuntimeException("New length exceeds current capacity. Grow capacity first");
    }
    this.length = length;
  }

  /**
   * Function forces all unset bits to zero. This is needed for some operations
   * like add which depend on unset bits being 0.
   */
  private void zeroOutUnsetBits() {
    // Get the last partially filled entry.
    int num_items = numItemsForSize(length);

    // How many bits in the last int need to be zeroed out.
    int shift = num_items * BITSPERITEM - length * alphabet.bitsPerLetter();
    if (shift > 0) {
      int mask = 0xFFFFFFFF;
      mask = mask >>> shift;
      //mask = mask >> 1;
      data[num_items -1] = data[num_items -1] & mask;
    }
    // Fill in the remaining items
    Arrays.fill(data, num_items, data.length, 0);
  }
}
