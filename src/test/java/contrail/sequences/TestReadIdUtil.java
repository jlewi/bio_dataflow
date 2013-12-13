/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.sequences;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestReadIdUtil {
  @Test
  public void testIsValidMateId() {
    assertTrue(ReadIdUtil.isValidMateId("3/1"));
    assertTrue(ReadIdUtil.isValidMateId("2/1"));
    assertTrue(ReadIdUtil.isValidMateId(
        "gi|30260195|ref|NC_003997.3|_500_735_3:1:0_0:0:0_4/1"));
    
    assertFalse(ReadIdUtil.isValidMateId("someid/11"));
    assertFalse(ReadIdUtil.isValidMateId("2_"));
    assertFalse(ReadIdUtil.isValidMateId("2/"));
    assertFalse(ReadIdUtil.isValidMateId("/2"));
    assertFalse(ReadIdUtil.isValidMateId("_1"));
    assertFalse(ReadIdUtil.isValidMateId(
        "gi|30260195|ref|NC_003997.3|_500_735_3:1:0_0:0:0_4"));
  }
  
  @Test
  public void testisMatePair() {
    assertTrue(ReadIdUtil.isMatePair("2/1", "2/2"));
    assertTrue(ReadIdUtil.isMatePair("2_1", "2_2"));
    assertTrue(ReadIdUtil.isMatePair("someid/1", "someid/2"));
    
    assertFalse(ReadIdUtil.isMatePair("2/3", "2/2"));
    assertFalse(ReadIdUtil.isMatePair("somei/1", "someid/2"));
    assertFalse(ReadIdUtil.isMatePair("someid/1", "someid/1"));
  }
}
