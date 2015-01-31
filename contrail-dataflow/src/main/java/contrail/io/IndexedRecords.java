/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.io;

/**
 * A generic interface for classes that support random access.
 *
 * This is basically a subset of the Map interface which supports read only
 * access. So a Map could be used anywhere IndexedRecords are used.
 * @param <K>
 * @param <V>
 */
public interface IndexedRecords<K, V> {
  V get(K k);
}
