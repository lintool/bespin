/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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

package io.bespin.java.util;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TokenizerTest {
  // Public so we can share test cases across Java and Scala.
  public static final String[] EXAMPLES = {
      "It is Perfect! But there's something missing.",
      "There are perfect 2 pigs here, before the big bad wolf came."
  };

  // Public so we can share test cases across Java and Scala.
  public static final List<List<String>> EXPECTED = Lists.newArrayList(
      (List<String>) Lists.newArrayList("it", "is", "perfect", "but", "there's", "something", "missing"),
      (List<String>) Lists.newArrayList("there", "are", "perfect", "pigs", "here", "before", "the", "big", "bad",
          "wolf", "came"));
  // There has got to be a more concise way to do this...

  @Test
  public void testJavaTokenization() throws Exception {
    for (int i = 0; i < EXAMPLES.length; i++) {
      assertEquals(EXPECTED.get(i), Tokenizer.tokenize(EXAMPLES[i]));
    }
  }
}
