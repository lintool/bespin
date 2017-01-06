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

package io.bespin.scala.util

import org.scalatest.junit.AssertionsForJUnit
import scala.collection.JavaConversions._

import org.junit.Assert._
import org.junit.Test

class TokenizerTest extends AssertionsForJUnit {
  class MockTokenizer extends Tokenizer {}

  @Test def testScalaTokenization {
    val tokenizer = new MockTokenizer()

    for (i <- 0 until io.bespin.java.util.TokenizerTest.EXAMPLES.length ) {
      assertEquals(io.bespin.java.util.TokenizerTest.EXPECTED.get(i).toList,
        tokenizer.tokenize(io.bespin.java.util.TokenizerTest.EXAMPLES(i)))
    }
  }
}

