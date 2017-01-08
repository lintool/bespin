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

import java.util.StringTokenizer

import scala.collection.JavaConverters._

trait Tokenizer {
  def tokenize(s: String): List[String] = {
    val pattern = """(^[^a-z]+|[^a-z]+$)""".r

    new StringTokenizer(s).asScala.toList
      .map(t => pattern.replaceAllIn(t.asInstanceOf[String].toLowerCase(), ""))
      .filter(_.length != 0)
  }
}