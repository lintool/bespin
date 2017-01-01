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

import org.apache.hadoop.io._

trait WritableConversions {
  implicit def BooleanWritableUnbox(v: BooleanWritable) = v.get
  implicit def BooleanWritableBox  (v: Boolean) = new BooleanWritable(v)

  implicit def DoubleWritableUnbox(v: DoubleWritable) = v.get
  implicit def DoubleWritableBox  (v: Double) = new DoubleWritable(v)

  implicit def FloatWritableUnbox(v: FloatWritable) = v.get
  implicit def FloatWritableBox  (v: Float) = new FloatWritable(v)

  implicit def IntWritableUnbox(v: IntWritable) = v.get
  implicit def IntWritableBox  (v: Int) = new IntWritable(v)

  implicit def LongWritableUnbox(v: LongWritable) = v.get
  implicit def LongWritableBox  (v: Long) = new LongWritable(v)

  implicit def TextUnbox(v: Text) = v.toString
  implicit def TextBox  (v: String) = new Text(v)
}
