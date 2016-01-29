package io.bespin.scala.util

import org.apache.hadoop.io._
import tl.lin.data.pair.PairOfStrings

import scala.language.implicitConversions

/**
  * Provides a set of implicit conversions between the Hadoop Writable types and base Scala types
  */
trait WritableConversions {
  implicit def BooleanWritableUnbox(v: BooleanWritable): Boolean = v.get
  implicit def BooleanWritableBox  (v: Boolean): BooleanWritable = new BooleanWritable(v)

  implicit def DoubleWritableUnbox(v: DoubleWritable): Double = v.get
  implicit def DoubleWritableBox  (v: Double): DoubleWritable = new DoubleWritable(v)

  implicit def FloatWritableUnbox(v: FloatWritable): Float = v.get
  implicit def FloatWritableBox  (v: Float): FloatWritable = new FloatWritable(v)

  implicit def IntWritableUnbox(v: IntWritable): Int = v.get
  implicit def IntWritableBox  (v: Int): IntWritable = new IntWritable(v)

  implicit def LongWritableUnbox(v: LongWritable): Long = v.get
  implicit def LongWritableBox  (v: Long): LongWritable = new LongWritable(v)

  implicit def TextUnbox(v: Text): String = v.toString
  implicit def TextBox  (v: String): Text = new Text(v)

  implicit def PairOfStringsUnbox(v: PairOfStrings): (String, String) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfStringsBox(v: (String, String)): PairOfStrings = new PairOfStrings(v._1, v._2)
}
