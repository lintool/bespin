package io.bespin.scala.util;

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
