package io.bespin.scala.util

import org.apache.hadoop.io._
import tl.lin.data.pair._

import scala.language.implicitConversions

/**
  * Provides a set of implicit conversions between the Hadoop Writable types and base Scala types
  */
trait WritableConversions {
  implicit def BooleanWritableUnbox(v: BooleanWritable): Boolean = v.get
  implicit def BooleanWritableBox  (v: Boolean): BooleanWritable = new BooleanWritable(v)

  implicit def ShortWritableUnbox(v: ShortWritable): Short = v.get
  implicit def ShortWritableBox  (v: Short): ShortWritable = new ShortWritable(v)

  implicit def IntWritableUnbox(v: IntWritable): Int = v.get
  implicit def IntWritableBox  (v: Int): IntWritable = new IntWritable(v)

  implicit def LongWritableUnbox(v: LongWritable): Long = v.get
  implicit def LongWritableBox  (v: Long): LongWritable = new LongWritable(v)

  implicit def FloatWritableUnbox(v: FloatWritable): Float = v.get
  implicit def FloatWritableBox  (v: Float): FloatWritable = new FloatWritable(v)

  implicit def DoubleWritableUnbox(v: DoubleWritable): Double = v.get
  implicit def DoubleWritableBox  (v: Double): DoubleWritable = new DoubleWritable(v)

  implicit def TextUnbox(v: Text): String = v.toString
  implicit def TextBox  (v: String): Text = new Text(v)
}

trait PairWritableConversions {

  implicit def PairOfWritablesUnbox[L <: Writable, R <: Writable](v: PairOfWritables[L, R]): (L, R) =
    (v.getLeftElement, v.getRightElement)
  implicit def PairOfWritablesBox[L <: Writable, R <: Writable](v: (L, R)): PairOfWritables[L, R] =
    new PairOfWritables[L,R](v._1, v._2)

  implicit def PairOfObjectsUnbox[L <: Comparable[L], R<: Comparable[R]](v: PairOfObjects[L, R]): (L, R) =
    (v.getLeftElement, v.getRightElement)
  implicit def PairOfObjectsBox[L <: Comparable[L], R<: Comparable[R]](v: (L, R)): PairOfObjects[L, R] =
    new PairOfObjects(v._1, v._2)

  implicit def PairOfObjectShortUnbox[L <: Comparable[L]](v: PairOfObjectShort[L]): (L, Short) =
    (v.getLeftElement, v.getRightElement)
  implicit def PairOfObjectShortBox[L <: Comparable[L]](v: (L, Short)): PairOfObjectShort[L] =
    new PairOfObjectShort(v._1, v._2)

  implicit def PairOfObjectIntUnbox[L <: Comparable[L]](v: PairOfObjectInt[L]): (L, Int) =
    (v.getLeftElement, v.getRightElement)
  implicit def PairOfObjectIntBox[L <: Comparable[L]](v: (L, Int)): PairOfObjectInt[L] =
    new PairOfObjectInt(v._1, v._2)

  implicit def PairOfObjectLongUnbox[L <: Comparable[L]](v: PairOfObjectLong[L]): (L, Long) =
    (v.getLeftElement, v.getRightElement)
  implicit def PairOfObjectLongBox[L <: Comparable[L]](v: PairOfObjectLong[L]): PairOfObjectLong[L] =
    new PairOfObjectLong(v._1, v._2)

  implicit def PairOfObjectFloatUnbox[L <: Comparable[L]](v: PairOfObjectFloat[L]): (L, Float) =
    (v.getLeftElement, v.getRightElement)
  implicit def PairOfObjectFloatBox[L <: Comparable[L]](v: (L, Float)): PairOfObjectFloat[L] =
    new PairOfObjectFloat(v._1, v._2)

  implicit def PairOfObjectDoubleUnbox[L <: Comparable[L]](v: PairOfObjectDouble[L]): (L, Double) =
    (v.getLeftElement, v.getRightElement)
  implicit def PairOfObjectDoubleBox[L <: Comparable[L]](v: (L, Double)): PairOfObjectDouble[L] =
    new PairOfObjectDouble(v._1, v._2)

  implicit def PairOfStringsUnbox(v: PairOfStrings): (String, String) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfStringsBox(v: (String, String)): PairOfStrings = new PairOfStrings(v._1, v._2)

  implicit def PairOfStringIntUnbox(v: PairOfStringInt): (String, Int) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfStringIntBox(v: (String, Int)): PairOfStringInt = new PairOfStringInt(v._1, v._2)

  implicit def PairOfStringLongUnbox(v: PairOfStringLong): (String, Long) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfStringLongBox(v: (String, Long)): PairOfStringLong = new PairOfStringLong(v._1, v._2)

  implicit def PairOfStringFloatUnbox(v: PairOfStringFloat): (String, Float) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfStringFloatBox(v: (String, Float)): PairOfStringFloat = new PairOfStringFloat(v._1, v._2)

  implicit def PairOfIntsUnbox(v: PairOfInts): (Int, Int) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfIntsBox(v: (Int, Int)): PairOfInts = new PairOfInts(v._1, v._2)

  implicit def PairOfIntLongUnbox(v: PairOfIntLong): (Int, Long) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfIntLongBox(v: (Int, Long)): PairOfIntLong = new PairOfIntLong(v._1, v._2)

  implicit def PairOfIntFloatUnbox(v: PairOfIntFloat): (Int, Float) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfIntFloatBox(v: (Int, Float)): PairOfIntFloat = new PairOfIntFloat(v._1, v._2)

  implicit def PairOfIntStringUnbox(v: PairOfIntString): (Int, String) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfIntStringBox(v: (Int, String)): PairOfIntString = new PairOfIntString(v._1, v._2)

  implicit def PairOfLongsUnbox(v: PairOfLongs): (Long, Long) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfLongsBox(v: (Long, Long)): PairOfLongs = new PairOfLongs(v._1, v._2)

  implicit def PairOfLongIntUnbox(v: PairOfLongInt): (Long, Int) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfLongIntBox(v: (Long, Int)): PairOfLongInt = new PairOfLongInt(v._1, v._2)

  implicit def PairOfLongFloatUnbox(v: PairOfLongFloat): (Long, Float) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfLongFloatBox(v: (Long, Float)): PairOfLongFloat = new PairOfLongFloat(v._1, v._2)

  implicit def PairOfLongStringUnbox(v: PairOfLongString): (Long, String) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfLongStringBox(v: (Long, String)): PairOfLongString = new PairOfLongString(v._1, v._2)

  implicit def PairOfFloatsUnbox(v: PairOfFloats): (Float, Float) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfFloatsBox(v: (Float, Float)): PairOfFloats = new PairOfFloats(v._1, v._2)

  implicit def PairOfFloatIntUnbox(v: PairOfFloatInt): (Float, Int) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfFloatIntBox(v: (Float, Int)): PairOfFloatInt = new PairOfFloatInt(v._1, v._2)

  implicit def PairOfFloatStringUnbox(v: PairOfFloatString): (Float, String) = (v.getLeftElement, v.getRightElement)
  implicit def PairOfFloatStringBox(v: (Float, String)): PairOfFloatString = new PairOfFloatString(v._1, v._2)

}