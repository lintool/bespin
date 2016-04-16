package io.bespin.scala.util

import org.apache.hadoop.io._
import org.scalatest.{FunSpec, Matchers}

class WritableConversionsTest extends FunSpec with Matchers {

  val wc = new WritableConversions {}

  describe("ObjectWritable conversions") {
    import wc._
    case class A(v: String)
    case class B(v: Int)

    it("should correctly box ObjectWritable") {
      val obj = A("test")
      val boxed: ObjectWritable = obj
      boxed.get() shouldBe obj
    }
    it("should correctly unbox ObjectWritable") {
      val boxed = new ObjectWritable(classOf[A], A("test"))
      val unboxed = wc.ObjectWritableUnbox[A](boxed)
      unboxed shouldBe A("test")
    }
    it("should throw exception if unboxing to incompatible type") {
      val boxed = new ObjectWritable(classOf[A], A("test"))
      intercept [AssertionError] {
        wc.ObjectWritableUnbox[B](boxed)
      }
    }
  }

  describe("BooleanWritable conversions") {
    import wc._
    it("should correctly box BooleanWritable") {
      val boxed: BooleanWritable = true
      boxed.get() shouldBe true
    }

    it("should correctly unbox BooleanWritable") {
      val boxed = new BooleanWritable(true)
      val unboxed: Boolean = boxed
      unboxed shouldBe true
    }
  }

  describe("ShortWritable conversions") {
    import wc._
    it("should correctly box ShortWritable") {
      val boxed: ShortWritable = 5.toShort
      boxed.get() shouldBe 5
    }

    it("should correctly unbox ShortWritable") {
      val boxed = new ShortWritable(5)
      val unboxed: Short = boxed
      unboxed shouldBe 5
    }
  }

  describe("IntWritable conversions") {
    import wc._
    it("should correctly box IntWritable") {
      val boxed: IntWritable = 5
      boxed.get() shouldBe 5
    }

    it("should correctly unbox IntWritable") {
      val boxed = new IntWritable(5)
      val unboxed: Int = boxed
      unboxed shouldBe 5
    }
  }

  describe("LongWritable conversions") {
    import wc._
    it("should correctly box LongWritable") {
      val boxed: LongWritable = Long.MaxValue
      boxed.get() shouldBe Long.MaxValue
    }

    it("should correctly unbox LongWritable") {
      val boxed = new LongWritable(Long.MaxValue)
      val unboxed: Long = boxed
      unboxed shouldBe Long.MaxValue
    }
  }

  describe("FloatWritable conversions") {
    import wc._
    it("should correctly box FloatWritable") {
      val boxed: FloatWritable = 5.5f
      boxed.get() shouldBe 5.5d
    }

    it("should correctly unbox FloatWritable") {
      val boxed = new FloatWritable(5.5f)
      val unboxed: Float = boxed
      unboxed shouldBe 5.5d
    }
  }

  describe("DoubleWritable conversions") {
    import wc._
    it("should correctly box DoubleWritable") {
      val boxed: DoubleWritable = 5.5d
      boxed.get() shouldBe 5.5d
    }

    it("should correctly unbox DoubleWritable") {
      val boxed = new DoubleWritable(5.5d)
      val unboxed: Double = boxed
      unboxed shouldBe 5.5d
    }
  }

  describe("Text conversions") {
    import wc._
    it("should correctly box Text") {
      val boxed: Text = "testString"
      boxed.toString shouldBe "testString"
    }

    it("should correctly unbox Text") {
      val boxed = new Text("testString")
      val unboxed: String = boxed
      unboxed shouldBe "testString"
    }
  }

}
