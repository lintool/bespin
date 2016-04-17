package io.bespin.scala.mapreduce.util

import org.scalatest.{Matchers, FunSpec}

import scala.reflect.runtime.universe.typeOf

class TypedReducerTest extends FunSpec with Matchers {

  trait TypeA
  trait TypeB
  trait TypeC extends TypeA with TypeB
  
  describe("A Typed Reducer") {
    describe("upon creation") {
      it("should capture the correct runtime output types") {
        val m = new TypedReducer[TypeA, TypeB, TypeC, TypeB] {}
        assert(m.kEv.tpe =:= typeOf[TypeC])
        assert(m.kEv.tpe <:< typeOf[TypeA])
        assert(m.kEv.tpe <:< typeOf[TypeB])
        assert(m.vEv.tpe =:= typeOf[TypeB])
        assert(!(m.vEv.tpe <:< typeOf[TypeA]))
      }
    }
  }
}
