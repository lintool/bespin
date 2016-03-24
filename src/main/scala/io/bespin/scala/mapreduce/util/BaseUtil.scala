package io.bespin.scala.mapreduce.util

import scala.reflect.runtime.universe.{TypeTag => TT}

/**
  * A decorator trait providing an implicit mirror belonging to the implementing class' classloader.
  */
trait WithMirror { self =>
  protected[util] implicit val typeMirror: reflect.runtime.universe.Mirror =
    reflect.runtime.universe.runtimeMirror(self.getClass.getClassLoader)
}

/**
  * A decorator trait providing an implicit Class[_] object of the implementing class. Useful for
  * calling Job.setJarByClass (which Hadoop uses to look up the jar of the class creating/running the job)
  */
trait WithCallingClass { self =>
  implicit protected[util] val thisClass: Class[_] = self.getClass
}

/**
  * An interface which provides the runtime types of its key and value classes
  */
trait WithTypedOutput[KO, VO] {

  protected[util] val kEv: TT[KO]

  protected[util] val vEv: TT[VO]

  protected[util] def outputKeyType(implicit mirror: reflect.runtime.universe.Mirror): mirror.universe.RuntimeClass =
    mirror.runtimeClass(kEv.tpe.typeSymbol.asClass)

  protected[util] def outputValueType(implicit mirror: reflect.runtime.universe.Mirror): mirror.universe.RuntimeClass =
    mirror.runtimeClass(vEv.tpe.typeSymbol.asClass)

}

/**
  * A decorator trait providing a typed class symbol
  */
trait WrappedWithClazz[T] {
  protected[util] val clazz: Class[T]
}