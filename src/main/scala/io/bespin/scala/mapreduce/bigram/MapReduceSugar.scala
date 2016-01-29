package io.bespin.scala.mapreduce.bigram

import io.bespin.scala.util.WritableConversions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

import scala.language.{higherKinds, implicitConversions}
import scala.reflect.runtime.universe.{TypeTag => TT, typeOf}

/**
  * A decorator trait providing an implicit mirror belonging to the implementing class' classloader.
  */
trait WithMirror { self =>
  implicit protected val typeMirror: reflect.runtime.universe.Mirror =
    reflect.runtime.universe.runtimeMirror(self.getClass.getClassLoader)
}

/**
  * A decorator trait providing an implicit Class[_] object of the implementing class. Useful for
  * calling Job.setJarByClass (which Hadoop uses to look up the jar of the class creating/running the job)
  */
trait WithCallingClass { self =>
  implicit protected val thisClass: Class[_] = self.getClass
}

/**
  * A decorator trait providing a typed class symbol
  */
trait WrappedWithClazz[T] {
  def clazz: Class[T]
}

/**
  * A wrapped instance of a MapReduce Mapper object which encapsulates the type arguments of the Mapper
  * with TypeTags and provides a relatively type-safe way to get the class of the Mapper
  * @tparam KI Key Input type
  * @tparam VI Value Input type
  * @tparam KO Key Output type
  * @tparam VO Value Output type
  */
case class WrappedMapper[KI:TT,VI:TT,KO:TT,VO:TT](m: Mapper[KI,VI,KO,VO]) extends WrappedWithClazz[Mapper[KI,VI,KO,VO]] { self =>
  override def clazz: Class[Mapper[KI, VI, KO, VO]] = m.getClass.asInstanceOf[Class[Mapper[KI,VI,KO,VO]]]
}

/**
  * A wrapped instance of a MapReduce Reducer object which encapsulates the type arguments of the Reducer
  * with TypeTags and provides a relatively type-safe way to get the class of the Reducer
  * @tparam KI Key Input type
  * @tparam VI Value Input type
  * @tparam KO Key Output type
  * @tparam VO Value Output type
  */
case class WrappedReducer[KI:TT,VI:TT,KO:TT,VO:TT](r: Reducer[KI,VI,KO,VO]) extends WrappedWithClazz[Reducer[KI,VI,KO,VO]] {
  override def clazz: Class[Reducer[KI, VI, KO, VO]] = r.getClass.asInstanceOf[Class[Reducer[KI,VI,KO,VO]]]
}

/**
  * A wrapped instance of a MapReduce Partitioner object which encapsulates the type arguments of the Partitioner
  * with TypeTags and provides a relatively type-safe way to get the class of the Partitioner
  * @tparam KI Key Input type
  * @tparam VI Value Input type
  */
case class WrappedPartitioner[KI:TT,VI:TT](p: Partitioner[KI,VI]) extends WrappedWithClazz[Partitioner[KI,VI]] {
  override def clazz: Class[Partitioner[KI, VI]] = p.getClass.asInstanceOf[Class[Partitioner[KI,VI]]]
}

/**
  * An implementation of HasJob returns a Job object with possible configuration applied
  * to it. It may create this Job object itself, or have it passed in from another source.
  */
sealed trait HasJob {
  def job(implicit mirror: reflect.runtime.universe.Mirror): Job
}

/**
  * BaseJob is the root class for creating a MapReduce job using the syntactic sugar provided in this
  * and related classes.
  * @param name Name of the Hadoop job.
  * @param initialConfig Optional initial Configuration (can be None)
  * @param callingClass Class symbol of the calling class (needed by Hadoop to determine the source jar)
  */
case class BaseJob(name: String, initialConfig: Option[Configuration] = None, callingClass: Class[_]) extends HasJob {
  def job(implicit mirror: reflect.runtime.universe.Mirror): Job = {
    val j = initialConfig.fold(Job.getInstance)(Job.getInstance)
    j.setJobName(name)
    j.setJarByClass(callingClass)
    j
  }
}

/**
  * An InputDefinition describes the input format of a particular Job
  * @tparam KI Key Input type
  * @tparam VI Value Input type
  */
sealed trait InputDefinition[KI,VI] extends HasJob

/**
  * An OutputDefinition describes the output format of a particular Job.
  * Once an OutputDefinition (and all its prerequisites) have been fully defined, the Job can be
  * run as a MapReduce job.
  * @tparam KO Key Output type
  * @tparam VO Value Output type
  */
sealed trait OutputDefinition[KO,VO] extends HasJob {
  /**
    * Runs the constructed MapReduce Job
    * @param verbose Enables verbose logging output
    * @param mirror Implicit type mirror used for resolving runtime types of arguments
    */
  def run(verbose: Boolean = true)(implicit mirror: reflect.runtime.universe.Mirror): Boolean = {
    job.waitForCompletion(verbose)
  }
}

case class FileInputDefinition[KI:TT,VI:TT](baseJob: BaseJob,
                                            path: Path,
                                            inputFormat: Class[_<:FileInputFormat[KI,VI]]) extends InputDefinition[KI,VI] {
  def job(implicit mirror: reflect.runtime.universe.Mirror) = {
    val jobIn = baseJob.job
    FileInputFormat.addInputPath(jobIn, path)
    jobIn.setInputFormatClass(inputFormat)
    jobIn
  }
}

case class FileOutputDefinition[KO:TT,VO:TT](reduceStage: ReduceStage[_,_,KO,VO],
                                             path: Path,
                                             outputFormat: Class[_<:FileOutputFormat[KO,VO]] = classOf[TextOutputFormat[KO,VO]],
                                             deleteExisting: Boolean = true) extends OutputDefinition[KO,VO] {
  def job(implicit mirror: reflect.runtime.universe.Mirror) = {
    val jobIn = reduceStage.job
    FileOutputFormat.setOutputPath(jobIn, path)
    jobIn.setOutputFormatClass(outputFormat)
    if(deleteExisting)
      FileSystem.get(jobIn.getConfiguration).delete(path, true)

    jobIn
  }

  def withFormat[F<:FileOutputFormat[KO,VO]:TT](implicit mirror: reflect.runtime.universe.Mirror) =
    this.copy[KO,VO](outputFormat = mirror.runtimeClass(typeOf[F].typeSymbol.asClass).asInstanceOf[Class[FileOutputFormat[KO,VO]]])

}

/**
  * A class representing the "map" stage of a MapReduce job.
  * Also contains optional Partitioner and Combiner objects.
  * @param inputDefinition Reference to the "input" stage of the job, which must be defined before the mapper
  * @param map Wrapped mapper object
  * @param combine Optional wrapped combiner(reducer) object
  * @param partitioner Optional wrapped partitioner object
  * @tparam KI Key Input type
  * @tparam VI Value Input type
  * @tparam KO Key Output type
  * @tparam VO Value Output type
  */
case class MapStage[KI:TT,VI:TT,KO:TT,VO:TT](inputDefinition: InputDefinition[KI,VI],
                                             map: WrappedMapper[KI,VI,KO,VO],
                                             combine: Option[WrappedReducer[KO,VO,KO,VO]] = None,
                                             partitioner: Option[WrappedPartitioner[KO,VO]] = None)
  extends HasJob {

  /**
    * Creates a copy of this stage with the combiner set
    * @param c Wrapped combiner (reducer) object
    */
  def combine(c: WrappedReducer[KO,VO,KO,VO]): MapStage[KI,VI,KO,VO] =
    this.copy(combine = Option(c))

  /**
    * Creates a copy of this stage with the partitioning behaviour set
    * @param p Wrapped partitioner object
    */
  def partitionBy(p: WrappedPartitioner[KO,VO]): MapStage[KI,VI,KO,VO] =
    this.copy(partitioner = Option(p))

  /**
    * Creates a "reduce" stage with this map stage as the dependency
    * @param reducer Wrapped reducer object
    * @param numReduceTasks Number of reduce tasks (default = 1)
    * @tparam RKO Reducer Key Output
    * @tparam RVO Reducer Value Output
    */
  def reduce[RKO:TT,RVO:TT](reducer: WrappedReducer[KO,VO,RKO,RVO], numReduceTasks: Int = 1): ReduceStage[KO,VO,RKO,RVO] =
    ReduceStage[KO,VO,RKO,RVO](this, reduce = reducer, numReduceTasks)

  override def job(implicit mirror: reflect.runtime.universe.Mirror): Job = {
    val jobIn = inputDefinition.job
    jobIn.setMapOutputKeyClass(mirror.runtimeClass(typeOf[KO].typeSymbol.asClass))
    jobIn.setMapOutputValueClass(mirror.runtimeClass(typeOf[VO].typeSymbol.asClass))
    jobIn.setMapperClass(map.clazz)
    combine.foreach(c => jobIn.setCombinerClass(c.clazz))
    partitioner.foreach(p => jobIn.setPartitionerClass(p.clazz))
    jobIn
  }
}

/**
  * A class representing the "reduce" stage of a MapReduce job
  * @param mapStage Reference to the "map" stage of the job, which must be defined before the reduce
  * @param reduce Wrapped reducer object
  * @param numReduceTasks Number of reducers (default 1)
  * @tparam KI Key Input type
  * @tparam VI Value Input type
  * @tparam KO Key Output type
  * @tparam VO Value Output type
  */
case class ReduceStage[KI:TT,VI:TT,KO:TT,VO:TT](mapStage: MapStage[_,_,KI,VI],
                                                reduce: WrappedReducer[KI,VI,KO,VO],
                                                numReduceTasks: Int = 1)
  extends HasJob {

  /**
    * Specify the combiner behaviour (this creates a new copy of the "map" dependency)
    * @param c Wrapped reducer object to use as combiner
    */
  def combine(c: WrappedReducer[KI,VI,KI,VI]): ReduceStage[KI,VI,KO,VO] =
    this.copy(mapStage = mapStage.combine(c))

  /**
    * Specify the partitioning behaviour (this creates a new copy of the "map" dependency)
    * @param p Wrapped partitioner object to use as partitioner
    */
  def partitionBy(p: WrappedPartitioner[KI,VI]): ReduceStage[KI,VI,KO,VO] =
    this.copy(mapStage = mapStage.partitionBy(p))

  override def job(implicit mirror: reflect.runtime.universe.Mirror): Job = {
    val jobIn = mapStage.job
    jobIn.setOutputKeyClass(mirror.runtimeClass(typeOf[KO].typeSymbol.asClass))
    jobIn.setOutputValueClass(mirror.runtimeClass(typeOf[VO].typeSymbol.asClass))
    jobIn.setReducerClass(reduce.clazz)
    jobIn.setNumReduceTasks(numReduceTasks)
    jobIn
  }

}

/**
  * BaseSyntax provides implicit conversions and operations which are required across a range of applications
  */
trait BaseSyntax {

  implicit def optionMapper[T,R](opt: Option[T])(implicit conv: T => R): Option[R] = opt.map(conv)

}

/**
  * WrappingSyntax provides implicit conversions and operations for objects wrapping Hadoop/MapReduce classes
  */
trait WrappingSyntax extends BaseSyntax { self: BaseSyntax =>

  implicit def mapperToWrapper[KI:TT,VI:TT,KO:TT,VO:TT](m: Mapper[KI,VI,KO,VO]): WrappedMapper[KI,VI,KO,VO] = WrappedMapper(m)

  implicit def reducerToWrapper[KI:TT,VI:TT,KO:TT,VO:TT](r: Reducer[KI,VI,KO,VO]): WrappedReducer[KI,VI,KO,VO] = WrappedReducer(r)

  implicit def partitionerToWrapper[KI:TT,VI:TT](p: Partitioner[KI,VI]): WrappedPartitioner[KI,VI] = WrappedPartitioner(p)

}

/**
  * StageSyntax provides various operations for manipulating various stages of a MapReduce Job in order to
  * define a job in a type-safe and functional way.
  */
trait StageSyntax extends WrappingSyntax {

  /**
    * Creates a BaseJob instance which can be chained with other operations to configure a MapReduce Job.
    * @param name Name of the MapReduce job
    * @param caller Class of the invoking class. This can be provided implicitly, such as in the case where
    *               the enclosing class extends [[WithCallingClass]]
    */
  def job(name: String)(implicit caller: Class[_]): BaseJob = BaseJob(name, callingClass = caller)

  /**
    * Creates a BaseJob instance which can be chained with other operations to configure a MapReduce Job.
    * @param name Name of the MapReduce job
    * @param initialConfig Configuration object to supply to the constructor of the contained MapReduce Job
    * @param caller Class of the invoking class. This can be provided implicitly, such as in the case where
    *               the enclosing class extends [[WithCallingClass]]
    */
  def job(name: String, initialConfig: Configuration)(implicit caller: Class[_]): BaseJob = BaseJob(name, Some(initialConfig), caller)

  implicit class BaseJobSyntax(job: BaseJob) {
    /**
      * file is a general-purpose means of reading an input file, but requires the specification of a
      * FileInputFormat of the correct type in order to be applied. The type of the file determines what
      * the type constraints on the input types of the Mapper will be.
      * @param path Path of the file to read in
      * @param inputFormat Hadoop FileInputFormat[KI,VI] which determines how to parse the file
      * @tparam KI Key Input type of the file - Will determine the input key type of the Mapper stage
      * @tparam VI Value Input type of the file - Will determine the input value type of the Mapper stage
      */
    def file[KI:TT,VI:TT](path: Path,
                          inputFormat: Class[FileInputFormat[KI,VI]]): FileInputDefinition[KI,VI] = {
      FileInputDefinition(job, path, inputFormat)
    }

    /**
      * textFile creates a FileInputDefinition specifically using the TextInputFormat. The resulting
      * key type is LongWritable, and the value type is Text.
      * @param path Path of the file to read in
      */
    def textFile(path: Path): FileInputDefinition[LongWritable, Text] = {
      FileInputDefinition(job, path, classOf[TextInputFormat])
    }
  }

  implicit class InputStageSyntax[KI:TT,VI:TT](inputStage: InputDefinition[KI,VI]) {
    /**
      * Create a MapStage from an input stage
      * @param map Wrapped Mapper object to set as the Mapper for this job
      * @tparam KO Key Output type of the mapper
      * @tparam VO Value Output type of the mapper
      */
    def map[KO:TT,VO:TT](map: WrappedMapper[KI,VI,KO,VO]) = MapStage(inputStage, map)
  }

  implicit class ReduceStageSyntax[KO:TT,VO:TT](reduceStage: ReduceStage[_,_,KO,VO]) {
    /**
      * Bind the output path of the job to a file, and optionally delete the file(s) currently present
      * at that location
      * @param path Path of output directory
      * @param deleteExisting If true, deletes the contents of the output directory before running the job
      */
    def setOutputAsFile(path: Path, deleteExisting: Boolean = true): FileOutputDefinition[KO,VO] = {
      FileOutputDefinition[KO,VO](reduceStage, path, deleteExisting = deleteExisting)
    }
  }

}

trait MapReduceSugar
  extends StageSyntax
    with WithCallingClass
    with WithMirror
    with WritableConversions

