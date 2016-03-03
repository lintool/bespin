package io.bespin.scala.mapreduce.util

import io.bespin.scala.util.WritableConversions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text, WritableComparable}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, MapFileOutputFormat, SequenceFileOutputFormat, TextOutputFormat}

import scala.collection.JavaConverters._
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.runtime.universe.{TypeTag => TT, typeOf, typeTag}

/**
  * TypedMapper provides a scala wrapper to the Mapper class which reduces boilerplate and captures the run-time
  * output key and value types.
  *
  * @tparam KI Key Input type
  * @tparam VI Value Input type
  * @tparam KO Key Output type
  * @tparam VO Value Output type
  */
abstract class TypedMapper[KI,VI,KO:TT,VO:TT] extends Mapper[KI,VI,KO,VO] with WithTypedOutput[KO,VO] {

  protected[util] final val kEv = typeTag[KO]
  protected[util] final val vEv = typeTag[VO]

  protected final type Context = Mapper[KI,VI,KO,VO]#Context

  override def map(key: KI, value: VI, context: Context): Unit = super.map(key, value, context)

  override def setup(context: Context): Unit = super.setup(context)

  override def cleanup(context: Context): Unit = super.cleanup(context)
}

/**
  * TypedReducer provides a scala wrapper to the Reducer class which reduces boilerplate and captures the run-time
  * output key and value types.
  *
  * @tparam KI Key Input type
  * @tparam VI Value Input type
  * @tparam KO Key Output type
  * @tparam VO Value Output type
  */
abstract class TypedReducer[KI,VI,KO:TT,VO:TT] extends Reducer[KI,VI,KO,VO] with WithTypedOutput[KO,VO] {

  protected[util] final val kEv = typeTag[KO]
  protected[util] final val vEv = typeTag[VO]

  protected final type Context = Reducer[KI,VI,KO,VO]#Context

  /** Final overridden reduce method which handles conversion of 'values' to a scala Iterable */
  override final def reduce(key: KI, values: java.lang.Iterable[VI], context: Context): Unit = reduce(key, values.asScala, context)

  def reduce(key: KI, values: Iterable[VI], context: Context): Unit = super.reduce(key, values.asJava, context)

  override def setup(context: Context): Unit = super.setup(context)

  override def cleanup(context: Context): Unit = super.cleanup(context)

}

/**
  * A wrapped instance of a MapReduce Mapper object which encapsulates the type arguments of the Mapper
  * with TypeTags and provides a relatively type-safe way to get the class of the Mapper
  *
  * @tparam KI Key Input type
  * @tparam VI Value Input type
  * @tparam KO Key Output type
  * @tparam VO Value Output type
  */
case class WrappedMapper[KI,VI,KO,VO](m: Mapper[KI,VI,KO,VO])(implicit val kEv: TT[KO], val vEv: TT[VO])
  extends WithTypedOutput[KO,VO]
    with WrappedWithClazz[Mapper[KI,VI,KO,VO]] { self =>
  protected[util] val clazz: Class[Mapper[KI, VI, KO, VO]] = m.getClass.asInstanceOf[Class[Mapper[KI,VI,KO,VO]]]
}

/**
  * A wrapped instance of a MapReduce Reducer object which encapsulates the type arguments of the Reducer
  * with TypeTags and provides a relatively type-safe way to get the class of the Reducer
  *
  * @tparam KI Key Input type
  * @tparam VI Value Input type
  * @tparam KO Key Output type
  * @tparam VO Value Output type
  */
case class WrappedReducer[KI,VI,KO,VO](r: Reducer[KI,VI,KO,VO])(implicit val kEv: TT[KO], val vEv: TT[VO]) extends WithTypedOutput[KO,VO]
  with WrappedWithClazz[Reducer[KI,VI,KO,VO]] {
  protected[util] val clazz: Class[Reducer[KI, VI, KO, VO]] = r.getClass.asInstanceOf[Class[Reducer[KI,VI,KO,VO]]]
}

/**
  * A wrapped instance of a MapReduce Partitioner object which encapsulates the type arguments of the Partitioner
  * with TypeTags and provides a relatively type-safe way to get the class of the Partitioner
  *
  * @tparam KI Key Input type
  * @tparam VI Value Input type
  */
case class WrappedPartitioner[KI,VI](p: Partitioner[KI,VI])(implicit val kEv: TT[KI], val vEv: TT[VI]) extends WithTypedOutput[KI,VI]
  with WrappedWithClazz[Partitioner[KI,VI]] {
  protected[util] val clazz: Class[Partitioner[KI, VI]] = p.getClass.asInstanceOf[Class[Partitioner[KI,VI]]]
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
  *
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
  *
  * @tparam KI Key Input type
  * @tparam VI Value Input type
  */
sealed trait InputDefinition[KI,VI] extends HasJob

/**
  * An OutputDefinition describes the output format of a particular Job.
  * Once an OutputDefinition (and all its prerequisites) have been fully defined, the Job can be
  * run as a MapReduce job.
  *
  * @tparam KO Key Output type
  * @tparam VO Value Output type
  */
sealed trait OutputDefinition[KO,VO] extends HasJob {

  /**
    * Runs the constructed MapReduce Job
    *
    * @param verbose Enables verbose logging output
    * @param mirror Implicit type mirror used for resolving runtime types of arguments
    */
  def run(verbose: Boolean = true)(implicit mirror: reflect.runtime.universe.Mirror): Boolean = {
    job.waitForCompletion(verbose)
  }
}

case class FileInputDefinition[KI, VI](baseJob: BaseJob,
                                       path: Path,
                                       inputFormat: Class[_<:FileInputFormat[KI,VI]]) extends InputDefinition[KI,VI] {
  def job(implicit mirror: reflect.runtime.universe.Mirror) = {
    val jobIn = baseJob.job
    FileInputFormat.addInputPath(jobIn, path)
    jobIn.setInputFormatClass(inputFormat)
    jobIn
  }
}

case class FileOutputDefinition[KO, VO](reduceStage: ReducedJob[_,_,KO,VO],
                                        path: Path,
                                        outputFormat: Class[_ <: FileOutputFormat[_,_]] = classOf[TextOutputFormat[KO,VO]],
                                        deleteExisting: Boolean = true) extends OutputDefinition[KO,VO] {
  def job(implicit mirror: reflect.runtime.universe.Mirror) = {
    val jobIn = reduceStage.job
    FileOutputFormat.setOutputPath(jobIn, path)
    jobIn.setOutputFormatClass(outputFormat)
    if(deleteExisting)
      FileSystem.get(jobIn.getConfiguration).delete(path, true)

    jobIn
  }

  def withFormat[F1<:FileOutputFormat[KO,VO]:TT](implicit mirror: reflect.runtime.universe.Mirror) =
    this.copy[KO,VO](outputFormat = mirror.runtimeClass(typeOf[F1].typeSymbol.asClass).asInstanceOf[Class[F1]])

}

/**
  * A class representing the "map" stage of a MapReduce job.
  * Also contains optional Partitioner and Combiner objects.
  *
  * @param inputDefinition Reference to the "input" stage of the job, which must be defined before the mapper
  * @param map Wrapped mapper object
  * @param combine Optional wrapped combiner(reducer) object
  * @param partitioner Optional wrapped partitioner object
  * @tparam KI Key Input type
  * @tparam VI Value Input type
  * @tparam KO Key Output type
  * @tparam VO Value Output type
  */
case class MappedJob[KI, VI, KO, VO](inputDefinition: InputDefinition[KI,VI],
                                     map: WrappedMapper[KI,VI,KO,VO],
                                     combine: Option[WrappedReducer[KO,VO,KO,VO]] = None,
                                     partitioner: Option[WrappedPartitioner[KO,VO]] = None)
  extends HasJob {

  /**
    * Creates a copy of this stage with the combiner set
    *
    * @param c Wrapped combiner (reducer) object
    */
  def combine(c: WrappedReducer[KO,VO,KO,VO]): MappedJob[KI,VI,KO,VO] =
    this.copy(combine = Option(c))

  /**
    * Creates a copy of this stage with the partitioning behaviour set
    *
    * @param p Wrapped partitioner object
    */
  def partition(p: WrappedPartitioner[KO,VO]): MappedJob[KI,VI,KO,VO] =
    this.copy(partitioner = Option(p))

  /**
    * Creates a "reduce" stage with this map stage as the dependency
    *
    * @param reducer Wrapped reducer object
    * @param numReduceTasks Number of reduce tasks (default = 1)
    * @tparam RKO Reducer Key Output
    * @tparam RVO Reducer Value Output
    */
  def reduce[RKO, RVO](reducer: WrappedReducer[KO,VO,RKO,RVO], numReduceTasks: Int = 1): ReducedJob[KO,VO,RKO,RVO] =
    ReducedJob[KO,VO,RKO,RVO](this, reduce = reducer, numReduceTasks)

  override def job(implicit mirror: reflect.runtime.universe.Mirror): Job = {
    val jobIn = inputDefinition.job
    jobIn.setMapOutputKeyClass(map.outputKeyType)
    jobIn.setMapOutputValueClass(map.outputValueType)
    jobIn.setMapperClass(map.clazz)
    combine.foreach(c => jobIn.setCombinerClass(c.clazz))
    partitioner.foreach(p => jobIn.setPartitionerClass(p.clazz))
    jobIn
  }
}

/**
  * A class representing the "reduce" stage of a MapReduce job
  *
  * @param mapStage Reference to the "map" stage of the job, which must be defined before the reduce
  * @param reduce Wrapped reducer object
  * @param numReduceTasks Number of reducers (default 1)
  * @tparam KI Key Input type
  * @tparam VI Value Input type
  * @tparam KO Key Output type
  * @tparam VO Value Output type
  */
case class ReducedJob[KI, VI, KO, VO](mapStage: MappedJob[_,_,KI,VI],
                                      reduce: WrappedReducer[KI,VI,KO,VO],
                                      numReduceTasks: Int = 1)
  extends HasJob {

  /**
    * Specify the combiner behaviour (this creates a new copy of the "map" dependency)
    *
    * @param c Wrapped reducer object to use as combiner
    */
  def combine(c: WrappedReducer[KI,VI,KI,VI]): ReducedJob[KI,VI,KO,VO] =
    this.copy(mapStage = mapStage.combine(c))

  /**
    * Specify the partitioning behaviour (this creates a new copy of the "map" dependency)
    *
    * @param p Wrapped partitioner object to use as partitioner
    */
  def partitionBy(p: WrappedPartitioner[KI,VI]): ReducedJob[KI,VI,KO,VO] =
    this.copy(mapStage = mapStage.partition(p))

  override def job(implicit mirror: reflect.runtime.universe.Mirror): Job = {
    val jobIn = mapStage.job
    jobIn.setOutputKeyClass(reduce.outputKeyType)
    jobIn.setOutputValueClass(reduce.outputValueType)
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
    *
    * @param name Name of the MapReduce job
    * @param caller Class of the invoking class. This can be provided implicitly, such as in the case where
    *               the enclosing class extends [[WithCallingClass]]
    */
  def job(name: String)(implicit caller: Class[_]): BaseJob = BaseJob(name, callingClass = caller)

  /**
    * Creates a BaseJob instance which can be chained with other operations to configure a MapReduce Job.
    *
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
      *
      * @param path Path of the file to read in
      * @param inputFormat Hadoop FileInputFormat[KI,VI] which determines how to parse the file
      * @tparam KI Key Input type of the file - Will determine the input key type of the Mapper stage
      * @tparam VI Value Input type of the file - Will determine the input value type of the Mapper stage
      */
    def file[KI, VI](path: Path,
                     inputFormat: Class[FileInputFormat[KI,VI]]): FileInputDefinition[KI,VI] = {
      FileInputDefinition(job, path, inputFormat)
    }

    /**
      * textFile creates a FileInputDefinition specifically using the TextInputFormat. The resulting
      * key type is LongWritable, and the value type is Text.
      *
      * @param path Path of the file to read in
      */
    def textFile(path: Path): FileInputDefinition[LongWritable, Text] = {
      FileInputDefinition(job, path, classOf[TextInputFormat])
    }
  }

  implicit class InputStageSyntax[KI:TT,VI:TT](inputStage: InputDefinition[KI,VI]) {
    /**
      * Create a MapStage from an input stage
      *
      * @param map Wrapped Mapper object to set as the Mapper for this job
      * @tparam KO Key Output type of the mapper
      * @tparam VO Value Output type of the mapper
      */
    def map[KO, VO](map: WrappedMapper[KI,VI,KO,VO]) = MappedJob(inputStage, map)
  }

  /**
    * Set of syntax operations which are valid only on "reduce" stages. These operations are mostly concerned with
    * determining the output location and format for the job.
    *
    * @param reduceStage Input "reduce" stage
    * @tparam KO Output key type of the reduce stage
    * @tparam VO Output value type of the reduce stage
    */
  implicit class ReduceStageSyntax[KO, VO](reduceStage: ReducedJob[_,_,KO,VO]) {
    /**
      * Bind the output path of the job to a file, and optionally delete the file(s) currently present
      * at that location
      *
      * @param path Path of output directory
      * @param deleteExisting If true, deletes the contents of the output directory before running the job
      */
    def setOutputAsFile(path: Path, deleteExisting: Boolean = true): FileOutputDefinition[KO,VO] = {
      FileOutputDefinition(reduceStage, path, deleteExisting = deleteExisting)
    }

    /**
      * Sets the output destination, configures output to be TextOutputFormat, and runs the MapReduce job immediately.
      *
      * @param path Path of output directory
      * @param deleteExisting If true, deletes the contents of the output directory before running the job
      * @param verbose If true, enables verbose output from the framework while running
      * @param mirror Implicit type mirror used for resolving runtime types of arguments
      */
    def saveAsTextFile(path: Path, deleteExisting: Boolean = true, verbose: Boolean = true)
                      (implicit mirror: reflect.runtime.universe.Mirror): Boolean = {
      val jobWithOutput = FileOutputDefinition(
        reduceStage, path, outputFormat = classOf[TextOutputFormat[KO,VO]], deleteExisting)
      jobWithOutput.run(verbose)
    }

    /**
      * Sets the output destination, configures output to be SequenceFileOutputFormat, and runs the MapReduce job immediately.
      *
      * @param path Path of output directory
      * @param deleteExisting If true, deletes the contents of the output directory before running the job
      * @param verbose If true, enables verbose output from the framework while running
      * @param mirror Implicit type mirror used for resolving runtime types of arguments
      */
    def saveAsSequenceFile(path: Path, deleteExisting: Boolean = true, verbose: Boolean = true)
                          (implicit mirror: reflect.runtime.universe.Mirror): Boolean = {
      val jobWithOutput = FileOutputDefinition(
        reduceStage, path, outputFormat = classOf[SequenceFileOutputFormat[KO,VO]], deleteExisting)
      jobWithOutput.run(verbose)
    }
  }

  /**
    * Set of syntax operations which are valid only on "reduce" stages which have a key class extending
    * WritableComparable. These operations are mostly concerned with determining the output location and format for the job.
    *
    * @param reduceStage Input "reduce" stage
    * @tparam KO Output key type of the reduce stage. Must extend WritableComparable
    * @tparam VO Output value type of the reduce stage
    */
  implicit class WritableComparableReduceStageSyntax[KO<:WritableComparable[_],VO](reduceStage: ReducedJob[_,_,KO,VO]) {

    /**
      * Sets the output destination, configures output to be MapFileOutputFormat, and runs the MapReduce job immediately.
      *
      * @param path Path of output directory
      * @param deleteExisting If true, deletes the contents of the output directory before running the job
      * @param verbose If true, enables verbose output from the framework while running
      * @param mirror Implicit type mirror used for resolving runtime types of arguments
      */
    def saveAsMapFile(path: Path, deleteExisting: Boolean = true, verbose: Boolean = true)
                     (implicit mirror: reflect.runtime.universe.Mirror): Boolean = {
      val jobWithOutput = FileOutputDefinition(
        reduceStage, path, outputFormat = classOf[MapFileOutputFormat], deleteExisting)
      jobWithOutput.run(verbose)
    }

  }

}

/**
  * MapReduceSugar brings together the above syntax and utility traits into a single trait which can be mixed
  * in to a class to provide a convenient way to specify MapReduce jobs in Scala.
  */
trait MapReduceSugar
  extends StageSyntax
    with WithCallingClass
    with WithMirror
    with WritableConversions

