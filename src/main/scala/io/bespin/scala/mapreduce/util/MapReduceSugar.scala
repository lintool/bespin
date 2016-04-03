package io.bespin.scala.mapreduce.util

import io.bespin.scala.util.{PairWritableConversions, WritableConversions}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text, WritableComparable}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, MapFileOutputFormat, SequenceFileOutputFormat, TextOutputFormat}

import scala.collection.JavaConverters._
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.runtime.universe.{typeOf, typeTag, TypeTag => TT}

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

object TypedMapper {
  /**
    * The null mapper performs no map operations, but maintains the type-safety of the operation
    * chain.
    */
  def nullMapper[KO:TT,VO:TT]: TypedMapper[KO,VO,KO,VO] = new TypedMapper[KO,VO,KO,VO] {}
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

object TypedReducer {
  /**
    * The null reducer performs no reduce operations, but maintains the type-safety of the operation
    * chain.
    */
  def nullReducer[KO:TT,VO:TT]: TypedReducer[KO,VO,KO,VO] = new TypedReducer[KO,VO,KO,VO] {}
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
  * A Stage is some hadoop job component which produces a typed output
  */
sealed trait Stage[K,V] extends HasJob with WithTypedOutput[K, V]

/**
  * StageFromtype is a wrapper abstract class which fills in the key and value type information using some other
  * object which provides key and value information.
  */
abstract sealed class StageFromType[K,V](typeSource: WithTypedOutput[K,V]) extends Stage[K,V] {
  override protected[util] final val kEv: TT[K] = typeSource.kEv
  override protected[util] final val vEv: TT[V] = typeSource.vEv
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
sealed trait InputDefinition[KI,VI] extends Stage[KI,VI]

/**
  * An OutputDefinition describes the output format of a particular Job.
  * Once an OutputDefinition (and all its prerequisites) have been fully defined, the Job can be
  * run as a MapReduce job.
  *
  * @tparam KO Key Output type
  * @tparam VO Value Output type
  */
sealed trait OutputDefinition[KO,VO] extends Stage[KO,VO] {

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

case class FileInputDefinition[KI:TT, VI:TT](baseJob: BaseJob,
                                             path: Path,
                                             inputFormat: Class[_<:FileInputFormat[KI,VI]]) extends InputDefinition[KI,VI] {
  def job(implicit mirror: reflect.runtime.universe.Mirror) = {
    val jobIn = baseJob.job
    FileInputFormat.addInputPath(jobIn, path)
    jobIn.setInputFormatClass(inputFormat)
    jobIn
  }

  override protected[util] val kEv: TT[KI] = typeTag[KI]
  override protected[util] val vEv: TT[VI] = typeTag[VI]
}

case class FileOutputDefinition[KO, VO](stage: Stage[KO,VO],
                                        path: Path,
                                        outputFormat: Class[_ <: FileOutputFormat[_,_]] = classOf[TextOutputFormat[KO,VO]],
                                        deleteExisting: Boolean = true) extends StageFromType[KO,VO](stage) with OutputDefinition[KO,VO] {
  def job(implicit mirror: reflect.runtime.universe.Mirror) = {
    val jobIn = stage.job
    FileOutputFormat.setOutputPath(jobIn, path)
    jobIn.setOutputFormatClass(outputFormat)
    if (deleteExisting)
      FileSystem.get(jobIn.getConfiguration).delete(path, true)

    jobIn
  }

  def withFormat[F1 <: FileOutputFormat[KO, VO] : TT](implicit mirror: reflect.runtime.universe.Mirror) =
    this.copy[KO, VO](outputFormat = mirror.runtimeClass(typeOf[F1].typeSymbol.asClass).asInstanceOf[Class[F1]])
}

/**
  * A MapStage object encapsulates the logic for setting the relevant Hadoop job parameters for the given mapper.
  * @param input Input stage; must not already have a mapper class set.
  * @param mapper Mapper object to set the Hadoop job to use
  */
case class MapStage[KI,VI,KO,VO](input: Stage[KI,VI],
                                 mapper: WrappedMapper[KI,VI,KO,VO])
  extends StageFromType[KO,VO](mapper) {

  override def job(implicit mirror: reflect.runtime.universe.Mirror): Job = {
    val jobIn = input.job
    jobIn.setMapOutputKeyClass(mapper.outputKeyType)
    jobIn.setMapOutputValueClass(mapper.outputValueType)
    val currentMapper = Option(jobIn.getMapperClass)
    // Throw an exception if a non-default mapper is found to be on the job already
    if(currentMapper != Some(classOf[Mapper[_,_,_,_]])) {
      throw new IllegalArgumentException(
        "Attempted to set a mapper on a Hadoop job, but found one set on the job already. " +
          "Are you calling map() twice?"
      )
    }
    jobIn.setMapperClass(mapper.clazz)
    jobIn
  }
}

/**
  * A CombineStage object encapsulates the logic for setting the relevant Hadoop job parameters for the given combiner.
  * @param input Input stage; must not already have a combiner class set.
  * @param combiner Combiner object to set the Hadoop job to use.
  */
case class CombineStage[K,V](input: Stage[K,V],
                             combiner: WrappedReducer[K,V,K,V])
  extends StageFromType[K,V](combiner) {

  override def job(implicit mirror: reflect.runtime.universe.Mirror): Job = {
    val jobIn = input.job
    val currentCombiner = Option(jobIn.getCombinerClass)
    // Throw an exception if a non-default combiner is found to be on the job already
    if(currentCombiner.isDefined) {
      throw new IllegalArgumentException(
        "Attempted to set a combiner on a Hadoop job, but found one set on the job already. " +
          "Are you calling combine() twice?"
      )
    }
    jobIn.setCombinerClass(combiner.clazz)
    jobIn
  }
}

/**
  * A PartitionStage object encapsulates the logic for setting the relevant Hadoop job parameters for the given
  * partitioner.
  *
  * @param input Input stage
  * @param partitioner Partitioner object to set the Hadoop job to use; will overwrite any existing partitioner already
  *                    set for the job.
  */
case class PartitionStage[K,V](input: Stage[K,V],
                               partitioner: WrappedPartitioner[K,V])
  extends StageFromType[K,V](partitioner) {

  override def job(implicit mirror: reflect.runtime.universe.Mirror): Job = {
    val jobIn = input.job
    // Unfortunately cannot reliably check to see if partitioner is set here because the default partitioner
    // can be a number of different classes.
    jobIn.setPartitionerClass(partitioner.clazz)
    jobIn
  }
}

/**
  * A ReduceStage object encapsulates the logic for setting the relevant Hadoop job parameters for the given
  * reduce stage and number of reducers
  *
  * @param input Input stage; must not have already had a reducer set
  * @param reducer Reducer object to set the Hadoop job to use
  * @param numReducers Number of reduce tasks to run
  */
case class ReduceStage[KI,VI,KO,VO](input: Stage[KI,VI],
                                    reducer: WrappedReducer[KI,VI,KO,VO],
                                    numReducers: Int = 1)
  extends StageFromType[KO,VO](reducer) {

  override def job(implicit mirror: reflect.runtime.universe.Mirror): Job = {
    val jobIn = input.job
    jobIn.setOutputKeyClass(reducer.outputKeyType)
    jobIn.setOutputValueClass(reducer.outputValueType)
    val currentReducer = Option(jobIn.getReducerClass)
    // Throw an exception if a non-default reducer is found to be on the job already
    if(currentReducer != Some(classOf[Reducer[_,_,_,_]])) {
      throw new IllegalArgumentException(
        "Attempted to set a reducer on a Hadoop job, but found one set on the job already. " +
          "Are you calling reduce() twice?"
      )
    }
    jobIn.setReducerClass(reducer.clazz)
    jobIn.setNumReduceTasks(numReducers)
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
  * CompositionSyntax provides various operations for manipulating various stages of a MapReduce Job in order to
  * define a job in a type-safe and functional way.
  */
trait CompositionSyntax extends WrappingSyntax {

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
    def file[KI:TT, VI:TT](path: Path,
                           inputFormat: Class[_<:FileInputFormat[KI,VI]]): FileInputDefinition[KI,VI] = {
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

  /**
    * This syntax class provides the main operations for MapReduce jobs: mapping, reducing, partitioning, and combining.
    */
  implicit class StageSyntax[K:TT,V:TT](stage: Stage[K,V]) {
    def map[KO,VO](mapper: WrappedMapper[K,V,KO,VO]): MapStage[K, V, KO, VO] = MapStage(stage, mapper)
    def combine(combiner: WrappedReducer[K,V,K,V]): CombineStage[K,V] = CombineStage(stage, combiner)
    def partition(partitioner: WrappedPartitioner[K,V]): PartitionStage[K,V] = PartitionStage(stage, partitioner)
    def reduce[KO,VO](reducer: WrappedReducer[K,V,KO,VO], numReducers: Int = 1): ReduceStage[K,V,KO,VO] = ReduceStage(stage, reducer, numReducers)
    def reduce(numReducers: Int = 1): ReduceStage[K,V,K,V] = ReduceStage(stage, TypedReducer.nullReducer[K,V], numReducers)
  }

  /**
    * Set of syntax operations which are valid only on "output" stages. These operations are mostly concerned with
    * determining the output location and format for the job.
    *
    * @param outputStage Input "output" stage (usually a reduce stage, but can be a map with no reduce)
    * @tparam KO Output key type of the output stage
    * @tparam VO Output value type of the output stage
    */
  implicit class OutputStageSyntax[KO, VO](outputStage: Stage[KO,VO]) {
    /**
      * Bind the output path of the job to a file, and optionally delete the file(s) currently present
      * at that location
      *
      * @param path Path of output directory
      * @param deleteExisting If true, deletes the contents of the output directory before running the job
      */
    def setOutputAsFile(path: Path, deleteExisting: Boolean = true): FileOutputDefinition[KO,VO] = {
      FileOutputDefinition(outputStage, path, deleteExisting = deleteExisting)
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
        outputStage, path, outputFormat = classOf[TextOutputFormat[KO,VO]], deleteExisting)
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
        outputStage, path, outputFormat = classOf[SequenceFileOutputFormat[KO,VO]], deleteExisting)
      jobWithOutput.run(verbose)
    }
  }

  /**
    * Set of syntax operations which are valid only on stages which have a key class extending
    * WritableComparable. These operations are mostly concerned with determining the output location and format for the job.
    *
    * @param outputStage Output stage
    * @tparam KO Output key type of the stage. Must extend WritableComparable
    * @tparam VO Output value type of the stage
    */
  implicit class WritableComparableOutputStageSyntax[KO<:WritableComparable[_],VO](outputStage: Stage[KO,VO]) {

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
        outputStage, path, outputFormat = classOf[MapFileOutputFormat], deleteExisting)
      jobWithOutput.run(verbose)
    }

  }

}

/**
  * MapReduceSugar brings together the above syntax and utility traits into a single trait which can be mixed
  * in to a class to provide a convenient way to specify MapReduce jobs in Scala.
  */
trait MapReduceSugar
  extends CompositionSyntax
    with WithCallingClass
    with WithMirror
    with WritableConversions
    with PairWritableConversions

