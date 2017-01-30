/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.gpuenabler

import org.apache.spark.rdd._
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.{Partition, TaskContext, _}

import scala.language.implicitConversions
import scala.reflect.ClassTag
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2, _}

private[gpuenabler] case class Key(val rdd : RDD[Int], key : RDDBlockId)

private[gpuenabler] object GPUKeyManager {

  private var gpuMemoryManagerKey : Option[Key] = None;
  private var cudaManagerKey : Option[Key] = None;


  def getGPUMemoryManagerKey(sc : SparkContext) = {
    gpuMemoryManagerKey.getOrElse {
      gpuMemoryManagerKey = Some {
        val x = sc.parallelize(1 to 1);
        Key(x, RDDBlockId(x.id, 0)) }
    }
    gpuMemoryManagerKey.get
  }

  def getCudaManagerKey(sc : SparkContext) = {
    cudaManagerKey.getOrElse {
      cudaManagerKey = Some {
        val x = sc.parallelize(1 to 1);
        Key(x, RDDBlockId(x.id, 0))
      }
    }
    cudaManagerKey.get
  }
}

/**
  * An RDD that applies the provided CUDA kernel to every partition of the parent RDD.
  *
  * @param prev previous RDD reference
  * @param f  lambda to be executed on each element of this RDD
  * @param kernel holds the External function which points to the GPU native
  *               function name and the GPU kernel associated with it
  * @param preservesPartitioning Default `true` to preserve the partitioning
  * @param outputArraySizes If the expected result is an array folded in a linear
  *                         form, specific a sequence of the array length for every
  *                         output columns
  * @param inputFreeVariables Specify a list of free variable that need to be
  *                           passed in to the GPU kernel function, if any
  */
private[gpuenabler] class MapGPUPartitionsRDD[U: ClassTag, T: ClassTag](
       prev: RDD[T],
       f: (TaskContext, Int, Iterator[T]) => Iterator[U], // (TaskContext, partition index, iterator)
       kernel : ExternalFunction = null,
       preservesPartitioning: Boolean = false,
       outputArraySizes: Seq[Int] = null,
       inputFreeVariables: Seq[Any] = null)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  private val inputColSchema: ColumnPartitionSchema = ColumnPartitionSchema.schemaFor[T]
  private val outputColSchema: ColumnPartitionSchema = ColumnPartitionSchema.schemaFor[U]

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    if (GPUSparkEnv.get.isGPUEnabled) {
      // Use the block ID of this particular (rdd, partition)
      val blockId = RDDBlockId(this.id, split.index)

      // Handle empty partitions.
      if (firstParent[T].iterator(split, context).length <= 0)
        return new Array[U](0).toIterator

      val inputHyIter = firstParent[T].iterator(split, context) match {
        case hyIter: HybridIterator[T] => {
          hyIter
        }
        case iter: Iterator[T] => {
          // println("Converting Regular Iterator to hybridIterator")
          val parentBlockId = RDDBlockId(firstParent[T].id, split.index)
          val hyIter = new HybridIterator[T](iter.toArray, inputColSchema,
            kernel.inputColumnsOrder, Some(parentBlockId))
          hyIter
        }
      }

      val resultIter = kernel.compute[U, T](inputHyIter,
        Seq(inputColSchema, outputColSchema), None,
        outputArraySizes, inputFreeVariables, Some(blockId))

      resultIter
    } else {
      f(context, split.index, firstParent[T].iterator(split, context))
    }
  }
}

/**
  * An RDD that convert partition's iterator to a format supported by GPU computation
  * to every partition of the parent RDD.
  */
private[gpuenabler] class ConvertGPUPartitionsRDD[T: ClassTag](
        prev: RDD[T],
        preservesPartitioning: Boolean = false)
  extends RDD[T](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  val inputColSchema: ColumnPartitionSchema = ColumnPartitionSchema.schemaFor[T]

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    // Use the block ID of this particular (rdd, partition)
    val blockId = RDDBlockId(this.id, split.index)

    val resultIter = firstParent[T].iterator(split, context) match {
      case hyIter: HybridIterator[T] => {
        hyIter
      }
      case iter: Iterator[T] => {
        // println("Converting Regular Iterator to hybridIterator")
        // val parentBlockId = RDDBlockId(firstParent[T].id, split.index)
        val hyIter = new HybridIterator[T](iter.toArray, inputColSchema,
          null, Some(blockId))
        hyIter
      }
    }

    resultIter
  }
}

/**
  * Wrapper Function for Java APIs. It exposes 4 APIs
  * mapExtFunc, reduceExtFunc, cacheGpu, unCacheGpu
  *
  * @param rdd Name of the Native code's function
  * classTag need to be passed in to a scala API from java program only
  * as a last argument.
  *
  * {{{
  * import com.ibm.gpuenabler.JavaCUDARDD;
  *
  *      JavaRDD<Integer> inputData = sc.parallelize(range);
  *      ClassTag<Integer> tag = scala.reflect.ClassTag$.MODULE$.apply(Integer.TYPE);
  *      JavaCUDARDD<Integer> ci = new JavaCUDARDD(inputData.rdd(), tag);
  * }}}
  *
  */
class JavaCUDARDD[T: ClassTag](override val rdd: RDD[T])
  extends JavaRDD[T](rdd) {

  import CUDARDDImplicits._

  override def wrapRDD(rdd: RDD[T]): JavaCUDARDD[T] = JavaCUDARDD.fromRDD(rdd)

  implicit def toScalaFunction[T, R](fun: JFunction[T, R]): T => R = x => fun.call(x)

  implicit def toScalaFunction[T, R](fun: JFunction2[T, T, R]): (T, T) => R = (x, y) => fun.call(x, y)

  def mapExtFunc[U: ClassTag](f: JFunction[T, U],
                                  extfunc: JavaCUDAFunction): JavaCUDARDD[U] =
  {
    def fn: (T) => U = (x: T) => f.call(x)
    new JavaCUDARDD[U](rdd.mapExtFunc[U](fn, extfunc.cf,
      null, null))
  }

  def mapExtFunc[U: ClassTag](fn: JFunction[T, U], extfunc: JavaCUDAFunction,
                                  outputArraySizes: Seq[Int] = null,
                                  inputFreeVariables: Seq[Any] = null): JavaCUDARDD[U] =
  {    
    new JavaCUDARDD[U](rdd.mapExtFunc(fn, extfunc.cf,
      outputArraySizes, inputFreeVariables))
  }

  def reduceExtFunc(fn: JFunction2[T, T, T], extfunc: JavaCUDAFunction,
                        outputArraySizes: Seq[Int] = null,
                        inputFreeVariables: Seq[Any] = null): T = {    
    rdd.reduceExtFunc(fn,extfunc.cf, outputArraySizes, inputFreeVariables)
  }

  def reduceExtFunc(fn: JFunction2[T, T, T], extfunc: JavaCUDAFunction): T = {    
    rdd.reduceExtFunc(fn,extfunc.cf, null, null)
  }

  def cacheGpu() = wrapRDD(rdd.cacheGpu())

  def unCacheGpu() = wrapRDD(rdd.unCacheGpu())
}

object JavaCUDARDD {
  implicit def fromRDD[T: ClassTag](rdd: RDD[T]): JavaCUDARDD[T] =
    new JavaCUDARDD[T](rdd)

  implicit def toRDD[T](rdd: JavaCUDARDD[T]): RDD[T] = rdd.rdd
}

/**
  * Adds additional functionality to existing RDD's which are
  * specific to performing computation on Nvidia GPU's attached
  * to executors. To use these additional functionality import
  * the following packages,
  *
  * {{{
  * import com.ibm.gpuenabler.cuda._
  * import com.ibm.gpuenabler.CUDARDDImplicits._
  * }}}
  *
  */
object CUDARDDImplicits {

  implicit class CUDARDDFuncs[T: ClassTag](rdd: RDD[T])
    extends Serializable {

    def sc = rdd.sparkContext

    /**
      * This function is used to mark the respective RDD's data to
      * be cached in GPU for future computation rather than cleaning it
      * up every time the RDD is processed. 
      * 
      * By marking an RDD to cache in GPU, huge performance gain can
      * be achieved as data movement between CPU memory and GPU 
      * memory is considered costly.
      */
    def cacheGpu() = {
      GPUSparkEnv.get.gpuMemoryManager.cacheGPUSlaves(rdd.id); rdd
    }

    /**
      * This function is used to clean up all the caches in GPU held
      * by the respective RDD on the various partitions.
      */
    def unCacheGpu() = {
      GPUSparkEnv.get.gpuMemoryManager.unCacheGPUSlaves(rdd.id); rdd
    }

    /**
      * Return a new RDD by applying a function to all elements of this RDD.
      *
      * @param f  Specify the lambda to apply to all elements of this RDD
      * @param extfunc  Provide the ExternalFunction instance which points to the
      *                 GPU native function to be executed for each element in
      *                 this RDD
      * @param outputArraySizes If the expected result is an array folded in a linear
      *                         form, specific a sequence of the array length for every
      *                         output columns
      * @param inputFreeVariables Specify a list of free variable that need to be
      *                           passed in to the GPU kernel function, if any
      * @tparam U Result RDD type
      * @return Return a new RDD of type U after executing the user provided
      *         GPU function on all elements of this RDD
      */
    def mapExtFunc[U: ClassTag](f: T => U, extfunc: ExternalFunction,
                                outputArraySizes: Seq[Int] = null,
                                inputFreeVariables: Seq[Any] = null): RDD[U] = {
      import org.apache.spark.gpuenabler.CUDAUtils
      val cleanF = CUDAUtils.cleanFn(sc, f) // sc.clean(f)
      new MapGPUPartitionsRDD[U, T](rdd, (context, pid, iter) => iter.map(cleanF),
        extfunc, outputArraySizes = outputArraySizes, inputFreeVariables = inputFreeVariables)
    }

    /**
     * Return a new RDD by applying a function to all elements of this RDD.
     */
    def mapGpu[U: ClassTag](f: T => U): RDD[U] = {
      import org.apache.spark.gpuenabler.CUDAUtils
      val cleanF = CUDAUtils.cleanFn(sc, f) // sc.clean(f)
      val cudaFunc = CUDACodeGenerator.generateForMap[U, T](cleanF).getOrElse(
        throw new UnsupportedOperationException("Cannot generate GPU code")
      )
      new MapGPUPartitionsRDD[U, T](rdd, (context, pid, iter) => iter.map(cleanF), cudaFunc)
    }


    /**
      * Trigger a reduce action on all elements of this RDD.
      *
      * @param f Specify the lambda to apply to all elements of this RDD
      * @param extfunc Provide the ExternalFunction instance which points to the
      *                 GPU native function to be executed for each element in
      *                 this RDD
      * @param outputArraySizes If the expected result is an array folded in a linear
      *                         form, specific a sequence of the array length for every
      *                         output columns
      * @param inputFreeVariables Specify a list of free variable that need to be
      *                           passed in to the GPU kernel function, if any
      * @return Return the result after performing a reduced operation on all
      *         elements of this RDD
      */
    def reduceExtFunc(f: (T, T) => T, extfunc: ExternalFunction,
                      outputArraySizes: Seq[Int] = null,
                      inputFreeVariables: Seq[Any] = null): T = {
      import org.apache.spark.gpuenabler.CUDAUtils

      val cleanF = CUDAUtils.cleanFn(sc, f) // sc.clean(f)

      val reducePartition: (TaskContext, Iterator[T]) => Option[T] = (ctx: TaskContext, data: Iterator[T]) =>  {
        if (GPUSparkEnv.get.isGPUEnabled) {
          val inputColSchema: ColumnPartitionSchema = ColumnPartitionSchema.schemaFor[T]
          val outputColSchema: ColumnPartitionSchema = ColumnPartitionSchema.schemaFor[T]

          data match {
            case col: HybridIterator[T] =>
              if (col.numElements != 0) {
                val colIter = extfunc.compute[T, T](col, Seq(inputColSchema, outputColSchema),
                  Some(1), outputArraySizes,
                  inputFreeVariables, None).asInstanceOf[HybridIterator[T]]
                Some(colIter.next)
              } else {
                None
              }
            // Handle partitions with no data
            case _ =>
              if (data.hasNext) {
                Some(data.reduceLeft(cleanF))
              }
              else {
                None
              }
          }
        } else {
          if (data.hasNext) {
            Some(data.reduceLeft(cleanF))
          }
          else {
            None
          }
        }
      }

      var jobResult: Option[T] = None
      val mergeResult = (index: Int, taskResult: Option[T]) => {
        if (taskResult.isDefined) {
          jobResult = jobResult match {
            case Some(value) => Some(f(value, taskResult.get))
            case None => taskResult
          }
        }
      }

      sc.runJob(rdd, reducePartition, 0 until rdd.partitions.length, mergeResult)
      jobResult.getOrElse(throw new UnsupportedOperationException("empty collection"))
    }

    /**
     * Reduces the elements of this RDD using the specified commutative and
     * associative binary operator.
     */
    def reduceGpu(f: (T, T) => T): T = {
      import org.apache.spark.gpuenabler.CUDAUtils

      val cleanF = CUDAUtils.cleanFn(sc, f) // sc.clean(f)

      val cudaFunc = CUDACodeGenerator.generateForReduce[T](cleanF).getOrElse(
        throw new UnsupportedOperationException("Cannot generate GPU code")
      )

      val inputColSchema: ColumnPartitionSchema = ColumnPartitionSchema.schemaFor[T]
      val outputColSchema: ColumnPartitionSchema = ColumnPartitionSchema.schemaFor[T]

      val reducePartition: (TaskContext, Iterator[T]) => Option[T] =
        (ctx: TaskContext, data: Iterator[T]) => {
          // Handle partitions with no data
          if (data.length > 0) {
            data match {
              case col: HybridIterator[T] =>
                if (col.numElements != 0) {
                  val colIter = cudaFunc.compute[T, T](col, Seq(inputColSchema, outputColSchema),
                    Some(1), null, null, None).asInstanceOf[HybridIterator[T]]
                  Some(colIter.next)
                } else {
                  None
                }
            }
          } else None
        }

      var jobResult: Option[T] = None
      val mergeResult = (index: Int, taskResult: Option[T]) => {
        if (taskResult.isDefined) {
          jobResult = jobResult match {
            case Some(value) => Some(f(value, taskResult.get))
            case None => taskResult
          }
        }
      }
      sc.runJob(rdd, reducePartition, 0 until rdd.partitions.length, mergeResult)
      jobResult.getOrElse(throw new UnsupportedOperationException("empty collection"))
    }

    /**
     * Return a new RDD by applying a function to all elements of this RDD.
     */
    private[gpuenabler] def convert(x: PartitionFormat, unpersist: Boolean = true): RDD[T] = {
      val convertedRDD = new ConvertGPUPartitionsRDD[T](rdd)
      if (unpersist) {
        rdd.unpersist(false)
      }
      convertedRDD
    }
  }
}
