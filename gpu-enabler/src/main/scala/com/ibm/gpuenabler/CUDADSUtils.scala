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

import jcuda.driver.CUdeviceptr
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.spark.sql.gpuenabler.CUDAUtils._

case class MAPGPUExec[T, U](cf: DSCUDAFunction, constArgs : Array[Any],
                            outputArraySizes: Array[Int],
                            child: SparkPlan,
                            inputEncoder: Encoder[T], outputEncoder: Encoder[U],
                            outputObjAttr: Attribute,
                            cached: Int,
                            gpuPtrs: Array[mutable.HashMap[String, CUdeviceptr]])
  extends ObjectConsumerExec with ObjectProducerExec  {

  lazy val inputSchema: StructType = inputEncoder.schema
  lazy val outputSchema: StructType = outputEncoder.schema

  override def output: Seq[Attribute] = outputObjAttr :: Nil

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext,
      "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    val inexprEnc = inputEncoder.asInstanceOf[ExpressionEncoder[T]]
    val outexprEnc = outputEncoder.asInstanceOf[ExpressionEncoder[U]]

    val childRDD = child.execute()

    childRDD.mapPartitions{ iter =>
      val jcudaIterator = JCUDACodeGen.generate(inputSchema,
                     outputSchema,cf,constArgs, outputArraySizes)
      val list = new mutable.ListBuffer[InternalRow]
      iter.foreach(x =>
        list += inexprEnc.toRow(x.get(0, inputSchema).asInstanceOf[T]).copy())

      // cached: 1 -> this logical plan is cached; 2 -> child logical plan is cached
      val imgpuPtrs: java.util.List[java.util.Map[String, CUdeviceptr]] = cached match {
        case 1 => List(gpuPtrs(0).asJava, Map[String, CUdeviceptr]().asJava).asJava
        case 2 => List(Map[String, CUdeviceptr]().asJava, gpuPtrs(1).asJava).asJava
        case _ => List(Map[String, CUdeviceptr]().asJava, Map[String, CUdeviceptr]().asJava).asJava
      }

      val (stages, userGridSizes, userBlockSizes) = JCUDACodeGen.getUserDimensions(list.size)

      jcudaIterator.init(list.toIterator.asJava, constArgs,
                list.size, cached, imgpuPtrs, userGridSizes, userBlockSizes, stages)

      new Iterator[InternalRow] {
        override def hasNext: Boolean = jcudaIterator.hasNext()

        override def next: InternalRow =
          InternalRow(outexprEnc
            .resolveAndBind(getAttributes(outputEncoder.schema))
            .fromRow(jcudaIterator.next().copy()))
      }
    }
  }
}

object MAPGPU
{
  def apply[T: Encoder, U : Encoder](
                                      func: DSCUDAFunction,
                                      args : Array[Any],
                                      outputArraySizes: Array[Int],
                                      child: LogicalPlan) : LogicalPlan = {
    val deserialized = CatalystSerde.deserialize[T](child)
    val mapped = MAPGPU(
      func, args, outputArraySizes,
      deserialized,
      implicitly[Encoder[T]],
      implicitly[Encoder[U]],
      CatalystSerde.generateObjAttr[U]
    )

    CatalystSerde.serialize[U](mapped)
  }
}

case class MAPGPU[T: Encoder, U : Encoder](func: DSCUDAFunction,
                                           args : Array[Any],
                                           outputArraySizes: Array[Int],
                                           child: LogicalPlan,
                                           inputEncoder: Encoder[T], outputEncoder: Encoder[U],
                                           outputObjAttr: Attribute)
  extends ObjectConsumer with ObjectProducer {
  override def otherCopyArgs : Seq[AnyRef] = inputEncoder :: outputEncoder ::  Nil
}

object GPUOperators extends Strategy {
  private val DScache = GPUSparkEnv.get.gpuMemoryManager.cachedGPUDS

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case MAPGPU(cf, args, outputArraySizes, child,inputEncoder, outputEncoder, outputObjAttr) =>

      // cached possible values : 0 - NoCache; 1 - plan is cached; 2 - child plan is cached;
      var cached = if (DScache.contains(plan)) 1 else 0
      cached = cached | (if (DScache.contains(child)) 2 else 0)

      // get access the GpuPtr's HashMap for the corresponding plan's
      val gpuPtrs = new Array[mutable.HashMap[String, CUdeviceptr]](2)
      gpuPtrs(0) = GPUSparkEnv.get.gpuMemoryManager.getCachedGPUPointersDS.getOrElse(plan, null)
      gpuPtrs(1) = GPUSparkEnv.get.gpuMemoryManager.getCachedGPUPointersDS.getOrElse(child, null)

      MAPGPUExec(cf, args, outputArraySizes, planLater(child),
        inputEncoder, outputEncoder, outputObjAttr, cached, gpuPtrs) :: Nil
    case _ => Nil
  }
}

case class DSCUDAFunction(
                           funcName: String,
                           _inputColumnsOrder: Seq[String] = null,
                           _outputColumnsOrder: Seq[String] = null,
                           resource: Any,
                           stagesCount: Option[Long => Int] = None,
                           dimensions: Option[(Long, Int) => (Int, Int)] = None,
                           outputSize: Option[Long] = None
                         )

/**
  * Adds additional functionality to existing Dataset/DataFrame's which are
  * specific to performing computation on Nvidia GPU's attached
  * to executors. To use these additional functionality import
  * the following packages,
  *
  * {{{
  * import com.ibm.gpuenabler.cuda._
  * import com.ibm.gpuenabler.CUDADSImplicits._
  * }}}
  *
  */
object CUDADSImplicits {

  implicit class CUDADSFuncs[T: Encoder](ds: _ds[T]) {

    def mapExtFunc[U:Encoder](func: T => U,
                          cf: DSCUDAFunction,
                          args: Array[Any],
                          outputArraySizes: Array[Int] = Array.empty): Dataset[U] =  {

      DS[U](ds.sparkSession,
          MAPGPU[T, U](cf, args, outputArraySizes,
            getLogicalPlan(ds)))
    }

    def reduceExtFunc(func: (T, T) => T,
                          cf: DSCUDAFunction,
                          args: Array[Any],
                          outputArraySizes: Array[Int] = Array.empty): T =  {

      val ds1 = DS[T](ds.sparkSession,
        MAPGPU[T, T](cf, args, outputArraySizes,
          getLogicalPlan(ds)))

      ds1.reduce(func)

    }

    def cacheGPU(): Dataset[T] = {
      val logPlan = ds.queryExecution.optimizedPlan transform {
        case SerializeFromObject(_, lp) => lp
      }

      GPUSparkEnv.get.gpuMemoryManager.cacheGPUSlaves(logPlan)
      ds
    }

    def uncacheGPU(): Dataset[T] = {
      val logPlan = ds.queryExecution.optimizedPlan transform {
        case SerializeFromObject(_, lp) => lp
      }

      GPUSparkEnv.get.gpuMemoryManager.unCacheGPUSlaves(logPlan)
      ds
    }

    ds.sparkSession.experimental.extraStrategies = GPUOperators :: Nil
  }
}
