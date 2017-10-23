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
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.gpuenabler.CUDAUtils

case class MAPGPUExec[T, U](cpf: AnyRef ,cf: DSCUDAFunction, constArgs : Array[Any],
                            outputArraySizes: Array[Int], partSizes: Broadcast[Map[Int, Int]],
                            child: SparkPlan,
                            inputEncoder: Encoder[T], outputEncoder: Encoder[U],
                            outputObjAttr: Attribute,
                            logPlans: Array[String])
  extends ObjectConsumerExec with ObjectProducerExec  {

  lazy val inputSchema: StructType = inputEncoder.schema
  lazy val outputSchema: StructType = outputEncoder.schema

  override def output: Seq[Attribute] = outputObjAttr :: Nil
  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext,
      "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    val outexprEnc = outputEncoder.asInstanceOf[ExpressionEncoder[U]]
    val outEnc = outexprEnc
      .resolveAndBind(getAttributes(outputEncoder.schema))

    val childRDD = child.execute()
    var skipExecution = false

    // handle special case of loadGPU; Since data is already in GPU, do nothing
    if (cf.funcName == "") {
      skipExecution = true
    }
    //Check if GPU is available or Enabled on the Executor Node
    if(GPUSparkEnv.get.isGPUEnabled) {
      childRDD.mapPartitionsWithIndex{ (partNum, iter) =>

        // Differentiate cache by setting:
        // cached: 1 -> this logical plan is cached;
        // cached: 2 -> child logical plan is cached;
        // cached: 0 -> NoCache;
        // cached: 4 -> this logical plan is cached; data resides only in GPU ;
        val DScache = GPUSparkEnv.get.gpuMemoryManager.cachedGPUDS
        var cached = if (DScache.contains(logPlans(0))) 1 else 0
        cached |= (if(DScache.contains(logPlans(1))) 2 else 0)
        cached |= (if(GPUSparkEnv.get.gpuMemoryManager.cachedGPUOnlyDS.contains(logPlans(0))) 4 else 0)

        // Generate the JCUDA program to be executed and obtain the iterator object
        val jcudaIterator = JCUDACodeGen.generate(inputSchema,
          outputSchema,cf,constArgs, outputArraySizes)
        val list = new mutable.ListBuffer[InternalRow]

        // Get hold of hashmap for this Plan to store the GPU pointers from output parameters
        // cached: 1 -> this logical plan is cached; 2 -> child logical plan is cached
        val curPlanPtrs: java.util.Map[String, CachedGPUMeta] = if ((cached & 1) > 0) {
          logDebug("current plan is cached")
          val partPtr = GPUSparkEnv.get.gpuMemoryManager.getCachedGPUPointersDS.getOrElse(logPlans(0), null)
          if (partPtr != null) {
            partPtr.getOrElseUpdate(partNum.toLong, {
              logDebug("no cached ptrs for current plan ")
              new ConcurrentHashMap[String, CachedGPUMeta].asScala
            }).asJava
          } else {
            null
          }
        } else {
          Map[String, CachedGPUMeta]().asJava
        }

        val childPlanPtrs: java.util.Map[String, CachedGPUMeta] =  if ((cached & 2) > 0) {
          logDebug("child plan is cached")
          val partPtr = GPUSparkEnv.get.gpuMemoryManager.getCachedGPUPointersDS.getOrElse(logPlans(1), null)
          if (partPtr != null) {
            partPtr.getOrElseUpdate(partNum.toLong, {
              logDebug("no cached ptr for child plan ")
              new ConcurrentHashMap[String, CachedGPUMeta].asScala
            }).asJava
          } else {
            null
          }
        } else {
          Map[String, CachedGPUMeta]().asJava
        }

        val imgpuPtrs: java.util.List[java.util.Map[String, CachedGPUMeta]] =
          List(curPlanPtrs, childPlanPtrs).asJava
        
        var dataSize = 0
        if (!skipExecution) {
          dataSize = partSizes.value.getOrElse(partNum, 1)
          if (dataSize == 0) {
            skipExecution = true;
          }
        }

        if (!skipExecution) {
          // Generate the JCUDA program to be executed and obtain the iterator object
          val jcudaIterator = JCUDACodeGen.generate(inputSchema,
            outputSchema,cf,constArgs, outputArraySizes)

          //assert(dataSize > 0)

          // Compute the GPU Grid Dimensions based on the input data size
          // For user provided Dimensions; retrieve it along with the
          // respective stage information.
          val (stages, userGridSizes, userBlockSizes, sharedMemory) =
          JCUDACodeGen.getUserDimensions(cf, dataSize)

          // Initialize the auto generated code's iterator
          jcudaIterator.init[T](iter.asJava, constArgs,
            dataSize, cached, imgpuPtrs, partNum,
            userGridSizes, userBlockSizes, stages, sharedMemory, inputEncoder)

          // Triggers execution
          jcudaIterator.hasNext()

          val outEnc = outexprEnc
            .resolveAndBind(getAttributes(outputEncoder.schema))

          new Iterator[InternalRow] {
            override def hasNext: Boolean = jcudaIterator.hasNext()

            override def next: InternalRow =
              InternalRow(outEnc
                .fromRow(jcudaIterator.next()))
          }
        } else {
          new Iterator[InternalRow] {
            override def hasNext: Boolean = false

            override def next: InternalRow =
              InternalRow(outEnc
                .fromRow(null))
          }
        }
      }
    }
      //else part is executed if the GPU is not Available or Enabled
    else {
      if (!skipExecution) {
        val callFunc: Any => Any = cpf match {
          case m: MapFunction[_, _] => i => m.asInstanceOf[MapFunction[Any, Any]].call(i)
          case f: scala.Function1[_, _] => cpf.asInstanceOf[Any => Any]
          case _ => null
        }

        val reduceCallFunc: (Any, Any) => Any = cpf match {
          case m: scala.Function2[_, _, _] => m.asInstanceOf[(Any, Any) => Any]
          case _ => null
        }

        if (callFunc != null) {
          childRDD.mapPartitions { iter =>
            val getObject = ObjectOperator.unwrapObjectFromRow(child.output.head.dataType)
            val outputObject = ObjectOperator.wrapObjectToRow(outputObjAttr.dataType)
            iter.map(row => outputObject(callFunc(getObject(row))))
          }
        } else {
          assert(reduceCallFunc != null, "Unknown Function Type")
          val outEnc = outexprEnc.resolveAndBind(getAttributes(outputEncoder.schema))

          //Return RDD
          childRDD
        }
      } else
        childRDD
    }
  }
}

object MAPGPU
{
  def apply[T: Encoder, U : Encoder]( cpfunc: AnyRef,
                                      func: DSCUDAFunction,
                                      args : Array[Any],
                                      outputArraySizes: Array[Int],
                                      partSizes: Broadcast[Map[Int, Int]],
                                      child: LogicalPlan) : LogicalPlan = {
    val deserialized = CatalystSerde.deserialize[T](child)
    val mapped = MAPGPU(cpfunc, func, args, outputArraySizes, partSizes,
      deserialized,
      implicitly[Encoder[T]],
      implicitly[Encoder[U]],
      CatalystSerde.generateObjAttr[U]
    )

    CatalystSerde.serialize[U](mapped)
  }
}


object LOADGPU
{
  def apply[T: Encoder](partSizes: Broadcast[Map[Int, Int]], child: LogicalPlan) : LogicalPlan = {
    val deserialized = CatalystSerde.deserialize[T](child)
    val mapped = LOADGPU(
      partSizes,
      deserialized,
      implicitly[Encoder[T]],
      CatalystSerde.generateObjAttr[T]
    )

    CatalystSerde.serialize[T](mapped)
  }
}

case class LOADGPU[T: Encoder](partSizes: Broadcast[Map[Int, Int]], child: LogicalPlan,
                               inputEncoder: Encoder[T],
                               outputObjAttr: Attribute)
  extends ObjectConsumer with ObjectProducer {
  override def otherCopyArgs : Seq[AnyRef] =
    inputEncoder ::  Nil
}

case class MAPGPU[T: Encoder, U : Encoder](cpfunc: AnyRef, func: DSCUDAFunction,
			   args : Array[Any],
			   outputArraySizes: Array[Int], partSizes: Broadcast[Map[Int, Int]],
			   child: LogicalPlan,
			   inputEncoder: Encoder[T], outputEncoder: Encoder[U],
			   outputObjAttr: Attribute)
  extends ObjectConsumer with ObjectProducer {
  override def otherCopyArgs : Seq[AnyRef] =
				inputEncoder :: outputEncoder ::  Nil
}

object GPUOperators extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case MAPGPU(cpf,cf, args, outputArraySizes, partSizes, child,inputEncoder, outputEncoder,
         outputObjAttr) =>
      // Store the logical plan UID and pass it to physical plan as 
      // cached it done with logical plan UID.
      val logPlans = new Array[String](2)
      val modChildPlan = child match {
        case DeserializeToObject(_, _, lp) =>
          lp
	case _ => child
      }

      logPlans(0) = md5HashObj(plan)
      logPlans(1) = md5HashObj(modChildPlan)

      if (GPUSparkEnv.isAutoCacheEnabled) {
        logInfo(s"Optimize : Enable caching for child of logicalplan(${cf.funcName}) : ${logPlans(1)}")
        GPUSparkEnv.get.gpuMemoryManager.cacheGPUSlavesAuto(logPlans(1))
      }

      MAPGPUExec(cpf,cf, args, outputArraySizes, partSizes, planLater(child),
        inputEncoder, outputEncoder, outputObjAttr, logPlans) :: Nil
    case LOADGPU(partSizes, child, inputEncoder, outputObjAttr) =>
      val logPlans = new Array[String](2)
      val modChildPlan = child match {
        case DeserializeToObject(_, _, lp) => lp
        case _ => child
      }
      logPlans(0) = md5HashObj(plan)
      logPlans(1) = md5HashObj(modChildPlan)

      val cf = DSCUDAFunction("",null,null,"")
      MAPGPUExec(null , cf, null, null, partSizes, planLater(child),
        inputEncoder, inputEncoder, outputObjAttr, logPlans) :: Nil
    case _ => Nil
  }
}

/**
  * gpuParameters: This case class is used to describe the GPU Parameters
  * such as dimensions and shared Memory size which is optional.
  * @param dimensions : Dimensions should be Given in the following format
  *                     GridSizeX, BlockSizeX, GridSizeY, BlockSizeY, GridSizeZ, BlockSizeZ.
  * @param sharedMemorySize : SharedMemorySize to be used in Bytes.
  *                           It will be validated with the sharedMemorySize of the GPU.
  */
case class gpuParameters (
                        dimensions: (Long, Int) => (Int, Int, Int, Int, Int, Int),
                        sharedMemorySize: Option[Int] = None
                      )
		      
/**
  * DSCUDAFunction: This case class is used to describe the CUDA kernel and 
  *   maps the i/o parameters to the DataSet's column name on which this 
  *   function is applied. Stages & Dimensions can be specified. If the
  *   kernel is going to perform a reduce kind of operation, the output size
  *   will be different from the input size, so it must be provided by the user
  */
case class DSCUDAFunction(
                           funcName: String,
                           _inputColumnsOrder: Seq[String] = null,
                           _outputColumnsOrder: Seq[String] = null,
                           resource: Any,
                           stagesCount: Option[Long => Int] = None,
                           gpuParams: Option[gpuParameters] = None,
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
  val cachedPartitionSizes = new mutable.HashMap[String, Broadcast[Map[Int, Int]]]
  implicit class CUDADSFuncs[T: Encoder](ds: _ds[T]) extends Serializable {
  /** 
    * getPartSizes: Helper routine to get Partition Size & add broadcast it.
    * Getting the partition size right before MAPGPU operation 
    * improves performance.
    */
    def getPartSizes: Broadcast[Map[Int, Int]] = {
      val execPlan = ds.queryExecution.executedPlan
      val logPlan = ds.queryExecution.logical match {
        case SerializeFromObject(_, lp) => lp
        case _ => ds.queryExecution.logical
      }

      val partSizes: Broadcast[Map[Int, Int]] = logPlan match {
        case MAPGPU(_ ,_, _, _, partSize, _, _, _, _) =>
          partSize
        case _ =>
          val execPlan: SparkPlan = ds.queryExecution.executedPlan
          cachedPartitionSizes.getOrElseUpdate(md5HashObj(execPlan), {
            val partSize: Map[Int, Int] = execPlan.execute().mapPartitionsWithIndex {
              (partNum, iter) => Iterator(Map(partNum -> iter.length))
            }.reduce(_ ++ _)
            val broadcastpartsize = ds.sparkSession.sparkContext.broadcast(partSize)
            cachedPartitionSizes += md5HashObj(execPlan) -> broadcastpartsize
            broadcastpartsize
          })
      }
      partSizes
    }

    /**
      * Return a new Dataset by applying a function to all elements of this Dataset.
      *
      * @param func  Specify the lambda to apply to all elements of this Dataset
      * @param cf  Provide the ExternalFunction instance which points to the
      *                 GPU native function to be executed for each element in
      *                 this Dataset
      * @param args Specify a list of free variable that need to be
      *                           passed in to the GPU kernel function, if any
      * @param outputArraySizes If the expected result is an array folded in a linear
      *                         form, specific a sequence of the array length for every
      *                         output columns
      * @tparam U Result Dataset type
      * @return Return a new Dataset of type U after executing the user provided
      *         GPU function on all elements of this Dataset
      */
    def mapExtFunc[U:Encoder](func: T => U,
                          cf: DSCUDAFunction,
                          args: Array[Any] = Array.empty,
                          outputArraySizes: Array[Int] = Array.empty): Dataset[U] =  {

      val cleanF = CUDAUtils.cleanFn(ds.sparkSession.sparkContext, func)
      DS[U](ds.sparkSession,
          MAPGPU[T, U](cleanF,cf, args, outputArraySizes, getPartSizes,
            getLogicalPlan(ds)))
    }

    /**
      * Trigger a reduce action on all elements of this Dataset.
      *
      * @param func Specify the lambda to apply to all elements of this Dataset
      * @param cf Provide the DSCUDAFunction instance which points to the
      *                 GPU native function to be executed for each element in
      *                 this Dataset
      * @param args Specify a list of free variable that need to be
      *                           passed in to the GPU kernel function, if any
      * @param outputArraySizes If the expected result is an array folded in a linear
      *                         form, specific a sequence of the array length for every
      *                         output columns
      * @return Return the result after performing a reduced operation on all
      *         elements of this Dataset
      */
    def reduceExtFunc(func: (T, T) => T,
                          cf: DSCUDAFunction,
                          args: Array[Any] = Array.empty,
                          outputArraySizes: Array[Int] = Array.empty): T =  {

      val cleanF = CUDAUtils.cleanFn(ds.sparkSession.sparkContext, func)

      val ds1 = DS[T](ds.sparkSession,
        MAPGPU[T, T](cleanF,cf, args, outputArraySizes, getPartSizes,
          getLogicalPlan(ds)))

      ds1.reduce(cleanF)
    }

    /**
      * Load & Cache the partitions of the Dataset in GPU.
      *
      * @return Returns the same Dataset after performing the operation
      */
    def loadGpu(): Dataset[T] =  {
      // Enable Caching on the current Dataset
      val logPlan = ds.queryExecution.optimizedPlan match {
        case SerializeFromObject(_, lp) => lp
        case _ => ds.queryExecution.optimizedPlan
      }
      GPUSparkEnv.get.gpuMemoryManager.cacheGPUSlaves(md5HashObj(logPlan))
        // Create a new Dataset to load the data into GPU
        val ds1 = DS[T](ds.sparkSession,
          LOADGPU[T](getPartSizes, getLogicalPlan(ds)))

        // trigger an action
        ds1.count()
        ds1
    }

    /**
      * Mark the Dataset's partitions to be cached in GPU.
      * Unmarked Dataset partitions will be cleaned up on every Job Completion.
      *
      * @param onlyGPU Boolean value to indicate partitions will be used only inside GPU
      *                so that copy from GPU to Host will be skipped during Job execution.
      *                Boost performance but to be used with caution on need basis.
      * @return Returns the same Dataset after performing the operation
      */
    def cacheGpu(onlyGPU: Boolean = false): Dataset[T] = {
      val logPlan = ds.queryExecution.optimizedPlan match {
        case SerializeFromObject(_, lp) => lp
	      case _ => ds.queryExecution.optimizedPlan
      }
      GPUSparkEnv.get.gpuMemoryManager.cacheGPUSlaves(md5HashObj(logPlan), onlyGPU)
      ds
    }

    /**
      * Unloads the Dataset's partitions that were cached earlier in GPU.
      *
      * @return Returns the same Dataset after performing the operation
      */
    def unCacheGpu(): Dataset[T] = {
      val logPlan = ds.queryExecution.optimizedPlan match {
        case SerializeFromObject(_, lp) => lp
	      case _ => ds.queryExecution.optimizedPlan
      }
  
      GPUSparkEnv.get.gpuMemoryManager.unCacheGPUSlaves(md5HashObj(logPlan))
      ds
    }

    ds.sparkSession.experimental.extraStrategies = GPUOperators :: Nil
  }
}


