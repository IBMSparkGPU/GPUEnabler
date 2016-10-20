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

import java.io._
import java.net.URL

import jcuda.Pointer
import jcuda.driver.JCudaDriver._
import jcuda.driver.{CUdeviceptr, CUfunction, CUstream}
import jcuda.runtime.{JCuda, cudaStream_t}
import org.apache.commons.io.IOUtils
import org.apache.spark._
import org.apache.spark.storage.{BlockId, RDDBlockId}

import scala.language.existentials
import scala.reflect.ClassTag
import scala.collection.JavaConverters._
import scala.language.implicitConversions
import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2, _}

/**
  * An abstract class to represent a ''User Defined function'' from a Native GPU program.
  */
abstract class ExternalFunction extends Serializable {
 def compute[U : ClassTag, T : ClassTag](inp: HybridIterator[T],
                                         columnSchemas: Seq[ColumnPartitionSchema],
                                         outputSize: Option[Int] = None,
                                         outputArraySizes: Seq[Int] = null,
                                         inputFreeVariables: Seq[Any] = null,
                                         blockId : Option[BlockId] = None) : Iterator[U]

  def inputColumnsOrder(): Seq[String]

  def outputColumnsOrder(): Seq[String]
}

/**
  *   * A class to represent a ''User Defined function'' from a Native GPU program.
  *   * Wrapper Java function for CUDAFunction scala function
  *
  * Specify the `funcName`, `_inputColumnsOrder`, `_outputColumnsOrder`,
  * and `resourceURL` when creating a new `CUDAFunction`,
  * then pass this object as an input argument to `mapExtFunc` or
  *  `reduceExtFunc` as follows,
  *
  * {{{
  *
  * JavaCUDAFunction mapFunction = new JavaCUDAFunction(
  *              "multiplyBy2",
  *              Arrays.asList("this"),
  *              Arrays.asList("this"),
  *              ptxURL);
  *
  *   JavaRDD<Integer> inputData = sc.parallelize(range).cache();
  *   ClassTag<Integer> tag = scala.reflect.ClassTag$.MODULE$.apply(Integer.TYPE);
  *   JavaCUDARDD<Integer> ci = new JavaCUDARDD(inputData.rdd(), tag);
  *   
  *   JavaCUDARDD<Integer> output = ci.mapExtFunc((new Function<Integer, Integer>() {
  *          public Integer call(Integer x) {
  *              return (2 * x);
  *          }
  *    }), mapFunction, tag)
  *
  * }}}
  *
  * @constructor The "compute" method is initialized so that when invoked it will
  *             load and launch the GPU kernel with the required set of parameters
  *             based on the input & output column order.
  * @param funcName Name of the Native code's function
  * @param _inputColumnsOrder List of input columns name mapping to corresponding
  *                           class members of the input RDD.
  * @param _outputColumnsOrder List of output columns name mapping to corresponding
  *                            class members of the result RDD.
  * @param resourceURL  Points to the resource URL where the GPU kernel is present
  * @param constArgs  Sequence of constant argument that need to passed in to a
  *                   GPU Kernel
  * @param stagesCount  Provide a function which is used to determine the number
  *                     of stages required to run this GPU kernel in spark based on the
  *                     number of partition items to process. Default function return "1".
  * @param dimensions Provide a function which is used to determine the GPU compute
  *                   dimensions for each stage. Default function will determined the
  *                   dimensions based on the number of partition items but for a single
  *                   stage.
  */
class JavaCUDAFunction(val funcName: String,
                       val _inputColumnsOrder: java.util.List[String] = null,
                       val _outputColumnsOrder: java.util.List[String] = null,
                       val resourceURL: URL,
                       val constArgs: Seq[AnyVal] = Seq(),
                       val stagesCount: Option[JFunction[Long, Integer]] = null,
                       val dimensions: Option[JFunction2[Long, Integer, Tuple2[Integer,Integer]]] = null) 
    extends Serializable {

  implicit def toScalaTuples(x: Tuple2[Integer,Integer]) : Tuple2[Int,Int] = (x._1, x._2)

  implicit def toScalaFunction(fun: JFunction[Long, Integer]):
    Option[Long => Int] = if (fun != null)
      Some(x => fun.call(x))
    else None

  implicit def toScalaFunction(fun: JFunction2[Long, Integer, Tuple2[Integer, Integer]]):
    Option[(Long, Int) => Tuple2[Int, Int]] =  if (fun != null)
      Some((x, y) => fun.call(x, y))
    else None

  val stagesCountFn: Option[Long => Int]  = stagesCount match {
    case Some(fun: JFunction[Long, Integer]) => fun
    case _ => None
  }

  val dimensionsFn: Option[(Long, Int) => Tuple2[Int, Int]] = dimensions match {
    case Some(fun: JFunction2[Long, Integer, Tuple2[Integer, Integer]] ) => fun
    case _ => None
  }

  val cf = new CUDAFunction(funcName, _inputColumnsOrder.asScala, _outputColumnsOrder.asScala,
    resourceURL, constArgs, stagesCountFn, dimensionsFn)
  
  /* 
   * 3 variants - call invocations
   */
  def this(funcName: String, _inputColumnsOrder: java.util.List[String],
          _outputColumnsOrder: java.util.List[String],
          resourceURL: URL) =
    this(funcName, _inputColumnsOrder, _outputColumnsOrder,
      resourceURL, Seq(), None, None)

  def this(funcName: String, _inputColumnsOrder: java.util.List[String],
           _outputColumnsOrder: java.util.List[String],
           resourceURL: URL, constArgs: Seq[AnyVal]) =
    this(funcName, _inputColumnsOrder, _outputColumnsOrder,
    resourceURL, constArgs, None, None)

  def this(funcName: String, _inputColumnsOrder: java.util.List[String],
           _outputColumnsOrder: java.util.List[String],
           resourceURL: URL, constArgs: Seq[AnyVal],
            stagesCount: Option[JFunction[Long, Integer]]) =
    this(funcName, _inputColumnsOrder, _outputColumnsOrder,
    resourceURL, Seq(), stagesCount, None)
}


/**
  * A class to represent a ''User Defined function'' from a Native GPU program.
  *
  * Specify the `funcName`, `_inputColumnsOrder`, `_outputColumnsOrder`,
  * and `resourceURL` when creating a new `CUDAFunction`,
  * then pass this object as an input argument to `mapExtFunc` or
  *  `reduceExtFunc` as follows,
  *
  * {{{
  *     val mapFunction = new CUDAFunction(
  *           "multiplyBy2",
  *           Array("this"),
  *           Array("this"),
  *           ptxURL)
  *
  *   val output = sc.parallelize(1 to n, 1)
  *       .mapExtFunc((x: Int) => 2 * x, mapFunction)
  *
  * }}}
  *
  * @constructor The "compute" method is initialized so that when invoked it will
  *             load and launch the GPU kernel with the required set of parameters
  *             based on the input & output column order.
  * @param funcName Name of the Native code's function
  * @param _inputColumnsOrder List of input columns name mapping to corresponding
  *                           class members of the input RDD.
  * @param _outputColumnsOrder List of output columns name mapping to corresponding
  *                            class members of the result RDD.
  * @param resourceURL  Points to the resource URL where the GPU kernel is present
  * @param constArgs  Sequence of constant argument that need to passed in to a
  *                   GPU Kernel
  * @param stagesCount  Provide a function which is used to determine the number
  *                     of stages required to run this GPU kernel in spark based on the
  *                     number of partition items to process. Default function return "1".
  * @param dimensions Provide a function which is used to determine the GPU compute
  *                   dimensions for each stage. Default function will determined the
  *                   dimensions based on the number of partition items but for a single
  *                   stage.
  */
class CUDAFunction(
                    val funcName: String,
                    val _inputColumnsOrder: Seq[String] = null,
                    val _outputColumnsOrder: Seq[String] = null,
                    val resource: Any,
                    val constArgs: Seq[AnyVal] = Seq(),
                    val stagesCount: Option[Long => Int] = None,
                    val dimensions: Option[(Long, Int) => (Int, Int)] = None
                   )
  extends ExternalFunction {
  implicit def toScalaFunction(fun: JFunction[Long, Int]): Long => Int = x => fun.call(x)

  implicit def toScalaFunction(fun: JFunction2[Long, Int, Tuple2[Int,Int]]): (Long, Int) => 
    Tuple2[Int,Int] = (x, y) => fun.call(x, y)

  def inputColumnsOrder: Seq[String] = _inputColumnsOrder
  def outputColumnsOrder: Seq[String] = _outputColumnsOrder

  var _blockId: Option[BlockId] = Some(RDDBlockId(0, 0))

  //touch GPUSparkEnv for endpoint init
  GPUSparkEnv.get
  val ptxmodule = resource match {
    case resourceURL: URL =>
      (resourceURL.toString, {
        val inputStream = resourceURL.openStream()
        val moduleBinaryData = IOUtils.toByteArray(inputStream)
        inputStream.close()
        new String(moduleBinaryData.map(_.toChar))
      })
    case (name: String, ptx: String) => (name, ptx)
    case _ => throw new UnsupportedOperationException("this type is not supported for module")
  }

  // asynchronous Launch of kernel
  private def launchKernel(function: CUfunction, numElements: Int,
                           kernelParameters: Pointer,
                           dimensions: Option[(Long, Int) => (Int, Int)] = None,
                           stageNumber: Int = 1,
                           cuStream: CUstream) = {

    val (gpuGridSize, gpuBlockSize) = dimensions match {
      case Some(computeDim) => computeDim(numElements, stageNumber)
      case None => GPUSparkEnv.get.cudaManager.computeDimensions(numElements)
    }

    cuLaunchKernel(function,
      gpuGridSize, 1, 1,  // how many blocks
      gpuBlockSize, 1, 1, // threads per block (eg. 1024)
      0, cuStream, // Shared memory size and stream
      kernelParameters, null // Kernel- and extra parameters
    )
  }

  def createkernelParameterDesc2(a: Any, cuStream: CUstream):
        Tuple5[_, _, CUdeviceptr, Pointer, _] = {
    val (arr, hptr, devPtr, gptr, sz) = a match {
      case h if h.isInstanceOf[Int] => {
        val arr = Array.fill(1)(a.asInstanceOf[Int])
        val devPtr = GPUSparkEnv.get.cudaManager.allocateGPUMemory(INT_COLUMN.bytes)
        cuMemcpyHtoDAsync(devPtr, Pointer.to(arr), INT_COLUMN.bytes, cuStream)
        (arr, Pointer.to(arr), devPtr, Pointer.to(devPtr), INT_COLUMN.bytes)
      }
      case h if h.isInstanceOf[Byte] => {
        val arr = Array.fill(1)(a.asInstanceOf[Byte])
        val devPtr = GPUSparkEnv.get.cudaManager.allocateGPUMemory(BYTE_COLUMN.bytes)
        cuMemcpyHtoDAsync(devPtr, Pointer.to(arr), BYTE_COLUMN.bytes, cuStream)
        (arr, Pointer.to(arr), devPtr, Pointer.to(devPtr), BYTE_COLUMN.bytes)
      }
      case h if h.isInstanceOf[Short] => {
        val arr = Array.fill(1)(a.asInstanceOf[Short])
        val devPtr = GPUSparkEnv.get.cudaManager.allocateGPUMemory(SHORT_COLUMN.bytes)
        cuMemcpyHtoDAsync(devPtr, Pointer.to(arr), SHORT_COLUMN.bytes, cuStream)
        (arr, Pointer.to(arr), devPtr, Pointer.to(devPtr), SHORT_COLUMN.bytes)
      }
      case h if h.isInstanceOf[Long] => {
        val arr = Array.fill(1)(a.asInstanceOf[Long])
        val devPtr = GPUSparkEnv.get.cudaManager.allocateGPUMemory(LONG_COLUMN.bytes)
        cuMemcpyHtoDAsync(devPtr, Pointer.to(arr), LONG_COLUMN.bytes, cuStream)
        (arr, Pointer.to(arr), devPtr, Pointer.to(devPtr), LONG_COLUMN.bytes)
      }
      case h if h.isInstanceOf[Double] => {
        val arr = Array.fill(1)(a.asInstanceOf[Double])
        val devPtr = GPUSparkEnv.get.cudaManager.allocateGPUMemory(DOUBLE_COLUMN.bytes)
        cuMemcpyHtoDAsync(devPtr, Pointer.to(arr), DOUBLE_COLUMN.bytes, cuStream)
        (arr, Pointer.to(arr), devPtr, Pointer.to(devPtr), DOUBLE_COLUMN.bytes)
      }
      case h if h.isInstanceOf[Float] => {
        val arr = Array.fill(1)(a.asInstanceOf[Float])
        val devPtr = GPUSparkEnv.get.cudaManager.allocateGPUMemory(FLOAT_COLUMN.bytes)
        cuMemcpyHtoDAsync(devPtr, Pointer.to(arr), FLOAT_COLUMN.bytes, cuStream)
        (arr, Pointer.to(arr), devPtr, Pointer.to(devPtr), FLOAT_COLUMN.bytes)
      }
      case h if h.isInstanceOf[Array[Double]] => {
        val arr = h.asInstanceOf[Array[Double]]
        val sz = h.asInstanceOf[Array[Double]].length * DOUBLE_COLUMN.bytes
        val devPtr = GPUSparkEnv.get.cudaManager.allocateGPUMemory(sz)
        cuMemcpyHtoDAsync(devPtr, Pointer.to(arr), sz, cuStream)
        (arr, Pointer.to(arr), devPtr, Pointer.to(devPtr), sz)
      }
      case h if h.isInstanceOf[Array[Int]] => {
        val arr = h.asInstanceOf[Array[Int]]
        val sz = h.asInstanceOf[Array[Int]].length * INT_COLUMN.bytes
        val devPtr = GPUSparkEnv.get.cudaManager.allocateGPUMemory(sz)
        cuMemcpyHtoDAsync(devPtr, Pointer.to(arr), sz, cuStream)
        (arr, Pointer.to(arr), devPtr, Pointer.to(devPtr), sz)
      }
    }
    (arr, hptr, devPtr, gptr, sz)
  }

  /**
    *  This function is invoked from RDD `compute` function and it load & launches
    *  the GPU kernel and performs the computation on GPU and returns the results
    *  in an iterator.
    *
    * @param inputHyIter Provide the HybridIterator instance
    * @param columnSchemas Provide the input and output column schema
    * @param outputSize Specify the number of expected result. Default is equal to
    *                   the number of element in that partition/
    * @param outputArraySizes If the expected result is an array folded in a linear
    *                         form, specific a sequence of the array length for every
    *                         output columns
    * @param inputFreeVariables Specify a list of free variable that need to be
    *                           passed in to the GPU kernel function, if any
    * @param blockId  Specify the block ID associated with this operation
    * @tparam U Output Iterator's type
    * @tparam T Input Iterator's type
    * @return Returns an iterator of type U
    */
  def compute[U: ClassTag, T: ClassTag](inputHyIter: HybridIterator[T],
                                        columnSchemas: Seq[ColumnPartitionSchema],
                                        outputSize: Option[Int] = None,
                                        outputArraySizes: Seq[Int] = null,
                                        inputFreeVariables: Seq[Any] = null,
                                        blockId: Option[BlockId] = None): Iterator[U] = {
    val module = GPUSparkEnv.get.cudaManager.cachedLoadModule(Right(ptxmodule))
    val function = new CUfunction
    cuModuleGetFunction(function, module, funcName)

    val stream = new cudaStream_t
    JCuda.cudaStreamCreateWithFlags(stream, JCuda.cudaStreamNonBlocking)
    val cuStream = new CUstream(stream)

    _blockId = blockId

    val inputColumnSchema = columnSchemas(0)
    val outputColumnSchema = columnSchemas(1)

    // Ensure the GPU is loaded with the same data in memory
    inputHyIter.copyCpuToGpu

    var listDevPtr: List[CUdeviceptr] = null

    // hardcoded first argument
    val (arr, hptr, devPtr: CUdeviceptr, gptr, sz) = createkernelParameterDesc2(inputHyIter.numElements, cuStream)

    listDevPtr = List(devPtr)
    // size + input Args based on inputColumnOrder + constArgs
    var kp: List[Pointer] = List(gptr) ++ inputHyIter.listKernParmDesc.map(_.gpuPtr)

    val outputHyIter = new HybridIterator[U](null, outputColumnSchema,
      outputColumnsOrder, blockId,
      outputSize.getOrElse(inputHyIter.numElements), outputArraySizes = outputArraySizes)

    kp = kp ++ outputHyIter.listKernParmDesc.map(_.gpuPtr)

    // add additional user input parameters
    if (inputFreeVariables != null) {
      val inputFreeVarPtrs = inputFreeVariables.map { inputFreeVariable =>
        createkernelParameterDesc2(inputFreeVariable, cuStream)
      }
      kp = kp ++ inputFreeVarPtrs.map(_._4) // gptr
      listDevPtr = listDevPtr ++ inputFreeVarPtrs.map(_._3) // CUdeviceptr
    }

    // add outputArraySizes to the list of arguments
    if (outputArraySizes != null ) {
      val outputArraySizes_kpd = createkernelParameterDesc2(outputArraySizes.toArray, cuStream)
      kp = kp ++ Seq(outputArraySizes_kpd._4) // gpuPtr
      listDevPtr = listDevPtr ++ List(outputArraySizes_kpd._3) // CUdeviceptr
    }

    // add user provided constant variables
    if (constArgs != null) {
      val inputConstPtrs = constArgs.map { constVariable =>
        createkernelParameterDesc2(constVariable, cuStream)
      }
      kp = kp ++ inputConstPtrs.map(_._4) // gpuPtr
      listDevPtr = listDevPtr ++ inputConstPtrs.map(_._3) // CUdeviceptr
    }

    stagesCount match {
      // normal launch, no stages, suitable for map

      case None =>
        val kernelParameters = Pointer.to(kp: _*)
        // Start the GPU execution with the populated kernel parameters
        launchKernel(function, inputHyIter.numElements, kernelParameters, dimensions, 1, cuStream)

      // launch kernel multiple times (multiple stages), suitable for reduce
      case Some(totalStagesFun) =>
        val totalStages = totalStagesFun(inputHyIter.numElements)
        if (totalStages <= 0) {
          throw new SparkException("Number of stages in a kernel launch must be positive")
        }

        // preserve the kernel parameter list so as to use it for every stage.
        val preserve_kp = kp
        (0 to totalStages - 1).foreach { stageNumber =>
          val stageParams =
            List(Pointer.to(Array[Int](stageNumber)), Pointer.to(Array[Int](totalStages)))

          // restore the preserved kernel parameters
          kp = preserve_kp

          val stageNumber_kpd = createkernelParameterDesc2(stageNumber, cuStream)
          kp = kp ++ Seq(stageNumber_kpd._4) // gpuPtr
          listDevPtr = listDevPtr ++ List(stageNumber_kpd._3) // CUdeviceptr

          val totalStages_kpd = createkernelParameterDesc2(totalStages, cuStream)
          kp = kp ++ Seq(totalStages_kpd._4) // gpuPtr
          listDevPtr = listDevPtr ++ List(totalStages_kpd._3) // CUdeviceptr

          // val kernelParameters = Pointer.to(params: _*)
          val kernelParameters = Pointer.to(kp: _*)

          // Start the GPU execution with the populated kernel parameters
          launchKernel(function, inputHyIter.numElements, kernelParameters, dimensions, stageNumber, cuStream)
        }
    }

    // Free up locally allocated GPU memory
    listDevPtr.foreach(devPtr => {
      cuMemFree(devPtr)
    })
    listDevPtr = List()

    outputHyIter.freeGPUMemory
    inputHyIter.freeGPUMemory

    JCuda.cudaStreamDestroy(stream)

    outputHyIter.asInstanceOf[Iterator[U]]
  }
}





