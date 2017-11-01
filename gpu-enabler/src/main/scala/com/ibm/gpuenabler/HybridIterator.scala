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

import java.nio.ByteOrder

import jcuda.driver.JCudaDriver._
import jcuda.driver.{CUdeviceptr, CUresult, CUstream}
import jcuda.runtime.{JCuda, cudaStream_t}
import jcuda.{CudaException, Pointer}
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.RDDBlockId
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials
import scala.reflect.ClassTag
import scala.reflect.runtime._
import scala.reflect.runtime.universe.TermSymbol
import scala.collection.mutable.HashMap

// scalastyle:off no.finalize
private[gpuenabler] case class KernelParameterDesc(
                                cpuArr: Array[_ >: Byte with Short with Int
                                  with Float with Long with Double <: AnyVal],
                                cpuPtr: Pointer,
                                devPtr: CUdeviceptr,
                                gpuPtr: Pointer,
                                sz: Int,
                                symbol: TermSymbol)

private[gpuenabler] class HybridIterator[T: ClassTag](inputArr: Array[T], 
        colSchema: ColumnPartitionSchema,
        __columnsOrder: Seq[String],
        _blockId: Option[BlockId],
        numentries: Int = 0,
        outputArraySizes: Seq[Int] = null) extends Iterator[T] {

  private var _arr: Array[T] = inputArr

  def arr: Array[T] = if (_arr == null) {
    // Validate the CPU pointers before deserializing
    copyGpuToCpu
    _arr = getResultList
    _arr
  } else {
    _arr
  }

  private var _columnsOrder = __columnsOrder

  def columnsOrder: Seq[String] = if (_columnsOrder == null) {
    _columnsOrder = colSchema.getAllColumns()
    _columnsOrder
  } else {
    _columnsOrder

  }

  private val _outputArraySizes = if (outputArraySizes != null) {
    outputArraySizes 
  } else {
    val tempbuf = new ArrayBuffer[Int]()
    // if outputArraySizes is not provided by user program; create one
    // based on the number of columns and initialize it to '1' to denote
    // the object has only one element in it.
    columnsOrder.foreach(_ => tempbuf += 1)
    tempbuf
  }

  var _numElements = if (inputArr != null) inputArr.length else 0
  var idx: Int = -1

  val blockId: Option[BlockId] = _blockId match {
    case Some(x) => _blockId
    case None => {
      val r = scala.util.Random
      Some(RDDBlockId(r.nextInt(99999), r.nextInt(99999)))
    }
  }

  def rddId: Int = blockId.getOrElse(RDDBlockId(0, 0)).asRDDId.get.rddId

  def cachedGPUPointers: HashMap[String, KernelParameterDesc] =
    GPUSparkEnv.get.gpuMemoryManager.getCachedGPUPointers

  def numElements: Int = _numElements

  val stream = new cudaStream_t
  JCuda.cudaStreamCreateWithFlags(stream, JCuda.cudaStreamNonBlocking)
  val cuStream = new CUstream(stream)

  def hasNext: Boolean = {
    idx < arr.length - 1
  }

  def next: T = {
    idx = idx + 1
    arr(idx)
  }

  private def gpuCache: Boolean = GPUSparkEnv.get.gpuMemoryManager.cachedGPURDDs.contains(rddId)
  private def gpuAutoCache: Boolean = GPUSparkEnv.get.gpuMemoryManager.autoCachedGPURDDs.contains(rddId)

  // Function to free the allocated GPU memory if the RDD is not cached.
  def freeGPUMemory: Unit = {
    if (!(gpuCache || gpuAutoCache)) {
      // Make sure the CPU ptrs are populated before GPU memory is freed up.
      copyGpuToCpu
      if (_listKernParmDesc == null) return
      _listKernParmDesc = _listKernParmDesc.map(kpd => {
        if (kpd.devPtr != null) {
          GPUSparkEnv.get.cudaManager.freeGPUMemory(kpd.devPtr)
        }
        KernelParameterDesc(kpd.cpuArr, kpd.cpuPtr, null, null, kpd.sz, kpd.symbol)
      })
      cachedGPUPointers.retain(
        (name, kernelParameterDesc) => !name.startsWith(blockId.get.toString))
    }
  }

  // TODO: Discuss the need for finalize; how to handle streams;
  override def finalize(): Unit = {
    JCuda.cudaStreamDestroy(stream)
    super.finalize
  }

  // This function is used to copy the CPU memory to GPU for
  // an existing Hybrid Iterator
  def copyCpuToGpu: Unit = {
    if (_listKernParmDesc == null) return
    _listKernParmDesc = _listKernParmDesc.map(kpd => {
      if (kpd.devPtr == null) {
        val devPtr = GPUSparkEnv.get.cudaManager.allocateGPUMemory(kpd.sz)
        cuMemcpyHtoDAsync(devPtr, kpd.cpuPtr, kpd.sz, cuStream)
        cuCtxSynchronize()
        val gPtr = Pointer.to(devPtr)
        KernelParameterDesc(kpd.cpuArr, kpd.cpuPtr, devPtr, gPtr, kpd.sz, kpd.symbol)
      } else {
        kpd
      }
    })
  }

  // This function is used to copy the GPU memory to CPU for
  // an existing Hybrid Iterator
  def copyGpuToCpu: Unit = {
    // Ensure main memory is allocated to hold the GPU data
    if (_listKernParmDesc == null) return
    _listKernParmDesc = (_listKernParmDesc, colSchema.orderedColumns(columnsOrder)).
        zipped.map((kpd, col) => {
      if (kpd.cpuArr == null && kpd.cpuPtr == null && kpd.devPtr != null) {
        val (cpuArr, cpuPtr: Pointer) = col.columnType match {
          case c if c == INT_COLUMN => {
            val y = new Array[Int](kpd.sz/ INT_COLUMN.bytes)
            (y, Pointer.to(y))
          }
          case c if c == SHORT_COLUMN => {
            val y = new Array[Short](kpd.sz/ SHORT_COLUMN.bytes)
            (y, Pointer.to(y))
          }
          case c if c == BYTE_COLUMN => {
            val y = new Array[Byte](kpd.sz/ BYTE_COLUMN.bytes)
            (y, Pointer.to(y))
          }
          case c if c == LONG_COLUMN => {
            val y = new Array[Long](kpd.sz/ LONG_COLUMN.bytes)
            (y, Pointer.to(y))
          }
          case c if c == FLOAT_COLUMN => {
            val y = new Array[Float](kpd.sz/ FLOAT_COLUMN.bytes)
            (y, Pointer.to(y))
          }
          case c if c == DOUBLE_COLUMN => {
            val y = new Array[Double](kpd.sz / DOUBLE_COLUMN.bytes)
            (y, Pointer.to(y))
          }
          case c if c == LONG_COLUMN => {
            val y = new Array[Long](kpd.sz / LONG_COLUMN.bytes)
            (y, Pointer.to(y))
          }
          case c if c == INT_ARRAY_COLUMN => {
            val y = new Array[Int](kpd.sz / INT_COLUMN.bytes)
            (y, Pointer.to(y))
          }
          case c if c == LONG_ARRAY_COLUMN => {
            val y = new Array[Long](kpd.sz / LONG_COLUMN.bytes)
            (y, Pointer.to(y))
          }
          case c if c == FLOAT_ARRAY_COLUMN => {
            val y = new Array[Float](kpd.sz / FLOAT_COLUMN.bytes)
            (y, Pointer.to(y))
          }
          case c if c == DOUBLE_ARRAY_COLUMN => {
            val y = new Array[Double](kpd.sz / DOUBLE_COLUMN.bytes)
            (y, Pointer.to(y))
          }
          case c if c == LONG_ARRAY_COLUMN => {
            val y = new Array[Long](kpd.sz / LONG_COLUMN.bytes)
            (y, Pointer.to(y))
          }
        }
        cuMemcpyDtoHAsync(cpuPtr, kpd.devPtr, kpd.sz, cuStream) 
        KernelParameterDesc(cpuArr, cpuPtr, kpd.devPtr, kpd.gpuPtr, kpd.sz, kpd.symbol)
      } else {
        kpd
      }
    })
    cuCtxSynchronize()
  }

  // Extract the getter method from the given object using reflection
  private def getter[C](obj: Any, symbol: TermSymbol): C = {
    currentMirror.reflect(obj).reflectField(symbol).get.asInstanceOf[C]
  }

  // valVarMembers will be available only for non-primitive types
  private val valVarMembers = if (colSchema.isPrimitive) {
    null
  } else {
    val runtimeCls = implicitly[ClassTag[T]].runtimeClass
    val clsSymbol = currentMirror.classSymbol(runtimeCls)
    clsSymbol.typeSignature.members.view
      .filter(p => !p.isMethod && p.isTerm).map(_.asTerm)
      .filter(p => p.isVar || p.isVal)
  }

  // Allocate Memory from Off-heap Pinned Memory and returns
  // the pointer & buffer address pointing to it
  private def allocPinnedHeap(size: Long) = {
    val ptr: Pointer = new Pointer()
    try {
      val result: Int = JCuda.cudaHostAlloc(ptr, size, JCuda.cudaHostAllocPortable)
      if (result != CUresult.CUDA_SUCCESS) {
        throw new CudaException(JCuda.cudaGetErrorString(result))
      }
    }
    catch {
      case ex: Exception => {
        throw new OutOfMemoryError("Could not alloc pinned memory: " + ex.getMessage)
      }
    }
    (ptr, ptr.getByteBuffer(0, size).order(ByteOrder.LITTLE_ENDIAN))
  }

  def listKernParmDesc: Seq[KernelParameterDesc] = _listKernParmDesc

  private var _listKernParmDesc = if (inputArr != null && inputArr.length > 0) {
    // initFromInputIterator
    val kernParamDesc = colSchema.orderedColumns(columnsOrder).map { col =>
      cachedGPUPointers.getOrElseUpdate(blockId.get + col.prettyAccessor, {
        val cname = col.prettyAccessor.split("\\.").reverse.head
        val symbol = if (colSchema.isPrimitive) {
          null
        } else {
          valVarMembers.find(_.name.toString.startsWith(cname)).get
        }

        val (hPtr: Pointer, colDataSize: Int) = {
           val mirror = ColumnPartitionSchema.mirror
           val priv_getter = col.terms.foldLeft(identity[Any] _)((r, term) =>
              ((obj: Any) => mirror.reflect(obj).reflectField(term).get) compose r)

          var bufferOffset = 0

          col.columnType match {
            case c if c == INT_COLUMN => {
              val size = col.memoryUsage(inputArr.length).toInt
              val (ptr, buffer) = allocPinnedHeap(size)        
              inputArr.foreach(x => buffer.putInt(priv_getter(x).asInstanceOf[Int]))
              (ptr, size)
            }
            case c if c == LONG_COLUMN => {
              val size = col.memoryUsage(inputArr.length).toInt
              val (ptr, buffer) = allocPinnedHeap(size)        
              inputArr.foreach(x => buffer.putLong(priv_getter(x).asInstanceOf[Long]))
              (ptr, size)
            }
            case c if c == SHORT_COLUMN => {
              val size = col.memoryUsage(inputArr.length).toInt
              val (ptr, buffer) = allocPinnedHeap(size)
              inputArr.foreach(x => buffer.putShort(priv_getter(x).asInstanceOf[Short]))
              (ptr, size)
            }
            case c if c == BYTE_COLUMN => {
              val size = col.memoryUsage(inputArr.length).toInt
              val (ptr, buffer) = allocPinnedHeap(size)              
              inputArr.foreach(x => buffer.put(priv_getter(x).asInstanceOf[Byte]))
              (ptr, size)
            }
            case c if c == FLOAT_COLUMN => {
              val size = col.memoryUsage(inputArr.length).toInt
              val (ptr, buffer) = allocPinnedHeap(size)
              inputArr.foreach(x => buffer.putFloat(priv_getter(x).asInstanceOf[Float]))
              (ptr, size)
            }
            case c if c == DOUBLE_COLUMN => {
              val size = col.memoryUsage(inputArr.length).toInt
              val (ptr, buffer) = allocPinnedHeap(size)
              inputArr.foreach(x => buffer.putDouble(priv_getter(x).asInstanceOf[Double]))
              (ptr, size)
            }
            case c if c == LONG_COLUMN => {
              val size = col.memoryUsage(inputArr.length).toInt
              val (ptr, buffer) = allocPinnedHeap(size)
              inputArr.foreach(x => buffer.putLong(priv_getter(x).asInstanceOf[Long]))
              (ptr, size)
            }
            case c if c == INT_ARRAY_COLUMN => {
              // retrieve the first element to determine the array size.
              val arrLength = priv_getter(inputArr.head).asInstanceOf[Array[Int]].length
              val size = col.memoryUsage(inputArr.length * arrLength).toInt
              val (ptr, buffer) = allocPinnedHeap(size)              
              inputArr.foreach(x => {
                buffer.position(bufferOffset)
                buffer.asIntBuffer().put(priv_getter(x).asInstanceOf[Array[Int]], 0, arrLength)
                // bufferOffset += col.memoryUsage(arrLength).toInt
                bufferOffset += arrLength * INT_COLUMN.bytes
              })
              (ptr, size)
            }
            case c if c == LONG_ARRAY_COLUMN => {
              // retrieve the first element to determine the array size.
              val arrLength = priv_getter(inputArr.head).asInstanceOf[Array[Long]].length
              val size = col.memoryUsage(inputArr.length * arrLength).toInt
              val (ptr, buffer) = allocPinnedHeap(size)
              inputArr.foreach(x => {
                buffer.position(bufferOffset)
                buffer.asLongBuffer().put(priv_getter(x).asInstanceOf[Array[Long]], 0, arrLength)
                // bufferOffset += col.memoryUsage(arrLength).toInt
                bufferOffset += arrLength * LONG_COLUMN.bytes
              })
              (ptr, size)
            }
            case c if c == FLOAT_ARRAY_COLUMN => {
              // retrieve the first element to determine the array size.
              val arrLength = priv_getter(inputArr.head).asInstanceOf[Array[Float]].length
              val size = col.memoryUsage(inputArr.length * arrLength).toInt
              val (ptr, buffer) = allocPinnedHeap(size)              
              inputArr.foreach(x => {
                buffer.position(bufferOffset)
                buffer.asFloatBuffer().put(priv_getter(x).asInstanceOf[Array[Float]], 0, arrLength)
                bufferOffset += arrLength * FLOAT_COLUMN.bytes
                // bufferOffset += col.memoryUsage(arrLength).toInt
              })
              (ptr, size)
            }
            case c if c == DOUBLE_ARRAY_COLUMN => {
              // retrieve the first element to determine the array size.
              val arrLength = priv_getter(inputArr.head).asInstanceOf[Array[Double]].length
              val size = col.memoryUsage(inputArr.length * arrLength).toInt
              val (ptr, buffer) = allocPinnedHeap(size)
              inputArr.foreach(x => {
                buffer.position(bufferOffset)
                buffer.asDoubleBuffer().put(priv_getter(x).asInstanceOf[Array[Double]], 0, arrLength)
                bufferOffset += arrLength * DOUBLE_COLUMN.bytes
                // bufferOffset += col.memoryUsage(arrLength).toInt
              })
              (ptr, size)
            }
            case c if c == LONG_ARRAY_COLUMN => {
              // retrieve the first element to determine the array size.
              val arrLength = priv_getter(inputArr.head).asInstanceOf[Array[Long]].length
              val size = col.memoryUsage(inputArr.length * arrLength).toInt
              val (ptr, buffer) = allocPinnedHeap(size)
              inputArr.foreach(x => {
                buffer.position(bufferOffset)
                buffer.asLongBuffer().put(priv_getter(x).asInstanceOf[Array[Long]], 0, arrLength)
                bufferOffset += arrLength * LONG_COLUMN.bytes
                // bufferOffset += col.memoryUsage(arrLength).toInt
              })
              (ptr, size)
            }
          }
        }
        val devPtr = GPUSparkEnv.get.cudaManager.allocateGPUMemory(colDataSize)
        cuMemcpyHtoDAsync(devPtr, hPtr, colDataSize, cuStream)
        val gPtr = Pointer.to(devPtr)

        // mark the cpuPtr null as we use pinned memory and got the Pointer directly
        new KernelParameterDesc(null, hPtr, devPtr, gPtr, colDataSize, symbol)
      })
    }
    cuCtxSynchronize()
    kernParamDesc
  } else if (numentries != 0) { // initEmptyArrays - mostly used by output argument list
    // set the number of entries to numentries as its initialized to '0'
    _numElements = numentries
    val colOrderSizes = colSchema.orderedColumns(columnsOrder) zip _outputArraySizes

    val kernParamDesc = colOrderSizes.map { col =>
      cachedGPUPointers.getOrElseUpdate(blockId.get + col._1.prettyAccessor, {
        val cname = col._1.prettyAccessor.split("\\.").reverse.head
        val symbol = if (colSchema.isPrimitive) {
          null
        } else {
          valVarMembers.find(_.name.toString.startsWith(cname)).get
        }

        val colDataSize: Int = col._1.columnType match {
          case c if c == INT_COLUMN => {
            numentries * INT_COLUMN.bytes
          }
          case c if c == LONG_COLUMN => {
            numentries * LONG_COLUMN.bytes
          }
          case c if c == SHORT_COLUMN => {
            numentries * SHORT_COLUMN.bytes
          }
          case c if c == BYTE_COLUMN => {
            numentries * BYTE_COLUMN.bytes
          }
          case c if c == FLOAT_COLUMN => {
            numentries * FLOAT_COLUMN.bytes
          }
          case c if c == DOUBLE_COLUMN => {
            numentries * DOUBLE_COLUMN.bytes
          }
          case c if c == LONG_COLUMN => {
            numentries * LONG_COLUMN.bytes
          }
          case c if c == INT_ARRAY_COLUMN => {
            col._2 * numentries * INT_COLUMN.bytes
          }
          case c if c == LONG_ARRAY_COLUMN => {
            col._2 * numentries * LONG_COLUMN.bytes
          }
          case c if c == FLOAT_ARRAY_COLUMN => {
            col._2 * numentries * FLOAT_COLUMN.bytes
          }
          case c if c == DOUBLE_ARRAY_COLUMN => {
            col._2 * numentries * DOUBLE_COLUMN.bytes
          }
          case c if c == LONG_ARRAY_COLUMN => {
            col._2 * numentries * LONG_COLUMN.bytes
          }
          // TODO : Handle error condition
        }
        val devPtr = GPUSparkEnv.get.cudaManager.allocateGPUMemory(colDataSize)
        cuMemsetD32Async(devPtr, 0, colDataSize / 4, cuStream)
        val gPtr = Pointer.to(devPtr)

        // Allocate only GPU memory; main memory will be allocated during deserialization
        new KernelParameterDesc(null, null, devPtr, gPtr, colDataSize, symbol)
      })
    }
    cuCtxSynchronize()
    kernParamDesc
  } else {
    null
  }

  // Use reflection to instantiate object without calling constructor
  private def instantiateClass(cls: Class[_]): AnyRef = {
    val rf = sun.reflect.ReflectionFactory.getReflectionFactory
    val parentCtor = classOf[java.lang.Object].getDeclaredConstructor()
    val newCtor = rf.newConstructorForSerialization(cls, parentCtor)
    val obj = newCtor.newInstance().asInstanceOf[AnyRef]
    obj
  }

  // Extract the setter method from the given object using reflection
  private def setter[C](obj: Any, value: C, symbol: TermSymbol) = {
    currentMirror.reflect(obj).reflectField(symbol).set(value)
  }

  def deserializeColumnValue(columnType: ColumnType, cpuArr: Array[_ >: Byte with Short with Int
    with Float with Long with Double <: AnyVal], index: Int, outsize: Int = 0): Any = {
    columnType match {
      case  INT_COLUMN => cpuArr(index).asInstanceOf[Int]
      case  LONG_COLUMN => cpuArr(index).asInstanceOf[Long]
      case  SHORT_COLUMN => cpuArr(index).asInstanceOf[Short]
      case  BYTE_COLUMN => cpuArr(index).asInstanceOf[Byte]
      case  FLOAT_COLUMN => cpuArr(index).asInstanceOf[Float]
      case  DOUBLE_COLUMN => cpuArr(index).asInstanceOf[Double]
      case  LONG_COLUMN => cpuArr(index).asInstanceOf[Long]
      case  INT_ARRAY_COLUMN => {
        val array = new Array[Int](outsize)
        var runIndex = index
        for (i <- 0 to outsize - 1) {
          array(i) = cpuArr(runIndex).asInstanceOf[Int]
          runIndex += 1
        }
        array
      }
      case  LONG_ARRAY_COLUMN => {
        val array = new Array[Long](outsize)
        var runIndex = index
        for (i <- 0 to outsize - 1) {
          array(i) = cpuArr(runIndex).asInstanceOf[Long]
          runIndex += 1
        }
        array
      }
      case  FLOAT_ARRAY_COLUMN => {
        val array = new Array[Float](outsize)
        var runIndex = index

        for (i <- 0 to outsize - 1) {
          array(i) = cpuArr(runIndex).asInstanceOf[Float]
          runIndex += 1
        }
        array
      }
      case  DOUBLE_ARRAY_COLUMN => {
        val array = new Array[Double](outsize)
        var runIndex = index

        for (i <- 0 to outsize - 1) {
          array(i) = cpuArr(runIndex).asInstanceOf[Double]
          runIndex += 1
        }
        array
      }
      case  LONG_ARRAY_COLUMN => {
        val array = new Array[Long](outsize)
        var runIndex = index

        for (i <- 0 to outsize - 1) {
          array(i) = cpuArr(runIndex).asInstanceOf[Long]
          runIndex += 1
        }
        array
      }
    }
  }

  def getResultList: Array[T] = {
    val resultsArray = new Array[T](numElements)
    val runtimeCls = implicitly[ClassTag[T]].runtimeClass

    for (index <- 0 to numElements - 1) {
      val obj = if (colSchema.isPrimitive) {
        (colSchema.orderedColumns(columnsOrder), listKernParmDesc, _outputArraySizes).zipped.map(
          (col, cdesc, outsize) => {
            val retObj = deserializeColumnValue(col.columnType,
              cdesc.cpuArr, index * outsize, outsize)
            retObj
        }).head
      } else {
        // For non-primitive types create an object on the fly and populate the values
        val retObj = instantiateClass(runtimeCls)

        (colSchema.orderedColumns(columnsOrder), listKernParmDesc,
          _outputArraySizes).zipped.foreach(
          (col, cdesc, outsize) => {
            setter(retObj, deserializeColumnValue(col.columnType, cdesc.cpuArr,
              index * outsize, outsize), cdesc.symbol)
          })
        retObj
      }
      resultsArray(index) = obj.asInstanceOf[T]
    }
    resultsArray
  }
}



