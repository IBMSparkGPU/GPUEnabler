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
 
import java.util.Date
import java.net.URL
import jcuda.Pointer
import jcuda.driver.JCudaDriver._
import jcuda.driver._
import jcuda.driver.{CUdeviceptr, CUmodule, JCudaDriver}
import jcuda.runtime.JCuda
import org.apache.commons.io.IOUtils
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable
import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import org.apache.spark.{SparkEnv, SparkException}

 
private[gpuenabler] object CUDAManagerCachedModule {
  private val cachedModules = new ConcurrentHashMap[(String, Int), CUmodule].asScala
  private val cachedContext = new ConcurrentHashMap[(String, Int), CUcontext].asScala

  def getInstance : collection.concurrent.Map[(String, Int), CUmodule] = { cachedModules }
  def getContext : collection.concurrent.Map[(String, Int), CUcontext] = { cachedContext }
}
 
private class CUDAManager {
  // Initialization
  // This is supposed to be called before ANY other JCuda* call to ensure we have properly loaded
  // native jCuda library and cuda context
  private[gpuenabler] var isGPUEnabled = false
  try {
    JCudaDriver.setExceptionsEnabled(true)
    JCudaDriver.cuInit(0)
    isGPUEnabled = true
    JCuda.cudaDeviceReset()
    CUDAManagerCachedModule.getInstance.clear
    CUDAManagerCachedModule.getContext.clear
  } catch {
    case _: Throwable =>
      CUDAManager.logger.info("Could not initialize CUDA, because native jCuda libraries were " +
        "not detected - continue to use CPU for execution")
  }
 
  // Returns the number of GPUs available in this node
  private[gpuenabler] def gpuCount = {
    val count = new Array[Int](1)
    cuDeviceGetCount(count)
    count(0)
  }

  def getModule(fname : String) : CUmodule = synchronized {
    val ptxURL = getClass.getResource(fname)
    val clm = cachedLoadModule(Left(ptxURL));
    clm
  }
 
  private[gpuenabler] def cachedLoadModule(resource: Either[URL, (String, String)]): CUmodule = {
    var resourceURL: URL = null
    var key: String = null
    var ptxString: String = null
    resource match {
      case Left(resURL) =>
        key = resURL.toString
        resourceURL = resURL
      case Right((k, v)) =>
        key = k
        ptxString = v
    }

    val devIx = new Array[Int](1)
    JCuda.cudaGetDevice(devIx)
 
    synchronized {
      // Create Context one per device & ptx file. Every thread created later, should set this context
      // before any CUDA related operation.
      val context:CUcontext = CUDAManagerCachedModule.getContext.getOrElseUpdate((key,devIx(0)), {
          val device: CUdevice = new CUdevice
          cuDeviceGet(device, 0)
          val context: CUcontext = new CUcontext
          cuCtxCreate(context, 0, device)
          cuCtxSetCurrent(context)
          JCuda.cudaDeviceSynchronize()
          context
      })

      // Make sure whether the current context is already set for this thread
      val prevcontext: CUcontext = new CUcontext
      cuCtxGetCurrent(prevcontext)
      if (prevcontext != context){
        cuCtxSetCurrent(context)
        JCuda.cudaDeviceSynchronize()
      }

      // Since multiple modules cannot be loaded into one context in runtime API,
      //   we use singleton cache http://stackoverflow.com/questions/32502375/
      //   loading-multiple-modules-in-jcuda-is-not-working
      // TODO support loading multiple ptxs
      //   http://stackoverflow.com/questions/32535828/jit-in-jcuda-loading-multiple-ptx-modules
      CUDAManagerCachedModule.getInstance.getOrElseUpdate((key, devIx(0)), {
        // TODO maybe unload the module if it won't be needed later
        var moduleBinaryData: Array[Byte] = null
        if (resourceURL != null) {
          val inputStream = resourceURL.openStream()
          moduleBinaryData = IOUtils.toByteArray(inputStream)
          inputStream.close()
        } else {
          moduleBinaryData = ptxString.getBytes()
        }
 
        val moduleBinaryData0 = new Array[Byte](moduleBinaryData.length + 1)
        System.arraycopy(moduleBinaryData, 0, moduleBinaryData0, 0, moduleBinaryData.length)
        moduleBinaryData0(moduleBinaryData.length) = 0
        val module = new CUmodule
        JCudaDriver.cuModuleLoadData(module, moduleBinaryData0)
        module
      })
    }
  }
 
  private[gpuenabler] def computeDimensions(size: Long): (Int, Int) = {
    val maxBlockDim = {
      import jcuda.driver.{CUdevice, CUdevice_attribute}
      val dev = new CUdevice
      JCudaDriver.cuCtxGetDevice(dev)
      val dim = new Array[Int](1)
      JCudaDriver.cuDeviceGetAttribute(dim, CUdevice_attribute.CU_DEVICE_ATTRIBUTE_MAX_BLOCK_DIM_X,
        dev)
      dim(0)
    }
    assert(size <= maxBlockDim * Int.MaxValue.toLong)
    (((size + maxBlockDim - 1) / maxBlockDim).toInt, maxBlockDim)
  }
 
  def allocateGPUMemory(sz: Int): CUdeviceptr = {
    val deviceInput = new CUdeviceptr()
    cuMemAlloc(deviceInput, sz)
    deviceInput
  }
 
  private[gpuenabler] def freeGPUMemory(ptr: Pointer) {
    JCuda.cudaFree(ptr)
  }
}
 
private[gpuenabler] object CUDAManager {
  private final val logger: Logger = LoggerFactory.getLogger(classOf[CUDAManager])
}
