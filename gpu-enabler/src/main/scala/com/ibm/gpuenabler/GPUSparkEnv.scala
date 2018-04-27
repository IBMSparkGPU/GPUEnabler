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
 
import jcuda.driver.{CUcontext, CUdevice}
import jcuda.driver.JCudaDriver.{cuCtxCreate, cuDeviceGet, cuCtxGetCurrent, cuCtxSetCurrent }
import org.apache.spark.SparkEnv
import org.apache.spark.gpuenabler.CUDAUtils._
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import jcuda.runtime.JCuda
 
private[gpuenabler] class GPUSparkEnv() {
 
  def rpcEnv = _rpcEnv
  def conf = SparkEnv.get.conf
  def executorId = SparkEnv.get.executorId
  def isDriver = executorId.equals("driver")
  def master: String = SparkEnv.get.conf.get("spark.master")
  def isLocal: Boolean = master == "local" || master.startsWith("local[")
 
  def registerOrLookupEndpoint( name: String, endpointCreator: => _RpcEndpoint): _RpcEndpointRef = {
    if (isDriver) {
      rpcEnv.setupEndpoint(name, endpointCreator)
    } else {
      _RpcUtils.makeDriverRef(name, conf, rpcEnv)
    }
  }

  val execContext: CUcontext = new CUcontext
  val _cudaManager = new CUDAManager
  val gpuMemoryManager = new GPUMemoryManager(executorId, rpcEnv,
                    registerOrLookupEndpoint(GPUMemoryManager.DRIVER_ENDPOINT_NAME,
                      new GPUMemoryManagerMasterEndPoint(rpcEnv)),
                    isDriver,
                    isLocal)
  val isGPUEnabled = if (_cudaManager != null) _cudaManager.isGPUEnabled else false
  def gpuCount = if (isGPUEnabled) _cudaManager.gpuCount else 0

  val isGPUCodeGenEnabled =
    isGPUEnabled && SparkEnv.get.conf.getBoolean("spark.gpu.codegen", false)

  var gpuDevice = 0

  def cudaManager:CUDAManager = {
    // Make sure whether the current context is already set for this thread
    val prevcontext: CUcontext = new CUcontext
    cuCtxGetCurrent(prevcontext)
    if (prevcontext != execContext){
      cuCtxSetCurrent(execContext)
    }
    _cudaManager
  }
}
 
private[gpuenabler] object GPUSparkEnv {
  private var env : GPUSparkEnv = _
  private var oldSparkEnv : SparkEnv = _
 
  def initalize(): Unit = {
      env = new GPUSparkEnv()
  }

  // Auto Caching in GPU Enabled by default
  val isAutoCacheEnabled: Boolean =
    if (SparkEnv.get.conf.getInt("spark.gpuenabler.autocache", 1) == 1) true else false

  /* Auto Cache Eviction in GPU Enabled by default
   * Eviction threshold is an equiped VRAM capacity by default
   *
   * Auto Cache Eviction discards auto-cached RDDs
   * Programmers set number N to conf.set("spark.gpuenabler.autocacheevict.gpumem","N").
   * The behavior of Auto Cache Eviction branches into two types:
   *   - ( 0 < N <= "VRAM_size" )      -> Auto Cache Evict is enabled.
   *                                      If an used space of VRAM becomes more than "N" MB,
   *                                      Auto Cache Evict discards auto-cached RDDs in LRU manner.
   *   - ( "VRAM_size" < N || N <= 0)  -> Auto Cache Evict is enabled.
   *                                      If an used space of VRAM becomes more than "VRAM_size",
   *                                      Auto Cache Evict discards auto-cached RDDs in LRU manner.
   */
  val isAutoCacheEvictEnabled: Boolean =
    if (SparkEnv.get.conf.getInt("spark.gpuenabler.autocacheevict.isenabled", 1) == 1) true else false

  val gpuMemCap: Long= {
    import jcuda.runtime.JCuda
    val freeMemArray = new Array[Long](1)
    val totalMemArray = new Array[Long](1)
    jcuda.runtime.JCuda.cudaMemGetInfo(freeMemArray, totalMemArray)
    val evictThreshold = SparkEnv.get.conf.getInt("spark.gpuenabler.autocacheevict.gpumem", 0) * 1000000.toLong
    if (isAutoCacheEvictEnabled) {
      if (evictThreshold <= totalMemArray(0) && evictThreshold > 0){
        evictThreshold
      } else {
        totalMemArray(0)
      }
    } else {
      0
    }
  }

  def get = {
      val curSparkEnv = SparkEnv.get
      synchronized {
        if (curSparkEnv != null && curSparkEnv  != oldSparkEnv) {
          oldSparkEnv = curSparkEnv
          initalize()

          if (env.isGPUEnabled) {
            val executorId = env.executorId match {
              case "driver" => 0
              case _ => 
                val execIdStr = SparkEnv.get.executorId
                if (execIdStr.forall( _.isDigit )) {
                  execIdStr.substring(0, Math.min(execIdStr.length, 5)).toInt
                } else {
                  val stripNum = ("[0-9]".r findAllIn execIdStr).mkString("")
                  if (stripNum.isEmpty) 0
                  else stripNum.substring(0, Math.min(stripNum.length, 5)).toInt
                }
            }

            env.gpuDevice = executorId % env.gpuCount
            JCuda.cudaSetDevice(env.gpuDevice )

            // Create a new Context
            val device: CUdevice = new CUdevice
            cuDeviceGet(device, env.gpuDevice)
            cuCtxCreate(env.execContext, 0, device)
          }
        }
      }
      env
    }
}
