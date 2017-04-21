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

import org.apache.spark.SparkEnv
import org.apache.spark.gpuenabler.CUDAUtils._
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

  val cudaManager = new CUDAManager
  val gpuMemoryManager = new GPUMemoryManager(executorId, rpcEnv,
                    registerOrLookupEndpoint(GPUMemoryManager.DRIVER_ENDPOINT_NAME,
                      new GPUMemoryManagerMasterEndPoint(rpcEnv)),
                    isDriver,
                    isLocal)
  val isGPUEnabled = if (cudaManager != null) cudaManager.isGPUEnabled else false
  def gpuCount = if (isGPUEnabled) cudaManager.gpuCount else 0
  val isGPUCodeGenEnabled =
    isGPUEnabled && SparkEnv.get.conf.getBoolean("spark.gpu.codegen", false)
}

private[gpuenabler] object GPUSparkEnv extends _Logging {
  private var env : GPUSparkEnv = _
  private var oldSparkEnv : SparkEnv = _

  def initalize(): Unit = {
      env = new GPUSparkEnv()
  }

//  initalize(true)
  def get = {
    this.synchronized {
      if (SparkEnv.get != oldSparkEnv) {
        oldSparkEnv = SparkEnv.get
        initalize()
      }
      
      if (env.isGPUEnabled) { 
        val executorId = env.executorId match {
          case "driver" => 0
          case _ => SparkEnv.get.executorId.toInt
        }
        JCuda.cudaSetDevice(executorId % env.gpuCount )
      }
      env
    }
  }
}
