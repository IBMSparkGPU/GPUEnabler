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

import org.apache.spark.SparkException
import org.apache.spark.gpuenabler.CUDAUtils._
import scala.collection.mutable

private[gpuenabler] case class RegisterGPUMemoryManager(id : String, slaveEndPointerRef: _RpcEndpointRef)

private[gpuenabler] case class UncacheGPU(id : Int)

private[gpuenabler] case class CacheGPU(id : Int)

private[gpuenabler] class GPUMemoryManagerMasterEndPoint(val rpcEnv: _RpcEnv) extends _ThreadSafeRpcEndpoint {

  val GPUMemoryManagerSlaves = new mutable.HashMap[String, _RpcEndpointRef]()

  def registerGPUMemoryManager(id : String, slaveEndpointRef: _RpcEndpointRef): Unit = {
    GPUMemoryManagerSlaves += id -> slaveEndpointRef
  }

  def unCacheGPU(rddId : Int): Unit = {
    for (slaveRef <- GPUMemoryManagerSlaves.values) {
      tell(slaveRef, UncacheGPU(rddId))
    }
  }

  def cacheGPU(rddId : Int): Unit = {
    for (slaveRef <- GPUMemoryManagerSlaves.values){
      tell(slaveRef, CacheGPU(rddId))
    }
  }

  override def receiveAndReply(context: _RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterGPUMemoryManager(id, slaveEndPointRef) =>
      registerGPUMemoryManager(id, slaveEndPointRef)
      context.reply (true)
    case UncacheGPU(rddId : Int) =>
      unCacheGPU(rddId)
      context.reply (true)
    case CacheGPU(rddId : Int) =>
      cacheGPU(rddId)
      context.reply (true)
  }

  private def tell(slaveEndPointRef: _RpcEndpointRef, message: Any) {
    if (!slaveEndPointRef.askWithRetry[Boolean](message)) {
      throw new SparkException("GPUMemoryManagerSlaveEndPoint returned false, expected true.")
    }
  }
}

// private[gpuenabler] class GPUMemoryManagerSlaveEndPoint(override val rpcEnv: RpcEnv,
private[gpuenabler] class GPUMemoryManagerSlaveEndPoint(val rpcEnv: _RpcEnv,
                                     val master : GPUMemoryManager) extends _ThreadSafeRpcEndpoint {

  def unCacheGPU(rddId : Int): Unit = {
    master.unCacheGPU(rddId)
  }

  def cacheGPU(rddId : Int): Unit = {
    master.cacheGPU(rddId)
  }

  override def receiveAndReply(context: _RpcCallContext): PartialFunction[Any, Unit] = {
    case UncacheGPU(rddId : Int) =>
      unCacheGPU(rddId)
      context.reply (true)
    case CacheGPU(rddId : Int) =>
      cacheGPU(rddId)
      context.reply (true)
    case id : String =>
      context.reply (true)
  }
}

private[gpuenabler] class GPUMemoryManager(val executorId : String,
                       val rpcEnv : _RpcEnv,
                       val driverEndpoint: _RpcEndpointRef,
                       val isDriver : Boolean,
                       val isLocal : Boolean) {

  val cachedGPUPointers = new mutable.HashMap[String, KernelParameterDesc]()
  val cachedGPURDDs = new mutable.ListBuffer[Int]()

  def getCachedGPUPointers : mutable.HashMap[String, KernelParameterDesc] = cachedGPUPointers

  if (!isDriver || isLocal) {
    val slaveEndpoint = rpcEnv.setupEndpoint(
      "GPUMemoryManagerSlaveEndpoint_" + executorId,
      new GPUMemoryManagerSlaveEndPoint(rpcEnv, this))
    tell(com.ibm.gpuenabler.RegisterGPUMemoryManager(executorId, slaveEndpoint))
  }



  def unCacheGPU(rddId : Int): Unit = {
    cachedGPURDDs -= rddId
    for ((name, ptr) <- cachedGPUPointers) {
      if (name.startsWith("rdd_" + rddId)) {
        import com.ibm.gpuenabler.GPUSparkEnv
        // TODO: Free GPU memory
        GPUSparkEnv.get.cudaManager.freeGPUMemory(ptr.devPtr)
        cachedGPUPointers.remove(name)
      }
    }
  }

  def cacheGPU(rddId : Int): Unit = {
    if (!cachedGPURDDs.contains(rddId)) {
      cachedGPURDDs += rddId
    }
  }

  def unCacheGPUSlaves(rddId : Int): Unit = {
    tell(com.ibm.gpuenabler.UncacheGPU(rddId))
  }

  def cacheGPUSlaves(rddId : Int): Unit = {
    tell(com.ibm.gpuenabler.CacheGPU(rddId))
  }

  /** Send a one-way message to the master endpoint, to which we expect it to reply with true. */
  private def tell(message: Any) {
    if (!driverEndpoint.askWithRetry[Boolean](message)) {
      throw new SparkException("GPUMemoryManagerMasterEndPoint returned false, expected true.")
    }
  }
}

private[gpuenabler] object GPUMemoryManager {
  val DRIVER_ENDPOINT_NAME = "GPUMemoryManager"
}
