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
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.gpuenabler.CUDAUtils._
import java.util.concurrent.ConcurrentHashMap
import org.apache.spark.sql.gpuenabler.CUDAUtils._Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import scala.collection.JavaConverters._
import scala.collection.mutable

private[gpuenabler] case class RegisterGPUMemoryManager(id : String, slaveEndPointerRef: _RpcEndpointRef)
private[gpuenabler] case class UncacheGPU(id : Int)
private[gpuenabler] case class CacheGPU(id : Int)
private[gpuenabler] case class UncacheGPUDS(lp : String)
private[gpuenabler] case class CacheGPUDS(lp : String, flag: Boolean = false)
private[gpuenabler] case class UncacheGPUDSAuto(lp : String)
private[gpuenabler] case class CacheGPUDSAuto(lp : String)

private[gpuenabler] class GPUMemoryManagerMasterEndPoint(val rpcEnv: _RpcEnv)
  extends _ThreadSafeRpcEndpoint with _Logging{

  val GPUMemoryManagerSlaves = new mutable.HashMap[String, _RpcEndpointRef]()
  val cachedLP = scala.collection.mutable.ListBuffer.empty[(String, Boolean)]
  val cachedLPAuto = scala.collection.mutable.ListBuffer.empty[String]

  if (GPUSparkEnv.isAutoCacheEnabled) {
    SparkContext.getOrCreate().addSparkListener(new SparkListener() {
      override def onJobEnd(jobEnd: SparkListenerJobEnd) {
        cachedLPAuto.foreach(lp => {
          logInfo(s"Cleanup logicalPlan(${lp}) added via auto caching")
          for (slaveRef <- GPUMemoryManagerSlaves.values) {
            tell(slaveRef, UncacheGPUDS(lp))
          }
        })
        cachedLPAuto.clear()
      }
    })
  }

  def registerGPUMemoryManager(id : String, slaveEndpointRef: _RpcEndpointRef): Unit = {
    GPUMemoryManagerSlaves += id -> slaveEndpointRef
    // For new slave node, push the cached logical plan list.
    cachedLP.foreach(lp => tell(slaveEndpointRef, CacheGPUDS(lp._1, lp._2)))
    cachedLPAuto.foreach(lp => tell(slaveEndpointRef, CacheGPUDS(lp)))
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

  def unCacheGPUAuto(lp : String): Unit = {
    cachedLPAuto.find(_ == lp) match {
      case Some(_) => cachedLPAuto -= lp
        for (slaveRef <- GPUMemoryManagerSlaves.values) {
          tell(slaveRef, UncacheGPUDS(lp))
        }
      case None => logDebug(s"unCacheGPUAuto LogicalPlan(${lp}) not found in cache")
    }
  }

  def cacheGPUAuto(lp : String): Unit = {
    cachedLPAuto.find(_ == lp) match {
      case Some(_) => logDebug(s"cacheGPUAuto LogicalPlan(${lp}) already found in cache")
      case None =>
        cachedLP.find(_._1 == lp) match {
          case Some(_) => logDebug(s"cacheGPU LogicalPlan(${lp}) already found in cache")
          case None => 
            logDebug(s"cacheGPUAuto LogicalPlan(${lp}) added to cache")
            cachedLPAuto += lp
            for (slaveRef <- GPUMemoryManagerSlaves.values){
              tell(slaveRef, CacheGPUDS(lp))
            }
        }
    }
  }

  def unCacheGPU(lp : String): Unit = {
    cachedLP.find(_._1 == lp) match {
      case Some(x) => cachedLP -= ((lp, x._2))
        for (slaveRef <- GPUMemoryManagerSlaves.values) {
          tell(slaveRef, UncacheGPUDS(lp))
        }
      case None => logDebug(s"unCacheGPU LogicalPlan(${lp}) not found in cache")
    }
  }

  def cacheGPU(lp : String, flag: Boolean): Unit = {
    cachedLPAuto.find(_ == lp) match {
      case Some(_) => cachedLPAuto -= lp
      case None =>
    }
    cachedLP.find(_._1 == lp) match {
      case Some(_) => logDebug(s"cacheGPU LogicalPlan(${lp}) already found in cache")
      case None => cachedLP += ((lp, flag))
        for (slaveRef <- GPUMemoryManagerSlaves.values){
          tell(slaveRef, CacheGPUDS(lp, flag))
        }
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
    case UncacheGPUDS(lp : String) =>
      unCacheGPU(lp)
      context.reply (true)
    case CacheGPUDS(lp : String, flag: Boolean) =>
      cacheGPU(lp, flag)
      context.reply (true)
    case UncacheGPUDSAuto(lp : String) =>
      unCacheGPUAuto(lp)
      context.reply (true)
    case CacheGPUDSAuto(lp : String) =>
      cacheGPUAuto(lp)
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

  def unCacheGPU(lp : String): Unit = {
    master.unCacheGPU(lp)
  }

  def cacheGPU(lp : String, flag: Boolean): Unit = {
    master.cacheGPU(lp, flag)
  }

  override def receiveAndReply(context: _RpcCallContext): PartialFunction[Any, Unit] = {
    case UncacheGPU(rddId : Int) =>
      unCacheGPU(rddId)
      context.reply (true)
    case CacheGPU(rddId : Int) =>
      cacheGPU(rddId)
      context.reply (true)
    case UncacheGPUDS(lp : String) =>
      unCacheGPU(lp)
      context.reply (true)
    case CacheGPUDS(lp : String, flag: Boolean) =>
      cacheGPU(lp, flag)
      context.reply (true)
    case _ : String =>
      context.reply (true)
  }
}

case class CachedGPUMeta (ptr: CUdeviceptr, own: Boolean, colWidth: java.lang.Integer )

private[gpuenabler] class GPUMemoryManager(val executorId : String,
                       val rpcEnv : _RpcEnv,
                       val driverEndpoint: _RpcEndpointRef,
                       val isDriver : Boolean,
                       val isLocal : Boolean) {
  
  val cachedGPUPointersDS = new ConcurrentHashMap[String,
    collection.concurrent.Map[Long, collection.concurrent.Map[String, CachedGPUMeta]]].asScala
  val cachedGPUOnlyDS = new mutable.ListBuffer[String]()
  val cachedGPUDS = new mutable.ListBuffer[String]()

  def getCachedGPUPointersDS : collection.concurrent.Map[String,
    collection.concurrent.Map[Long, collection.concurrent.Map[String, CachedGPUMeta]]] = cachedGPUPointersDS

  def cacheGPU(lp : String, flag: Boolean =false): Unit = {
    if (flag) {
      synchronized {
        if (!cachedGPUOnlyDS.contains(lp)) {
          cachedGPUOnlyDS += lp
        }
      }
    }
    synchronized {
      if (!cachedGPUDS.contains(lp)) {
        cachedGPUDS += lp
      }
    }
    cachedGPUPointersDS.getOrElseUpdate(lp, {
      new ConcurrentHashMap[Long, collection.concurrent.Map[String, CachedGPUMeta]].asScala
    })
  }

  def unCacheGPU(lp : String): Unit = {
    cachedGPUDS -= lp

    cachedGPUPointersDS.get(lp) match {
      case Some(partNumPtrs) => 
        val gpuPtrs = partNumPtrs.values
        gpuPtrs.foreach((partPtr) => {
          partPtr.foreach{
            case (_, obj) => if (obj.own) { GPUSparkEnv.get.cudaManager.freeGPUMemory(obj.ptr) }
          }
        })
        cachedGPUPointersDS -= lp
      case None =>
    }
  }

  def unCacheGPUSlavesAuto(lp : String): Unit = {
    tell(com.ibm.gpuenabler.UncacheGPUDSAuto(lp))
  }

  def cacheGPUSlavesAuto(lp : String): Unit = {
    tell(com.ibm.gpuenabler.CacheGPUDSAuto(lp))
  }

  def unCacheGPUSlaves(lp : String): Unit = {
    tell(com.ibm.gpuenabler.UncacheGPUDS(lp))
  }

  def cacheGPUSlaves(lp : String, flag: Boolean = false): Unit = {
    tell(com.ibm.gpuenabler.CacheGPUDS(lp, flag))
  }

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


